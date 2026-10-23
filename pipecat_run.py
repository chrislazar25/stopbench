"""Say "stop" to a Pipecat agent at three moments during a refund tool call.

The scripted LLM makes the same tool call every run, so only the moment of the
interruption varies. A real model (Claude or OpenAI) shows what the agent then
tells the caller. Each run records what the ledger committed, what Pipecat wrote into the
LLM context as the tool's result, and the agent's reply after the stop.

    python pipecat_run.py --runs 10
    python pipecat_run.py --llm anthropic --runs 5 --cancel true   # needs ANTHROPIC_API_KEY in .env
    python pipecat_run.py --llm openai --runs 5 --cancel true      # needs OPENAI_API_KEY in .env
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

from loguru import logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.frames.frames import (
    EndFrame,
    FunctionCallFromLLM,
    InterruptionFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMMessagesAppendFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineWorker
from pipecat.processors.aggregators import async_tool_messages
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.llm_service import FunctionCallParams, LLMService
from pipecat.services.settings import LLMSettings
from pipecat.workers.runner import WorkerRunner

import refund
from stopbench import (
    AFTER_COMMIT_S,
    LLM_TIMEOUT_S,
    PREP_S,
    SETTLE_S,
    SYSTEM_PROMPT,
    TOOL_DESCRIPTION,
    USER_ASK,
    USER_STOP,
    Marks,
    Moment,
    Results,
    Told,
    ledger,
    load_env,
    reach_moment,
    verdict,
)

COMMIT_DELAY_MS = 400


@dataclass
class RunResult:
    framework: str
    pipecat_version: str
    moment: str
    cancel_on_interruption: bool
    run: int
    refund_id: str
    ledger_count: int
    tool_result_in_context: str | None
    interrupted_at: float | None
    received_at: float | None
    committed_at: float | None
    llm: str = "scripted"
    tool_called: bool = True
    agent_reply: str | None = None

    @property
    def landed(self) -> bool:
        return self.ledger_count > 0

    @property
    def told(self) -> Told:
        return told_from(self.__dict__)

    @property
    def verdict(self) -> str:
        return verdict(self.tool_called, self.landed, Moment(self.moment), self.told)


# Read after the run ends. With cancel_on_interruption=False, a result that lands after the
# stop goes in a separate developer message (_is_deferred in llm_response_universal.py), so
# the tool message still holds what the model had when it answered.
TOLD_SOURCE = "context_after_run"


def told_from(record: dict) -> Told:
    text = record["tool_result_in_context"]
    if text is None:
        return Told.NOTHING
    async_tool = async_tool_messages.parse_message({"role": "tool", "content": text})
    if text == "IN_PROGRESS" or (async_tool and async_tool.kind != "final"):
        return Told.PLACEHOLDER
    if "CANCEL" in text.upper():
        return Told.CANCELLED
    return Told.RESULT


class ScriptedLLM(LLMService):
    """Calls issue_refund once per user turn and never generates speech."""

    def __init__(self, refund_id: str, **kwargs):
        unset = dict.fromkeys(
            ["model", "system_instruction", "temperature", "max_tokens", "top_p", "top_k",
             "frequency_penalty", "presence_penalty", "seed", "filter_incomplete_user_turns",
             "user_turn_completion_config"]
        )
        super().__init__(settings=LLMSettings(**unset), **kwargs)
        self._refund_id = refund_id
        self._called = False

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if not isinstance(frame, LLMContextFrame):
            await self.push_frame(frame, direction)
            return
        await self.push_frame(LLMFullResponseStartFrame())
        if not self._called:
            self._called = True
            await self.run_function_calls(
                [
                    FunctionCallFromLLM(
                        function_name="issue_refund",
                        tool_call_id=f"call_{self._refund_id}",
                        arguments={"amount_cents": 4000},
                        context=frame.context,
                    )
                ]
            )
        await self.push_frame(LLMFullResponseEndFrame())


# provider -> (env var with the key, default model)
PROVIDERS = {
    "anthropic": ("ANTHROPIC_API_KEY", "claude-haiku-5-5"),
    "openai": ("OPENAI_API_KEY", "gpt-6.1-sol"),
}


def make_llm(kind: str, model: str, refund_id: str) -> LLMService:
    if kind == "scripted":
        return ScriptedLLM(refund_id)
    if kind == "openai":
        from pipecat.services.openai.responses.llm import OpenAIResponsesLLMService

        return OpenAIResponsesLLMService(
            api_key=os.environ["OPENAI_API_KEY"],
            settings=OpenAIResponsesLLMService.Settings(model=model),
        )
    from pipecat.services.anthropic.llm import AnthropicLLMService

    return AnthropicLLMService(
        api_key=os.environ["ANTHROPIC_API_KEY"],
        settings=AnthropicLLMService.Settings(model=model),
    )


def assistant_text(messages: list, start: int) -> str | None:
    parts = []
    for message in messages[start:]:
        if not isinstance(message, dict) or message.get("role") != "assistant":
            continue
        content = message.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            parts += [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
    text = " ".join(p.strip() for p in parts if p and p.strip())
    return text or None


def tool_result(context: LLMContext) -> str | None:
    for message in context.get_messages():
        if isinstance(message, dict) and message.get("role") == "tool":
            content = message.get("content")
            return content if isinstance(content, str) else json.dumps(content)
    return None


async def one_run(
    port: int, moment: Moment, cancel: bool, run: int, version: str, llm_kind: str, model: str
) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    marks = Marks()
    url = f"http://127.0.0.1:{port}/refunds"

    async def tool(params: FunctionCallParams):
        amount = params.arguments.get("amount_cents", 4000)
        await params.result_callback(await refund.issue_refund(url, refund_id, amount, marks))

    tools = ToolsSchema(
        standard_tools=[
            FunctionSchema(
                name="issue_refund",
                description=TOOL_DESCRIPTION,
                properties={"amount_cents": {"type": "integer", "description": "Amount in cents."}},
                required=["amount_cents"],
            )
        ]
    )
    context = LLMContext(messages=[{"role": "system", "content": SYSTEM_PROMPT}], tools=tools)
    aggregators = LLMContextAggregatorPair(context)
    llm = make_llm(llm_kind, model, refund_id)
    llm.register_function("issue_refund", tool, cancel_on_interruption=cancel)

    task = PipelineWorker(
        Pipeline([aggregators.user(), llm, aggregators.assistant()]), cancel_on_idle_timeout=False
    )
    interrupted_at = None
    tool_called = True
    reply_from = 1

    async def drive():
        nonlocal interrupted_at, tool_called, reply_from
        await task.queue_frame(
            LLMMessagesAppendFrame(messages=[{"role": "user", "content": USER_ASK}], run_llm=True)
        )
        tool_called = await reach_moment(marks, moment)
        if tool_called and moment is not Moment.NONE:
            interrupted_at = time.time()
            await task.queue_frame(InterruptionFrame())
            reply_from = len(context.get_messages())
            await task.queue_frame(
                LLMMessagesAppendFrame(messages=[{"role": "user", "content": USER_STOP}], run_llm=True)
            )
        await asyncio.sleep(PREP_S + COMMIT_DELAY_MS / 1000 + AFTER_COMMIT_S + SETTLE_S)
        if llm_kind != "scripted":
            deadline = time.time() + LLM_TIMEOUT_S
            while time.time() < deadline and not assistant_text(context.get_messages(), reply_from):
                await asyncio.sleep(0.25)
            await asyncio.sleep(SETTLE_S)
        await task.queue_frame(EndFrame())

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(task)
    await asyncio.gather(runner.run(), drive())

    ledger = await refund.read_ledger(url, refund_id)
    rows = ledger["rows"]
    return RunResult(
        framework="pipecat",
        pipecat_version=version,
        moment=moment.value,
        cancel_on_interruption=cancel,
        run=run,
        refund_id=refund_id,
        ledger_count=ledger["count"],
        tool_result_in_context=tool_result(context),
        interrupted_at=interrupted_at,
        received_at=rows[0]["received_at"] if rows else None,
        committed_at=rows[0]["committed_at"] if rows else None,
        llm=llm_kind if llm_kind == "scripted" else model,
        tool_called=tool_called,
        agent_reply=assistant_text(context.get_messages(), reply_from),
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--out")
    parser.add_argument("--llm", choices=["scripted", *PROVIDERS], default="scripted")
    parser.add_argument("--model")
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--cancel", nargs="*", choices=["true", "false"], default=["true", "false"])
    args = parser.parse_args()
    if args.llm != "scripted":
        key_var, default_model = PROVIDERS[args.llm]
        args.model = args.model or default_model
        load_env(Path(".env"))
        if not os.environ.get(key_var):
            sys.exit(f"{key_var} is not set. Put it in .env (gitignored) or the environment.")
    out = args.out or ("runs/pipecat.jsonl" if args.llm == "scripted" else f"runs/pipecat-{args.model}.jsonl")

    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    from importlib.metadata import version

    pipecat_version = version("pipecat-ai")
    results = Results(Path(out), TOLD_SOURCE)
    with ledger(results.db, COMMIT_DELAY_MS) as port:
        for cancel in [c == "true" for c in args.cancel]:
            for moment in [Moment(m) for m in args.moments]:
                for run in range(args.runs):
                    result = await one_run(
                        port, moment, cancel, run, pipecat_version, args.llm, args.model
                    )
                    results.add(result)
                    reply = f'  agent: "{result.agent_reply}"' if args.llm != "scripted" else ""
                    print(f"cancel={cancel!s:5} {moment.value:12} #{run}  {result.verdict}{reply}", flush=True)
    print(f"wrote {results.count} runs to {out}")


if __name__ == "__main__":
    asyncio.run(main())
