"""Say "stop" to a Pipecat agent at three moments during a refund tool call.

The LLM is scripted, so every run makes the same tool call. What varies is
when the interruption lands. Each run records what the ledger committed and
what Pipecat wrote into the LLM context as the tool's result, which is what
the model will believe when it answers the caller.

    python pipecat_run.py --runs 10
"""

import argparse
import asyncio
import enum
import json
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

import aiohttp
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
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import LLMContextAggregatorPair
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.llm_service import FunctionCallParams, LLMService
from pipecat.services.settings import LLMSettings
from pipecat.workers.runner import WorkerRunner

PREP_S = 0.2
AFTER_COMMIT_S = 0.2
COMMIT_DELAY_MS = 400
IN_FLIGHT_OFFSET_S = 0.05
SETTLE_S = 1.0


class Moment(enum.Enum):
    NONE = "none"
    BEFORE_SEND = "before_send"
    IN_FLIGHT = "in_flight"
    AFTER_COMMIT = "after_commit"


@dataclass
class Marks:
    started: asyncio.Event = field(default_factory=asyncio.Event)
    sent: asyncio.Event = field(default_factory=asyncio.Event)
    committed: asyncio.Event = field(default_factory=asyncio.Event)


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

    @property
    def landed(self) -> bool:
        return self.ledger_count > 0

    @property
    def told_cancelled(self) -> bool:
        return self.tool_result_in_context is not None and "CANCEL" in self.tool_result_in_context.upper()

    @property
    def verdict(self) -> str:
        if self.landed and self.told_cancelled:
            return "LIE: refund landed, model told cancelled"
        if self.landed and self.tool_result_in_context is None:
            return "BLIND: refund landed, model told nothing"
        if self.landed and self.moment == Moment.BEFORE_SEND.value:
            return "IGNORED: stop came before the request, refund landed anyway"
        if self.landed:
            return "ok: landed, model told landed"
        if self.told_cancelled:
            return "ok: stopped, model told cancelled"
        return "BLIND: nothing landed, model told nothing"


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


def tool_result(context: LLMContext) -> str | None:
    for message in context.get_messages():
        if isinstance(message, dict) and message.get("role") == "tool":
            content = message.get("content")
            return content if isinstance(content, str) else json.dumps(content)
    return None


async def one_run(port: int, moment: Moment, cancel: bool, run: int, version: str) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    marks = Marks()
    url = f"http://127.0.0.1:{port}/refunds"

    async def issue_refund(params: FunctionCallParams):
        marks.started.set()
        await asyncio.sleep(PREP_S)
        async with aiohttp.ClientSession() as session:
            marks.sent.set()
            async with session.post(url, json={"refund_id": refund_id, "amount_cents": 4000}) as resp:
                body = await resp.json()
        marks.committed.set()
        await asyncio.sleep(AFTER_COMMIT_S)
        await params.result_callback(body)

    tools = ToolsSchema(
        standard_tools=[
            FunctionSchema(
                name="issue_refund",
                description="Refund the caller.",
                properties={"amount_cents": {"type": "integer"}},
                required=["amount_cents"],
            )
        ]
    )
    context = LLMContext(messages=[{"role": "system", "content": "You issue refunds."}], tools=tools)
    aggregators = LLMContextAggregatorPair(context)
    llm = ScriptedLLM(refund_id)
    llm.register_function("issue_refund", issue_refund, cancel_on_interruption=cancel)

    task = PipelineWorker(
        Pipeline([aggregators.user(), llm, aggregators.assistant()]), cancel_on_idle_timeout=False
    )
    interrupted_at = None

    async def drive():
        nonlocal interrupted_at
        await task.queue_frame(
            LLMMessagesAppendFrame(
                messages=[{"role": "user", "content": "Refund my forty dollars."}], run_llm=True
            )
        )
        if moment is Moment.BEFORE_SEND:
            await marks.started.wait()
        elif moment is Moment.IN_FLIGHT:
            await marks.sent.wait()
            await asyncio.sleep(IN_FLIGHT_OFFSET_S)
        elif moment is Moment.AFTER_COMMIT:
            await marks.committed.wait()
        if moment is not Moment.NONE:
            interrupted_at = time.time()
            await task.queue_frame(InterruptionFrame())
        await asyncio.sleep(PREP_S + COMMIT_DELAY_MS / 1000 + AFTER_COMMIT_S + SETTLE_S)
        await task.queue_frame(EndFrame())

    runner = WorkerRunner(handle_sigint=False)
    await runner.add_workers(task)
    await asyncio.gather(runner.run(), drive())

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{url}/{refund_id}") as resp:
            ledger = await resp.json()
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
    )


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--out", default="runs/pipecat.jsonl")
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--cancel", nargs="*", default=["true", "false"])
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    from importlib.metadata import version

    pipecat_version = version("pipecat-ai")
    port = free_port()
    Path("runs").mkdir(exist_ok=True)
    db = Path("runs/ledger.db")
    db.unlink(missing_ok=True)
    ledger = subprocess.Popen(
        [sys.executable, "ledger.py", "--port", str(port), "--db", str(db),
         "--commit-delay-ms", str(COMMIT_DELAY_MS)],
        stdout=subprocess.PIPE,
    )
    ledger.stdout.readline()

    results = []
    try:
        for cancel in [c == "true" for c in args.cancel]:
            for moment in [Moment(m) for m in args.moments]:
                for run in range(args.runs):
                    result = await one_run(port, moment, cancel, run, pipecat_version)
                    results.append(result)
                    print(f"cancel={cancel!s:5} {moment.value:12} #{run}  {result.verdict}", flush=True)
    finally:
        ledger.terminate()

    with open(args.out, "w") as f:
        for r in results:
            f.write(json.dumps({**asdict(r), "landed": r.landed, "verdict": r.verdict}) + "\n")
    print(f"wrote {len(results)} runs to {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
