"""Say "stop" to a LiveKit agent at three moments during a refund tool call.

The LiveKit twin of pipecat_run.py: same prompts, same tool timing, same fake
ledger. The scripted LLM makes the same tool call every run, so only the moment
of the interruption varies. A real OpenAI model shows what the agent then tells
the caller (through the Responses API, since gpt-6.1-sol rejects tools on chat
completions). Each run records what the ledger committed, every tool output
LiveKit wrote into the session history, the tool output the model actually saw
when it answered the stop, and the agent's replies after the stop.

LiveKit has no cancel_on_interruption. An interrupt waits up to 5 s
(INTERRUPTION_TIMEOUT) for a running tool, so the matrix varies the ledger's
commit delay instead: 400 ms finishes inside that window, 6000 ms outlives it.

    python livekit_run.py --runs 10
    python livekit_run.py --llm openai --runs 5     # needs OPENAI_API_KEY in .env
    python livekit_run.py --llm openai --runs 5 --no-chain
"""

import argparse
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from importlib.metadata import version
from pathlib import Path

from livekit.agents import Agent, AgentSession, function_tool, llm
from livekit.agents.utils import http_context
from livekit.agents.voice.generation import _RUNNING_TOOL_PLACEHOLDER

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
    Told,
    free_port,
    load_env,
    reach_moment,
    verdict,
)

SCRIPTED_REPLY = "Okay."
FINAL_SUFFIX = "_final"
# Snapshotted in llm_node: the second call answers the stop, or with no stop, the tool's result.
TOLD_SOURCE = "llm_node"


@dataclass
class RunResult:
    framework: str
    livekit_version: str
    moment: str
    commit_delay_ms: int
    run: int
    refund_id: str
    ledger_count: int
    tool_outputs: list[str]
    reply_context: list[str]
    placeholder_seen: bool
    interrupted_at: float | None
    received_at: float | None
    committed_at: float | None
    llm: str = "scripted"
    tool_called: bool = True
    agent_reply: str | None = None
    followup_reply: str | None = None
    chained: bool | None = None

    @property
    def landed(self) -> bool:
        return self.ledger_count > 0

    @property
    def told(self) -> Told:
        return told_from(self.__dict__)

    @property
    def verdict(self) -> str:
        return verdict(self.tool_called, self.landed, Moment(self.moment), self.told)


def told_from(record: dict) -> Told:
    if record["placeholder_seen"]:
        return Told.PLACEHOLDER
    return Told.RESULT if record["reply_context"] else Told.NOTHING


class RefundAgent(Agent):
    def __init__(self, url: str, refund_id: str, marks: Marks, scripted: bool):
        super().__init__(instructions=SYSTEM_PROMPT)
        self._url = url
        self._refund_id = refund_id
        self._marks = marks
        self._scripted = scripted
        self._called = False
        self.contexts: list[list[str]] = []

    @function_tool(description=TOOL_DESCRIPTION)
    async def issue_refund(self, amount_cents: int) -> dict:
        """
        Args:
            amount_cents: Amount in cents.
        """
        return await refund.issue_refund(self._url, self._refund_id, amount_cents, self._marks)

    async def llm_node(self, chat_ctx, tools, model_settings):
        # LiveKit injects "still in progress" outputs into this ctx without persisting them,
        # so session.history alone can't show what the model was told.
        self.contexts.append(tool_outputs(chat_ctx.items))
        if not self._scripted:
            async for chunk in Agent.default.llm_node(self, chat_ctx, tools, model_settings):
                yield chunk
            return
        if self._called:
            yield SCRIPTED_REPLY
            return
        self._called = True
        call = llm.FunctionToolCall(
            name="issue_refund", arguments='{"amount_cents": 4000}', call_id=f"call_{self._refund_id}"
        )
        yield llm.ChatChunk(id=self._refund_id, delta=llm.ChoiceDelta(role="assistant", tool_calls=[call]))


class NoNetworkLLM(llm.LLM):
    """AgentSession needs an LLM to run llm_node; the scripted agent never calls this one."""

    def chat(self, **kwargs):
        raise RuntimeError("the scripted agent overrides llm_node, so chat() must never run")


def make_llm(kind: str, model: str, chain: bool) -> llm.LLM:
    if kind == "scripted":
        return NoNetworkLLM()
    from livekit.plugins import openai

    if chain:
        return openai.responses.LLM(model=model)
    # store=False stops the plugin chaining with previous_response_id. Chained requests in 1.8.8
    # re-send the function_call the previous response already holds, so the model sees it twice.
    return openai.responses.LLM(model=model, store=False)


def tool_outputs(items: list) -> list[str]:
    return [f"{i.call_id}: {i.output}" for i in items if i.type == "function_call_output"]


def assistant_text(items: list) -> str | None:
    parts = [i.text_content for i in items if i.type == "message" and i.role == "assistant"]
    text = " ".join(p.strip() for p in parts if p and p.strip())
    return text or None


def final_index(items: list) -> int | None:
    return next(
        (n for n, i in enumerate(items)
         if i.type == "function_call_output" and i.call_id.endswith(FINAL_SUFFIX)),
        None,
    )


async def poll(done, timeout: float = LLM_TIMEOUT_S):
    deadline = time.time() + timeout
    while time.time() < deadline and not done():
        await asyncio.sleep(0.25)


def split_replies(items: list, stopped: bool) -> tuple[str | None, str | None]:
    """Assistant text after the stop (or after the ask, with no stop), and after a _final result."""
    start = 0
    if stopped:
        start = next(
            (n for n, i in enumerate(items) if i.type == "message" and i.role == "user"
             and i.text_content == USER_STOP),
            len(items),
        )
    final = final_index(items)
    if final is None or final < start:
        return assistant_text(items[start:]), None
    return assistant_text(items[start:final]), assistant_text(items[final:])


async def one_run(
    port: int, moment: Moment, delay_ms: int, run: int, lk_version: str, llm_kind: str, model: str,
    chain: bool = True,
) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    marks = Marks()
    url = f"http://127.0.0.1:{port}/refunds"
    agent = RefundAgent(url, refund_id, marks, scripted=llm_kind == "scripted")
    interrupted_at = None
    stopped = moment is not Moment.NONE

    # http_context stands in for the job worker, which normally owns the plugin's HTTP session
    async with http_context.open(), AgentSession(
        llm=make_llm(llm_kind, model, chain), vad=None, turn_handling={"turn_detection": "manual"}
    ) as session:
        await session.start(agent)
        session.generate_reply(user_input=USER_ASK)
        tool_called = await reach_moment(marks, moment)
        if tool_called and stopped:
            interrupted_at = time.time()
            # the same three steps as LiveKit's text-input callback (voice/room_io/types.py:46-49)
            async with session._claim_user_turn():
                await session.interrupt()
                session.generate_reply(user_input=USER_STOP)
        await asyncio.sleep(PREP_S + delay_ms / 1000 + AFTER_COMMIT_S + SETTLE_S)
        await poll(lambda: split_replies(session.history.items, stopped)[0])
        await poll(
            lambda: final_index(session.history.items) is None
            or split_replies(session.history.items, stopped)[1]
        )
        if llm_kind != "scripted":
            await asyncio.sleep(SETTLE_S)
        items = list(session.history.items)

    ledger = await refund.read_ledger(url, refund_id)
    rows = ledger["rows"]
    agent_reply, followup_reply = split_replies(items, stopped)
    outputs = tool_outputs(items)
    reply_context = agent.contexts[1] if len(agent.contexts) > 1 else []
    return RunResult(
        framework="livekit",
        livekit_version=lk_version,
        moment=moment.value,
        commit_delay_ms=delay_ms,
        run=run,
        refund_id=refund_id,
        ledger_count=ledger["count"],
        tool_outputs=outputs,
        reply_context=reply_context,
        placeholder_seen=any(_RUNNING_TOOL_PLACEHOLDER in o for o in outputs + reply_context),
        interrupted_at=interrupted_at,
        received_at=rows[0]["received_at"] if rows else None,
        committed_at=rows[0]["committed_at"] if rows else None,
        llm=llm_kind if llm_kind == "scripted" else model,
        tool_called=tool_called,
        agent_reply=agent_reply,
        followup_reply=followup_reply,
        chained=None if llm_kind == "scripted" else chain,
    )


def start_ledger(delay_ms: int) -> tuple[subprocess.Popen, int]:
    port = free_port()
    ledger = subprocess.Popen(
        [sys.executable, "ledger.py", "--port", str(port), "--db", "runs/ledger-livekit.db",
         "--commit-delay-ms", str(delay_ms)],
        stdout=subprocess.PIPE,
    )
    ledger.stdout.readline()
    return ledger, port


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--out")
    parser.add_argument("--llm", choices=["scripted", "openai"], default="scripted")
    parser.add_argument("--model")
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--commit-delay-ms", nargs="*", type=int, default=[400, 6000])
    parser.add_argument("--no-chain", action="store_true", help="don't chain OpenAI requests (store=False)")
    args = parser.parse_args()
    if args.llm == "openai":
        args.model = args.model or "gpt-6.1-sol"
        load_env(Path(".env"))
        if not os.environ.get("OPENAI_API_KEY"):
            sys.exit("OPENAI_API_KEY is not set. Put it in .env (gitignored) or the environment.")
    suffix = "-unchained" if args.no_chain else ""
    out = args.out or ("runs/livekit.jsonl" if args.llm == "scripted" else f"runs/livekit-{args.model}{suffix}.jsonl")

    logging.basicConfig(level=logging.WARNING)
    lk_version = version("livekit-agents")
    Path("runs").mkdir(exist_ok=True)
    Path("runs/ledger-livekit.db").unlink(missing_ok=True)

    results = []
    for delay_ms in args.commit_delay_ms:
        ledger, port = start_ledger(delay_ms)
        try:
            for moment in [Moment(m) for m in args.moments]:
                for run in range(args.runs):
                    result = await one_run(
                        port, moment, delay_ms, run, lk_version, args.llm, args.model, not args.no_chain
                    )
                    results.append(result)
                    reply = f'  agent: "{result.agent_reply}"' if args.llm != "scripted" else ""
                    print(f"delay={delay_ms:<5} {moment.value:12} #{run}  {result.verdict}{reply}", flush=True)
        finally:
            ledger.terminate()

    with open(out, "w") as f:
        for r in results:
            f.write(json.dumps({
                **asdict(r), "landed": r.landed, "told": r.told.value, "told_source": TOLD_SOURCE,
                "verdict": r.verdict,
            }) + "\n")
    print(f"wrote {len(results)} runs to {out}")


if __name__ == "__main__":
    asyncio.run(main())
