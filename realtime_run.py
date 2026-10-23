"""Say "stop" to an OpenAI Agents SDK realtime agent while its refund tool runs.

There is no scripted mode: the SDK's scripted test model has no response.create sequencer,
so it can't show the ordering this runner measures.

The SDK never cancels a running tool on interrupt, and always sends the tool's result with
a request for a new response. Whether the model has that result when it answers the stop
comes down to one setting, async_tool_calls:

- True (default): the tool runs in the background. The stop gets its own response at once,
  while the call is still open; when the result lands, a second response starts unprompted.
- False: the tool runs inline and blocks the websocket reader, so the stop's
  response.create waits behind the call's response; the SDK then merges it with the
  result's, and the model answers once, seeing the stop and then the result.

    python realtime_run.py --runs 5                       # needs OPENAI_API_KEY in .env
    python realtime_run.py --runs 5 --async-tools true
    python realtime_run.py --runs 2 --commit-delay-ms 400 6000
"""

import argparse
import asyncio
import logging
import os
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Literal

from agents import FunctionTool, function_tool
from agents.realtime import (
    OpenAIRealtimeWebSocketModel,
    RealtimeAgent,
    RealtimeModelListener,
    RealtimeRunner,
)
from agents.realtime.model_events import RealtimeModelEvent

import refund
from stopbench import (
    LLM_TIMEOUT_S,
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

DEFAULT_MODEL = "gpt-realtime-2.1"


ItemKind = Literal["user", "assistant", "function_call", "function_call_output", "other"]
ResponseStatus = Literal["completed", "cancelled", "incomplete", "failed"]


@dataclass(frozen=True)
class Item:
    # With async_tool_calls=False the tool blocks the reader, so events behind it are
    # received late; only the order says what the model had.
    t: float
    item_id: str
    kind: ItemKind
    call_id: str | None
    # Assistant text and call arguments stream in after the item is added; ResponseEnd has them.
    text: str
    event: Literal["item"] = "item"


@dataclass(frozen=True)
class ResponseStart:
    t: float
    response_id: str
    event: Literal["response_start"] = "response_start"


@dataclass(frozen=True)
class ResponseEnd:
    t: float
    response_id: str
    status: ResponseStatus
    text: str | None
    calls: tuple[str, ...]
    event: Literal["response_end"] = "response_end"


@dataclass(frozen=True)
class ServerError:
    t: float
    message: str
    event: Literal["error"] = "error"


Wire = Item | ResponseStart | ResponseEnd | ServerError
WIRE_TYPES: dict[str, type] = {cls.event: cls for cls in (Item, ResponseStart, ResponseEnd, ServerError)}

MESSAGE_KINDS = {"user": "user", "assistant": "assistant"}


def message_text(content: list[dict[str, Any]]) -> str:
    return "".join(part.get("text") or "" for part in content)


def parse_server_event(raw: dict[str, Any], t: float) -> Wire | None:
    match raw["type"]:
        case "conversation.item.added":
            item = raw["item"]
            match item["type"]:
                case "message":
                    kind = MESSAGE_KINDS.get(item["role"], "other")
                    return Item(t, item["id"], kind, None, message_text(item.get("content") or []))
                case "function_call":
                    return Item(t, item["id"], "function_call", item["call_id"], item["name"])
                case "function_call_output":
                    return Item(t, item["id"], "function_call_output", item["call_id"], item["output"])
                case _:
                    return Item(t, item["id"], "other", None, "")
        case "response.created":
            return ResponseStart(t, raw["response"]["id"])
        case "response.done":
            response = raw["response"]
            output = response.get("output") or []
            messages = [o for o in output if o["type"] == "message"]
            text = " ".join(message_text(m.get("content") or []) for m in messages).strip()
            calls = tuple(f"{o['name']}({o['arguments']})" for o in output if o["type"] == "function_call")
            return ResponseEnd(t, response["id"], response["status"], text or None, calls)
        case "error":
            return ServerError(t, raw["error"].get("message", ""))
    return None


def load_wire(rows: list[dict[str, Any]]) -> list[Wire]:
    return [
        WIRE_TYPES[row["event"]](**({**row, "calls": tuple(row["calls"])} if "calls" in row else row))
        for row in rows
    ]


class WireTap(RealtimeModelListener):
    def __init__(self) -> None:
        self.events: list[Wire] = []
        self._changed = asyncio.Event()

    async def on_event(self, event: RealtimeModelEvent) -> None:
        if event.type == "raw_server_event":
            wire = parse_server_event(event.data, time.time())
            if wire:
                self.events.append(wire)
                self._changed.set()

    async def settled(self, settle_s: float = SETTLE_S, timeout_s: float = LLM_TIMEOUT_S) -> None:
        """A reply the model makes on its own after the tool's result lands starts only after
        the stop's reply ends, so quiet must hold for settle_s before this returns.
        """
        deadline = time.time() + timeout_s
        while (left := deadline - time.time()) > 0:
            self._changed.clear()
            quiet = is_quiet(self.events)
            try:
                await asyncio.wait_for(self._changed.wait(), min(settle_s, left) if quiet else left)
            except TimeoutError:
                if quiet:
                    return


def is_quiet(wire: Sequence[Wire]) -> bool:
    has_output = any(isinstance(e, Item) and e.kind == "function_call_output" for e in wire)
    started = {e.response_id for e in wire if isinstance(e, ResponseStart)}
    ended = {e.response_id for e in wire if isinstance(e, ResponseEnd)}
    return has_output and started <= ended


@dataclass(frozen=True)
class Reading:
    tool_outputs: list[str]
    reply_context: list[str]
    agent_reply: str | None
    followup_reply: str | None
    reply_status: ResponseStatus | None
    errors: list[str]


def read_wire(wire: Sequence[Wire], stopped: bool) -> Reading:
    """The server applies client events in the order they arrive on the one websocket and
    announces each conversation change before anything that depends on it. So an Item that
    comes before a ResponseStart was in the conversation that response read. The order
    decides; the receive times only annotate.
    """
    items: dict[str, tuple[int, Item]] = {}
    for i, e in enumerate(wire):
        if isinstance(e, Item):
            items.setdefault(e.item_id, (i, e))
    outputs = [(i, f"{e.call_id}: {e.text}") for i, e in items.values() if e.kind == "function_call_output"]
    tool_outputs = [o for _, o in outputs]
    errors = [e.message for e in wire if isinstance(e, ServerError)]

    if stopped:
        anchor = next((i for i, e in items.values() if e.kind == "user" and e.text == USER_STOP), None)
    else:
        anchor = outputs[0][0] if outputs else None
    starts = [] if anchor is None else [
        (i, e) for i, e in enumerate(wire) if isinstance(e, ResponseStart) and i > anchor
    ]
    if not starts:
        return Reading(tool_outputs, [], None, None, None, errors)

    ends = {e.response_id: e for e in wire if isinstance(e, ResponseEnd)}
    reply_at, reply = starts[0]
    reply_end = ends.get(reply.response_id)
    later = [end.text for _, s in starts[1:] if (end := ends.get(s.response_id)) and end.text]
    return Reading(
        tool_outputs=tool_outputs,
        reply_context=[o for i, o in outputs if i < reply_at],
        agent_reply=reply_end.text if reply_end else None,
        followup_reply=" ".join(later) or None,
        reply_status=reply_end.status if reply_end else None,
        errors=errors,
    )


TOLD_SOURCE = "server_event_order"


def told_from(record: dict) -> Told:
    if record["moment"] != Moment.NONE.value and record["reply_status"] is None:
        return Told.NO_ANSWER
    return Told.RESULT if record["reply_context"] else Told.NOTHING


def rederive(record: dict) -> dict:
    """The derived fields, rebuilt from the evidence the record stores."""
    return asdict(read_wire(load_wire(record["wire"]), record["moment"] != Moment.NONE.value))


@dataclass
class RunResult:
    framework: str
    openai_agents_version: str
    moment: str
    async_tool_calls: bool
    commit_delay_ms: int
    run: int
    refund_id: str
    ledger_count: int
    interrupted_at: float | None
    received_at: float | None
    committed_at: float | None
    llm: str
    tool_called: bool
    tool_outputs: list[str]
    reply_context: list[str]
    agent_reply: str | None
    followup_reply: str | None
    reply_status: str | None
    errors: list[str]
    wire: list[Wire]

    @property
    def landed(self) -> bool:
        return self.ledger_count > 0

    @property
    def told(self) -> Told:
        return told_from(self.__dict__)

    @property
    def verdict(self) -> str:
        return verdict(self.tool_called, self.ledger_count, Moment(self.moment), self.told)


def refund_tool(url: str, refund_id: str, marks: Marks) -> FunctionTool:
    @function_tool(name_override="issue_refund", description_override=TOOL_DESCRIPTION)
    async def tool(amount_cents: int) -> dict:
        """
        Args:
            amount_cents: Amount in cents.
        """
        return await refund.issue_refund(url, refund_id, amount_cents, marks)

    return tool


async def one_run(
    port: int, moment: Moment, async_tools: bool, delay_ms: int, run: int, sdk_version: str, model: str
) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    marks = Marks()
    url = f"http://127.0.0.1:{port}/refunds"
    interrupted_at = None
    stopped = moment is not Moment.NONE

    tap = WireTap()
    transport = OpenAIRealtimeWebSocketModel()
    # Added before the session connects, so for every event the tap runs before the
    # session's own listener.
    transport.add_listener(tap)
    agent = RealtimeAgent(name="support", instructions=SYSTEM_PROMPT, tools=[refund_tool(url, refund_id, marks)])
    runner = RealtimeRunner(agent, model=transport, config={
        "model_settings": {"model_name": model, "output_modalities": ["text"]},
        "async_tool_calls": async_tools,
    })
    async with await runner.run() as session:
        await session.send_message(USER_ASK)
        tool_called = await reach_moment(marks, moment)
        if tool_called and stopped:
            interrupted_at = time.time()
            # Sends nothing on the wire here: in a text session the server's turn detection
            # has interrupt_response on, so the SDK leaves cancelling to it. Kept because it
            # is what an app calls; a cancel would show as the reply's status.
            await session.interrupt()
            await session.send_message(USER_STOP)
        if tool_called:
            # The tool must finish before close, or the SDK drops its output.
            await tap.settled()

    ledger = await refund.read_ledger(url, refund_id)
    rows = ledger["rows"]
    return RunResult(
        framework="openai-agents-realtime",
        openai_agents_version=sdk_version,
        moment=moment.value,
        async_tool_calls=async_tools,
        commit_delay_ms=delay_ms,
        run=run,
        refund_id=refund_id,
        ledger_count=ledger["count"],
        interrupted_at=interrupted_at,
        received_at=rows[0]["received_at"] if rows else None,
        committed_at=rows[0]["committed_at"] if rows else None,
        llm=model,
        tool_called=tool_called,
        **asdict(read_wire(tap.events, stopped)),
        wire=list(tap.events),
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--async-tools", nargs="*", choices=["true", "false"], default=["true", "false"])
    parser.add_argument("--commit-delay-ms", nargs="*", type=int, default=[400])
    args = parser.parse_args()
    load_env(Path(".env"))
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("OPENAI_API_KEY is not set. Put it in .env (gitignored) or the environment.")
    out = args.out or f"runs/realtime-{args.model}.jsonl"

    logging.basicConfig(level=logging.WARNING)
    sdk_version = version("openai-agents")
    results = Results(Path(out), TOLD_SOURCE)
    for delay_ms in args.commit_delay_ms:
        with ledger(results.db, delay_ms) as port:
            for async_tools in [a == "true" for a in args.async_tools]:
                for moment in [Moment(m) for m in args.moments]:
                    for run in range(args.runs):
                        result = await one_run(port, moment, async_tools, delay_ms, run, sdk_version, args.model)
                        results.add(result)
                        later = f'  later: "{result.followup_reply}"' if result.followup_reply else ""
                        print(f"async={async_tools!s:5} delay={delay_ms:<5} {moment.value:12} #{run}  "
                              f'{result.verdict}  agent: "{result.agent_reply}"{later}', flush=True)
    print(f"wrote {results.count} runs to {out}")


if __name__ == "__main__":
    asyncio.run(main())
