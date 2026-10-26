"""Say "stop" to a Gemini Live agent while its refund tool runs.

Function calling in the Live API is client-side: the server sends tool_call on the websocket
and waits for send_tool_response (https://ai.google.dev/gemini-api/docs/live-tools). So the
refund runs in this process.

No Live model this API key lists accepts response_modalities TEXT (gemini-3.8-live,
gemini-3.1-flash-live-preview and gemini-2.5-flash-native-audio-latest all refuse it with
close code 1007), so the session answers in AUDIO with output_audio_transcription on, and
the agent's reply is read from the transcription. The caller's turns are text.

The stop is send_client_content with turn_complete=True. The API reference says a client
content message "will interrupt any current model generation", and the gemini-3.8-live page
says turn_complete=true "unconditionally interrupts active model generation". Realtime text
input is documented as sent "without interruption to model generation", so it is not the
stop (https://ai.google.dev/api/live, https://ai.google.dev/gemini-api/docs/models/gemini-3.8-live).

The axis is the function declaration's behavior, the one setting the docs give for what
a call does to the session while it runs:

- NON_BLOCKING (default on gemini-3.8-live, per its model page): the call's turn ends at once,
  the stop gets its own reply while the call is open, and the result triggers a later reply.
- BLOCKING: the call's turn stays open until its response. The stop interrupts that turn
  and the server drops the call from the model's context. The response sent afterwards
  changes nothing, and no tool_call_cancellation comes, although the API reference says it
  is sent when a client interrupts a server turn. The model then either says it stopped
  the refund, or calls issue_refund again, which lands a second refund.

What the model had is read from the order of messages on the websocket (read_wire). Each
server turn's usage_metadata is kept in the record as a check: the stop adds 11 prompt text
tokens, the call 13 and its result 15, so a reply's prompt size shows whether the result or
the call was in it. That check agreed with the order rule in every run so
far, including the ones where the response went out after the stop but before the reply's
first word. The order rule could be wrong if the server ever folded a response that came
after the stop into the reply's prompt; the token counts would show it. A BLOCKING turn
that runs a second call reports one usage summed over its steps, so it can't check that case.

The other setting is a NON_BLOCKING response's scheduling (INTERRUPT, WHEN_IDLE, SILENT),
which decides when the result is reported, not what the stop's reply has. It is left unset.
When tool_call_cancellation does come, it is recorded and the tool keeps running, since the
request may already be out; its response is still sent, because the docs only say a client
"may attempt to undo" the call.

    python gemini_run.py --runs 5                       # needs GOOGLE_API_KEY in .env
    python gemini_run.py --runs 5 --behavior BLOCKING
    python gemini_run.py --runs 2 --commit-delay-ms 400 6000
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

from google import genai
from google.genai import types

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

DEFAULT_MODEL = "gemini-3.8-live"
BEHAVIORS = ["NON_BLOCKING", "BLOCKING"]
# The model thinks for up to ~0.9 s before its first word, so one SETTLE_S of quiet can end
# a run before a reply the tool's result triggers.
QUIET_S = 2 * SETTLE_S


@dataclass(frozen=True)
class Sent:
    t: float
    what: Literal["ask", "stop", "tool_response"]
    call_id: str | None
    text: str
    event: Literal["sent"] = "sent"


@dataclass(frozen=True)
class ToolCall:
    t: float
    call_id: str
    args: str
    event: Literal["tool_call"] = "tool_call"


@dataclass(frozen=True)
class ToolCancel:
    t: float
    call_ids: tuple[str, ...]
    event: Literal["tool_cancel"] = "tool_cancel"


@dataclass(frozen=True)
class Speech:
    t: float
    text: str
    event: Literal["speech"] = "speech"


@dataclass(frozen=True)
class TurnEnd:
    t: float
    how: Literal["turn_complete", "interrupted"]
    event: Literal["turn_end"] = "turn_end"


@dataclass(frozen=True)
class Usage:
    t: float
    prompt_text_tokens: int | None
    prompt_audio_tokens: int | None
    response_tokens: int | None
    event: Literal["usage"] = "usage"


@dataclass(frozen=True)
class SessionError:
    t: float
    message: str
    event: Literal["error"] = "error"


Wire = Sent | ToolCall | ToolCancel | Speech | TurnEnd | Usage | SessionError
WIRE_TYPES: dict[str, type] = {
    cls.event: cls for cls in (Sent, ToolCall, ToolCancel, Speech, TurnEnd, Usage, SessionError)
}


def load_wire(rows: list[dict[str, Any]]) -> list[Wire]:
    return [
        WIRE_TYPES[row["event"]](**({**row, "call_ids": tuple(row["call_ids"])} if "call_ids" in row else row))
        for row in rows
    ]


def modality_tokens(details: list[types.ModalityTokenCount] | None, modality: types.MediaModality) -> int | None:
    return next((d.token_count for d in details or [] if d.modality == modality), None)


def parse_server_message(m: types.LiveServerMessage, t: float) -> list[Wire]:
    out: list[Wire] = []
    if m.tool_call:
        out += [ToolCall(t, fc.id, str(fc.args)) for fc in m.tool_call.function_calls or []]
    if m.tool_call_cancellation:
        out.append(ToolCancel(t, tuple(m.tool_call_cancellation.ids or [])))
    if sc := m.server_content:
        if sc.output_transcription and sc.output_transcription.text:
            out.append(Speech(t, sc.output_transcription.text))
        if sc.interrupted:
            out.append(TurnEnd(t, "interrupted"))
        if sc.turn_complete:
            out.append(TurnEnd(t, "turn_complete"))
    if u := m.usage_metadata:
        out.append(Usage(
            t,
            modality_tokens(u.prompt_tokens_details, types.MediaModality.TEXT),
            modality_tokens(u.prompt_tokens_details, types.MediaModality.AUDIO),
            u.response_token_count,
        ))
    return out


@dataclass(frozen=True)
class Reading:
    tool_outputs: list[str]
    reply_context: list[str]
    agent_reply: str | None
    followup_reply: str | None
    reply_status: Literal["turn_complete", "interrupted"] | None
    errors: list[str]


def speech_turns(wire: Sequence[Wire]) -> list[tuple[int, str, str | None]]:
    """(index of the first word, text, how the turn ended) for each turn the model spoke in."""
    turns, start, words = [], None, []
    for i, e in enumerate(wire):
        if isinstance(e, Speech):
            start = i if start is None else start
            words.append(e.text)
        elif isinstance(e, TurnEnd) and start is not None:
            turns.append((start, "".join(words).strip(), e.how))
            start, words = None, []
    if start is not None:
        turns.append((start, "".join(words).strip(), None))
    return turns


def read_wire(wire: Sequence[Wire], stopped: bool) -> Reading:
    """The server applies client messages in the order they arrive on the one websocket,
    and starts the stop's reply as soon as it applies the stop. So the reply's context holds
    the tool responses sent before the stop. If the model calls the tool again before it
    speaks, the reply waits for that call, and its context holds what was sent up to that
    call's response. A BLOCKING call whose turn the stop interrupted is gone from the
    context, so a response sent for it after the interruption is not counted.
    """
    responses = [(i, e) for i, e in enumerate(wire) if isinstance(e, Sent) and e.what == "tool_response"]
    tool_outputs = [f"{e.call_id}: {e.text}" for _, e in responses]
    errors = [e.message for e in wire if isinstance(e, SessionError)]

    if stopped:
        anchor = next((i for i, e in enumerate(wire) if isinstance(e, Sent) and e.what == "stop"), None)
    else:
        anchor = responses[0][0] if responses else None
    turns = [] if anchor is None else [t for t in speech_turns(wire) if t[0] > anchor]
    if not turns:
        return Reading(tool_outputs, [], None, None, None, errors)

    reply_at, reply_text, reply_status = turns[0]
    calls_in_between = {e.call_id for e in wire[anchor:reply_at] if isinstance(e, ToolCall)}
    trigger = max([anchor] + [i for i, e in responses if e.call_id in calls_in_between])

    def dropped(call_id: str, sent_at: int) -> bool:
        called_at = next(i for i, e in enumerate(wire) if isinstance(e, ToolCall) and e.call_id == call_id)
        ended_at, end = next(((i, e) for i, e in enumerate(wire) if i > called_at and isinstance(e, TurnEnd)),
                             (len(wire), None))
        return end is not None and end.how == "interrupted" and ended_at < sent_at

    return Reading(
        tool_outputs=tool_outputs,
        reply_context=[
            f"{e.call_id}: {e.text}" for i, e in responses if i <= trigger and not dropped(e.call_id, i)
        ],
        agent_reply=reply_text or None,
        followup_reply=" ".join(text for _, text, _ in turns[1:] if text) or None,
        reply_status=reply_status,
        errors=errors,
    )


TOLD_SOURCE = "socket_order"


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
    google_genai_version: str
    moment: str
    behavior: str
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


def refund_declaration(behavior: str) -> types.FunctionDeclaration:
    return types.FunctionDeclaration(
        name="issue_refund",
        description=TOOL_DESCRIPTION,
        parameters=types.Schema(
            type=types.Type.OBJECT,
            properties={"amount_cents": types.Schema(type=types.Type.INTEGER, description="Amount in cents.")},
            required=["amount_cents"],
        ),
        behavior=types.Behavior(behavior),
    )


class Session:
    """One Live session. Every server message and every client send goes into one list in
    the order this process saw it, which is the websocket's order for each direction.
    """

    def __init__(self, live: Any, url: str, refund_id: str, marks: Marks) -> None:
        self.live = live
        self.url = url
        self.refund_id = refund_id
        self.marks = marks
        self.wire: list[Wire] = []
        self.tools: list[asyncio.Task] = []
        self._changed = asyncio.Event()

    def log(self, event: Wire) -> None:
        self.wire.append(event)
        self._changed.set()

    async def say(self, what: Literal["ask", "stop"], text: str) -> None:
        self.log(Sent(time.time(), what, None, text))
        await self.live.send_client_content(
            turns=types.Content(role="user", parts=[types.Part(text=text)]), turn_complete=True
        )

    async def run_tool(self, call: types.FunctionCall) -> None:
        body = await refund.issue_refund(self.url, self.refund_id, int(call.args["amount_cents"]), self.marks)
        self.log(Sent(time.time(), "tool_response", call.id, str(body)))
        await self.live.send_tool_response(
            function_responses=[types.FunctionResponse(id=call.id, name=call.name, response=body)]
        )

    async def receive(self) -> None:
        try:
            while True:
                async for m in self.live.receive():
                    for event in parse_server_message(m, time.time()):
                        self.log(event)
                    for call in (m.tool_call.function_calls or []) if m.tool_call else []:
                        self.tools.append(asyncio.create_task(self.run_tool(call)))
        except Exception as e:
            self.log(SessionError(time.time(), f"{type(e).__name__}: {e}"))

    def quiet(self, stopped: bool) -> bool:
        """Also waits for the reply itself: the model can think for over QUIET_S after the
        tool's response before its first word.
        """
        turn_events = [e for e in self.wire if isinstance(e, Speech | TurnEnd | ToolCall)]
        idle = not turn_events or isinstance(turn_events[-1], TurnEnd)
        answered = read_wire(self.wire, stopped).reply_status is not None
        return all(t.done() for t in self.tools) and idle and answered

    async def settled(self, stopped: bool, timeout_s: float = LLM_TIMEOUT_S) -> None:
        deadline = time.time() + timeout_s
        while (left := deadline - time.time()) > 0:
            self._changed.clear()
            quiet = self.quiet(stopped)
            try:
                await asyncio.wait_for(self._changed.wait(), min(QUIET_S, left) if quiet else left)
            except TimeoutError:
                if quiet:
                    return


async def one_run(
    client: genai.Client, port: int, moment: Moment, behavior: str, delay_ms: int, run: int,
    sdk_version: str, model: str,
) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    marks = Marks()
    url = f"http://127.0.0.1:{port}/refunds"
    interrupted_at = None
    stopped = moment is not Moment.NONE
    config = types.LiveConnectConfig(
        response_modalities=[types.Modality.AUDIO],
        output_audio_transcription=types.AudioTranscriptionConfig(),
        system_instruction=SYSTEM_PROMPT,
        tools=[types.Tool(function_declarations=[refund_declaration(behavior)])],
    )
    async with client.aio.live.connect(model=model, config=config) as live:
        session = Session(live, url, refund_id, marks)
        receiver = asyncio.create_task(session.receive())
        await session.say("ask", USER_ASK)
        tool_called = await reach_moment(marks, moment)
        if tool_called and stopped:
            interrupted_at = time.time()
            await session.say("stop", USER_STOP)
        if tool_called:
            await session.settled(stopped)
        receiver.cancel()
        await asyncio.gather(*session.tools)

    ledger = await refund.read_ledger(url, refund_id)
    rows = ledger["rows"]
    return RunResult(
        framework="gemini-live",
        google_genai_version=sdk_version,
        moment=moment.value,
        behavior=behavior,
        commit_delay_ms=delay_ms,
        run=run,
        refund_id=refund_id,
        ledger_count=ledger["count"],
        interrupted_at=interrupted_at,
        received_at=rows[0]["received_at"] if rows else None,
        committed_at=rows[0]["committed_at"] if rows else None,
        llm=model,
        tool_called=tool_called,
        **asdict(read_wire(session.wire, stopped)),
        wire=list(session.wire),
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--behavior", nargs="*", choices=BEHAVIORS, default=BEHAVIORS)
    parser.add_argument("--commit-delay-ms", nargs="*", type=int, default=[400])
    args = parser.parse_args()
    load_env(Path(".env"))
    if not os.environ.get("GOOGLE_API_KEY"):
        sys.exit("GOOGLE_API_KEY is not set. Put it in .env (gitignored) or the environment.")
    out = args.out or f"runs/gemini-live-{args.model}.jsonl"

    logging.basicConfig(level=logging.WARNING)
    # The SDK warns on every audio message that it has non-text parts.
    logging.getLogger("google_genai").setLevel(logging.ERROR)
    client = genai.Client(api_key=os.environ["GOOGLE_API_KEY"])
    sdk_version = version("google-genai")
    results = Results(Path(out), TOLD_SOURCE)
    for delay_ms in args.commit_delay_ms:
        with ledger(results.db, delay_ms) as port:
            for behavior in args.behavior:
                for moment in [Moment(m) for m in args.moments]:
                    for run in range(args.runs):
                        result = await one_run(client, port, moment, behavior, delay_ms, run, sdk_version, args.model)
                        results.add(result)
                        later = f'  later: "{result.followup_reply}"' if result.followup_reply else ""
                        print(f"behavior={behavior:12} delay={delay_ms:<5} {moment.value:12} #{run}  "
                              f'{result.verdict}  agent: "{result.agent_reply}"{later}', flush=True)
    print(f"wrote {results.count} runs to {out}")


if __name__ == "__main__":
    asyncio.run(main())
