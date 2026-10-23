"""Say "stop" to an ElevenLabs agent while its refund tool runs.

issue_refund is a client tool: the platform sends client_tool_call over the conversation
websocket, this process runs refund.issue_refund, and answers with client_tool_result. The
conversation is text only, and the stop is what a chat app sends when the caller types:
a user_message on the same websocket. With the default interruption mode the server answers
that with an interruption event while the tool is still running.
https://elevenlabs.io/docs/eleven-agents/api-reference/eleven-agents/websocket
https://elevenlabs.io/docs/eleven-agents/guides/chat-mode

The axis is the tool's interruption_mode, the per-tool setting the docs give for "whether
users can interrupt the agent while a tool runs":
https://elevenlabs.io/docs/eleven-agents/customization/tools/tool-configuration/tool-interruptions

- allow (default): the stop interrupts. The server writes "Tool execution was abandoned due
  to user input" as the call's result and the model answers the stop with that. The real
  result, sent a moment later, is acknowledged on the socket (agent_tool_response, status
  success) but never enters the transcript.
- disable_during_tool: the stop is held until the tool returns. The model then has the real
  result and the stop together, and answers twice: once tagged as the reply to the ask, once
  as the reply to the stop.

On an interruption the official SDKs only stop audio playback; the client tool runs on and
sends its result, as it does here.
https://github.com/elevenlabs/elevenlabs-python/blob/main/src/elevenlabs/conversational_ai/conversation.py

The other setting that changes what the model has is execution_mode (immediate by default,
or async, post_tool_speech). With async the server writes an "in_progress ... Result will be
reported later" result at once, answers the stop with that, and later adds the real result
as a "Background task completed" result that triggers a second reply. This runner keeps the
default, immediate. expects_response is true: without it the model never gets a result.

What the model had is read from the platform's post-call transcript
(GET /v1/convai/conversations/{id}): the issue_refund tool_results that come before the
transcript entry of the first reply after the stop. That reply is found on the socket, as the
first agent_response received after the stop was sent, and matched to the transcript by its
position among the agent's messages. The transcript is the platform's conversation history,
not the literal LLM request, and its order is not always time order: with
disable_during_tool it lists the stop after the reply that already answered it. The order of
tool results before a reply is what this relies on; socket times only annotate.

    python elevenlabs_run.py --runs 5                     # needs ELEVENLABS_API_KEY in .env
    python elevenlabs_run.py --runs 1 --interruption-mode allow
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import aiohttp

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

API = "https://api.elevenlabs.io"
API_ID = "elevenlabs-convai-v1"
DEFAULT_MODEL = "gpt-6.1-sol"
INTERRUPTION_MODES = ["allow", "disable_during_tool"]
ASK_ID = "stopbench-ask"
STOP_ID = "stopbench-stop"
CLIENT_EVENTS = [
    "conversation_initiation_metadata", "ping", "interruption", "agent_response", "agent_response_correction",
    "client_tool_call", "agent_tool_request", "agent_tool_response", "agent_tool_response_full_payload",
    "agent_response_complete", "client_error", "guardrail_triggered", "context_usage",
]
# Streaming parts repeat agent_response; keeping them would only bloat the record.
UNRECORDED = {"ping", "agent_chat_response_part", "internal_tentative_agent_response"}
ABANDONED = "Tool execution was abandoned due to user input"


def tool_config(interruption_mode: str) -> dict:
    return {"tool_config": {
        "type": "client",
        "name": "issue_refund",
        "description": TOOL_DESCRIPTION,
        "expects_response": True,
        "execution_mode": "immediate",
        "interruption_mode": interruption_mode,
        "parameters": {
            "type": "object",
            "properties": {"amount_cents": {"type": "integer", "description": "Amount in cents."}},
            "required": ["amount_cents"],
        },
    }}


def agent_config(name: str, tool_id: str, model: str) -> dict:
    return {"name": name, "conversation_config": {
        "agent": {"first_message": "", "prompt": {
            "prompt": SYSTEM_PROMPT, "llm": model, "tool_ids": [tool_id],
            # A cascade to a backup model would change the model under test without saying so.
            "backup_llm_config": {"preference": "disabled"},
        }},
        "conversation": {"text_only": True, "client_events": CLIENT_EVENTS},
    }}


class Platform:
    """The tools and agents this run created, so cleanup deletes exactly those."""

    def __init__(self, http: aiohttp.ClientSession):
        self.http = http
        self.agents: list[str] = []
        self.tools: list[str] = []

    async def call(self, method: str, path: str, **kwargs) -> Any:
        async with self.http.request(method, f"{API}{path}", **kwargs) as resp:
            body = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"{method} {path} -> {resp.status}: {body[:500]}")
            return json.loads(body) if body else None

    async def agent_for(self, interruption_mode: str, model: str) -> str:
        tool = await self.call("POST", "/v1/convai/tools", json=tool_config(interruption_mode))
        self.tools.append(tool["id"])
        name = f"stopbench-{interruption_mode}"
        agent = await self.call("POST", "/v1/convai/agents/create", json=agent_config(name, tool["id"], model))
        self.agents.append(agent["agent_id"])
        return agent["agent_id"]

    async def cleanup(self) -> None:
        for agent_id in self.agents:
            await self.call("DELETE", f"/v1/convai/agents/{agent_id}")
        # A deleted agent's branch still holds the tool, so a plain delete answers 409.
        for tool_id in self.tools:
            await self.call("DELETE", f"/v1/convai/tools/{tool_id}", params={"force": "true"})

    async def transcript(self, conversation_id: str) -> list[dict]:
        deadline = time.time() + LLM_TIMEOUT_S
        while True:
            conv = await self.call("GET", f"/v1/convai/conversations/{conversation_id}")
            if conv["status"] in ("done", "failed") or time.time() > deadline:
                return [
                    {k: entry.get(k) for k in ("role", "message", "time_in_call_secs", "source_event_id",
                                               "interrupted", "producing_llm", "tool_calls", "tool_results")}
                    for entry in conv["transcript"]
                ]
            await asyncio.sleep(1)


@dataclass(frozen=True)
class Reading:
    tool_outputs: list[str]
    reply_context: list[str]
    agent_reply: str | None
    followup_reply: str | None
    errors: list[str]


def read_record(wire: Sequence[dict], transcript: Sequence[dict], stopped: bool, model: str) -> Reading:
    def sent(kind: str) -> list[int]:
        return [i for i, e in enumerate(wire) if e["dir"] == "out" and e["msg"]["type"] == kind]

    tool_outputs = [wire[i]["msg"]["result"] for i in sent("client_tool_result")]
    errors = [json.dumps(e["msg"]) for e in wire if e["msg"]["type"] in ("client_error", "guardrail_triggered")]
    replies = [i for i, e in enumerate(wire) if e["dir"] == "in" and e["msg"]["type"] == "agent_response"]

    if stopped:
        anchor = next((i for i in sent("user_message") if wire[i]["msg"]["id"] == STOP_ID), None)
    else:
        anchor = next(iter(sent("client_tool_result")), None)
    after = [] if anchor is None else [i for i in replies if i > anchor]
    if not after:
        return Reading(tool_outputs, [], None, None, errors)

    def text(i: int) -> str:
        return wire[i]["msg"]["agent_response_event"]["agent_response"]

    said = [i for i, entry in enumerate(transcript) if entry["role"] == "agent" and entry["message"]]
    if len(said) != len(replies):
        errors.append(f"transcript has {len(said)} agent messages, socket had {len(replies)} agent_response")
    errors += [f"reply came from {entry['producing_llm']}" for entry in transcript
               if entry.get("producing_llm") not in (None, model)]
    k = replies.index(after[0])
    reply_at = said[k] if k < len(said) else len(transcript)
    if k < len(said) and transcript[reply_at]["message"] != text(after[0]):
        errors.append("the reply's transcript entry has different text")
    reply_context = [
        r["result_value"] for entry in transcript[:reply_at] for r in entry["tool_results"] or []
        if r["tool_name"] == "issue_refund"
    ]
    return Reading(
        tool_outputs=tool_outputs,
        reply_context=reply_context,
        agent_reply=text(after[0]),
        followup_reply=" ".join(text(i) for i in after[1:]) or None,
        errors=errors,
    )


TOLD_SOURCE = "platform_transcript_order"


def told_from(record: dict) -> Told:
    if record["moment"] != Moment.NONE.value and record["agent_reply"] is None:
        return Told.NO_ANSWER
    if not record["reply_context"]:
        return Told.NOTHING
    latest = record["reply_context"][-1]
    if latest == ABANDONED:
        return Told.CANCELLED
    if latest in record["tool_outputs"]:
        return Told.RESULT
    raise ValueError(f"unknown tool result in the transcript: {latest!r}")


def rederive(record: dict) -> dict:
    """The derived fields, rebuilt from the evidence the record stores."""
    return asdict(read_record(record["wire"], record["transcript"], record["moment"] != Moment.NONE.value, record["llm"]))


@dataclass
class RunResult:
    framework: str
    api: str
    moment: str
    interruption_mode: str
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
    errors: list[str]
    conversation_id: str
    wire: list[dict]
    transcript: list[dict]

    @property
    def landed(self) -> bool:
        return self.ledger_count > 0

    @property
    def told(self) -> Told:
        return told_from(self.__dict__)

    @property
    def verdict(self) -> str:
        return verdict(self.tool_called, self.ledger_count, Moment(self.moment), self.told)


class Conversation:
    """One websocket conversation: logs every event in arrival order and runs the client tool."""

    def __init__(self, ws: aiohttp.ClientWebSocketResponse, url: str, refund_id: str, marks: Marks):
        self.ws = ws
        self.url = url
        self.refund_id = refund_id
        self.marks = marks
        self.wire: list[dict] = []
        self.tools: list[asyncio.Task] = []
        self.conversation_id = asyncio.get_running_loop().create_future()
        self._changed = asyncio.Event()

    async def send(self, msg: dict) -> None:
        self.wire.append({"t": time.time(), "dir": "out", "msg": msg})
        await self.ws.send_json(msg)

    async def say(self, text: str, msg_id: str) -> None:
        await self.send({"type": "user_message", "text": text, "id": msg_id})

    async def read(self) -> None:
        async for frame in self.ws:
            if frame.type is not aiohttp.WSMsgType.TEXT:
                break
            msg = json.loads(frame.data)
            match msg["type"]:
                case "ping":
                    await self.ws.send_json({"type": "pong", "event_id": msg["ping_event"]["event_id"]})
                case "conversation_initiation_metadata":
                    self.conversation_id.set_result(msg["conversation_initiation_metadata_event"]["conversation_id"])
                case "client_tool_call":
                    self.tools.append(asyncio.create_task(self.run_tool(msg["client_tool_call"])))
            if msg["type"] not in UNRECORDED:
                self.wire.append({"t": time.time(), "dir": "in", "msg": msg})
                self._changed.set()

    async def run_tool(self, call: dict) -> None:
        body = await refund.issue_refund(self.url, self.refund_id, call["parameters"]["amount_cents"], self.marks)
        await self.send({"type": "client_tool_result", "tool_call_id": call["tool_call_id"],
                         "result": json.dumps(body), "is_error": False})

    def answered(self, msg_id: str, anchor: str) -> bool:
        """An agent_response the platform tags as answering msg_id, sent after the anchor event."""
        start = next((i for i, e in enumerate(self.wire) if e["dir"] == "out" and e["msg"]["type"] == anchor), None)
        return start is not None and any(
            e["msg"]["type"] == "agent_response" and msg_id in (e["msg"]["agent_response_event"].get("in_response_to_ids") or [])
            for e in self.wire[start:]
        )

    async def settled(self, msg_id: str, anchor: str) -> None:
        """With disable_during_tool the platform first answers the ask, with the stop already in
        context, and only seconds later sends the reply it tags as answering the stop. So this
        waits for the tagged reply and the tool, then for SETTLE_S of quiet.
        """
        deadline = time.time() + LLM_TIMEOUT_S
        while (left := deadline - time.time()) > 0:
            self._changed.clear()
            done = self.answered(msg_id, anchor) and all(t.done() for t in self.tools)
            try:
                await asyncio.wait_for(self._changed.wait(), min(SETTLE_S, left) if done else left)
            except TimeoutError:
                if done:
                    return


async def one_run(
    platform: Platform, agent_id: str, port: int, moment: Moment, mode: str, delay_ms: int, run: int, model: str
) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    marks = Marks()
    url = f"http://127.0.0.1:{port}/refunds"
    interrupted_at = None
    stopped = moment is not Moment.NONE

    signed = await platform.call("GET", "/v1/convai/conversation/get-signed-url", params={"agent_id": agent_id})
    async with aiohttp.ClientSession() as session, session.ws_connect(signed["signed_url"]) as ws:
        conv = Conversation(ws, url, refund_id, marks)
        reader = asyncio.create_task(conv.read())
        await conv.send({"type": "conversation_initiation_client_data"})
        conversation_id = await asyncio.wait_for(conv.conversation_id, LLM_TIMEOUT_S)
        await conv.say(USER_ASK, ASK_ID)
        tool_called = await reach_moment(marks, moment)
        if tool_called and stopped:
            interrupted_at = time.time()
            await conv.say(USER_STOP, STOP_ID)
        if tool_called:
            # A reply to the ask can come before the tool's result (pre-tool speech); the
            # baseline waits for the one after it.
            await conv.settled(*((STOP_ID, "user_message") if stopped else (ASK_ID, "client_tool_result")))
        await asyncio.gather(*conv.tools, return_exceptions=True)
        await ws.close()
        await reader

    transcript = await platform.transcript(conversation_id)
    ledger_rows = await refund.read_ledger(url, refund_id)
    rows = ledger_rows["rows"]
    return RunResult(
        framework="elevenlabs-agents",
        api=API_ID,
        moment=moment.value,
        interruption_mode=mode,
        commit_delay_ms=delay_ms,
        run=run,
        refund_id=refund_id,
        ledger_count=ledger_rows["count"],
        interrupted_at=interrupted_at,
        received_at=rows[0]["received_at"] if rows else None,
        committed_at=rows[0]["committed_at"] if rows else None,
        llm=model,
        tool_called=tool_called,
        **asdict(read_record(conv.wire, transcript, stopped, model)),
        conversation_id=conversation_id,
        wire=conv.wire,
        transcript=transcript,
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--interruption-mode", nargs="*", choices=INTERRUPTION_MODES, default=INTERRUPTION_MODES)
    parser.add_argument("--commit-delay-ms", nargs="*", type=int, default=[400])
    args = parser.parse_args()
    load_env(Path(".env"))
    if not os.environ.get("ELEVENLABS_API_KEY"):
        sys.exit("ELEVENLABS_API_KEY is not set. Put it in .env (gitignored) or the environment.")
    out = args.out or f"runs/elevenlabs-{args.model}.jsonl"

    results = Results(Path(out), TOLD_SOURCE)
    async with aiohttp.ClientSession(headers={"xi-api-key": os.environ["ELEVENLABS_API_KEY"]}) as http:
        platform = Platform(http)
        try:
            agents = {mode: await platform.agent_for(mode, args.model) for mode in args.interruption_mode}
            for delay_ms in args.commit_delay_ms:
                with ledger(results.db, delay_ms) as port:
                    for mode in args.interruption_mode:
                        for moment in [Moment(m) for m in args.moments]:
                            for run in range(args.runs):
                                result = await one_run(platform, agents[mode], port, moment, mode, delay_ms, run, args.model)
                                results.add(result)
                                later = f'  later: "{result.followup_reply}"' if result.followup_reply else ""
                                print(f"mode={mode:19} delay={delay_ms:<5} {moment.value:12} #{run}  "
                                      f'{result.verdict}  agent: "{result.agent_reply}"{later}', flush=True)
        finally:
            await platform.cleanup()
    print(f"wrote {results.count} runs to {out}")


if __name__ == "__main__":
    asyncio.run(main())
