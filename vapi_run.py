"""Say "stop" to a Vapi assistant while its refund tool runs.

Vapi runs a function tool by POSTing a `tool-calls` webhook to the tool's server URL
(https://docs.vapi.ai/tools/custom-tools). Here that URL is a local aiohttp server behind a
cloudflared tunnel, and its handler is refund.issue_refund.

Text path. The Chat API (POST /chat, https://docs.vapi.ai/chat/quickstart) answers 402
"Add a payment method to use chat" on this account, so the runner never used it. Each run is a
call over the WebSocket transport instead (https://docs.vapi.ai/calls/websocket-transport):
POST /call with an inline assistant and `transport.provider = "vapi.websocket"`, silence on the
audio side, and both caller turns sent as `add-message` text frames with role user and
triggerResponseEnabled (https://docs.vapi.ai/api-reference/messages/client-inbound-message,
https://docs.vapi.ai/calls/call-features). That is how an app types into a live call. Vapi
treats the stop like a barge-in: it clears the pipeline, cuts any speech, and aborts any LLM
request in progress.

The axis is the function tool's `async` flag (https://docs.vapi.ai/tools/custom-tools-troubleshooting#async-vs-sync-behavior):

- False (default): Vapi waits for the webhook, so the model answers once with the real result.
  A stop that arrives meanwhile can start an LLM request that Vapi drops, with no completion,
  once the result lands (1 of 20 sync stop runs on 2026-10-25). While it
  waits, the conversation shows the call's tool message as "Tool Result Still Pending But
  Proceed Further If Possible."
- True: Vapi writes "Success." as the tool's result as it sends the webhook, and never uses the
  webhook's response. The model answers the stop believing the refund succeeded, whatever the
  ledger did.

Other settings that touch this, not swept: the tool's `messages` (by default Vapi speaks a
filler like "This'll just take a sec" while a sync tool runs, which the stop cuts off), and
GPT-Live assistants, whose async tools add the real result to the history later
(https://docs.vapi.ai/gpt-live/tools).

`told` comes from the call's log, which Vapi publishes after the call ends
(call.artifact.presignedLogUrl). Its "LLM request started" entries list the messages Vapi sent
the model for each request. The reply to the stop is the first request whose messages hold the
stop; what it holds as the tool's result is what the model had. Weak spots: the log's messages
are Vapi's own message list, not the provider's raw payload (Vapi rebuilds the OpenAI request
from it), and a request that the log never records reads as no answer.

    python vapi_run.py --runs 5                          # needs VAPI_PRIVATE_KEY in .env
    python vapi_run.py --runs 1 --async-tool true --moments in_flight
"""

import argparse
import asyncio
import gzip
import json
import os
import sys
import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

import aiohttp
from aiohttp import web

import refund
from stopbench import (
    LLM_TIMEOUT_S,
    SYSTEM_PROMPT,
    TOOL_DESCRIPTION,
    USER_ASK,
    USER_STOP,
    Marks,
    Moment,
    Results,
    Told,
    free_port,
    ledger,
    load_env,
    reach_moment,
    tunnel,
    verdict,
)

DEFAULT_MODEL = "gpt-6-luna"
API = "https://api.vapi.ai"
VAPI_API = "api.vapi.ai POST /call, transport vapi.websocket"
# Cloudflare in front of api.vapi.ai answers 403 (error 1010) to Python's default user agent.
USER_AGENT = "stopbench"
CLIENT_MESSAGES = ["model-output", "status-update", "tool-calls", "tool.completed", "user-interrupted"]
# Vapi's first token comes 0.7 to 1.4 s after what triggers it, so quiet must outlast that.
QUIET_S = 4.0
LOG_TIMEOUT_S = 60.0
SILENCE = b"\x00" * 640  # 20 ms of 16 kHz pcm_s16le

PENDING_NOTE = "Tool Result Still Pending But Proceed Further If Possible."
ASYNC_RESULT = "Success."


@dataclass(frozen=True)
class Request:
    t: float
    span_id: str
    messages: list[dict[str, Any]]
    event: Literal["request"] = "request"


@dataclass(frozen=True)
class Completion:
    t: float
    span_id: str
    text: str
    succeeded: bool
    event: Literal["completion"] = "completion"


@dataclass(frozen=True)
class ToolResult:
    """What Vapi recorded as the tool's result: the webhook's, or "Success." for an async tool."""

    t: float
    call_id: str
    result: str
    event: Literal["tool_result"] = "tool_result"


@dataclass(frozen=True)
class LogError:
    t: float
    message: str
    event: Literal["error"] = "error"


LogEvent = Request | Completion | ToolResult | LogError
LOG_TYPES: dict[str, type] = {cls.event: cls for cls in (Request, Completion, ToolResult, LogError)}
ERROR_LEVEL = 50


def parse_log_line(raw: dict[str, Any]) -> list[LogEvent]:
    t = raw["time"] / 1000
    attrs = raw.get("attributes") or {}
    match raw["body"], attrs.get("event"):
        case "LLM request started", "assistant.model.requestStarted":
            return [Request(t, attrs["spanId"], attrs["messages"])]
        case _, "assistant.model.responseSucceeded":
            return [Completion(t, attrs["spanId"], attrs.get("completionText") or "", True)]
        case _, "assistant.model.requestAborted":
            return [Completion(t, attrs["spanId"], attrs.get("completionText") or "", False)]
        case _, "assistant.tool.completed":
            return [ToolResult(t, r["toolCallId"], r["result"]) for r in attrs.get("results") or []]
    if raw.get("level", 0) >= ERROR_LEVEL:
        return [LogError(t, raw["body"])]
    return []


def load_log(rows: list[dict[str, Any]]) -> list[LogEvent]:
    return [LOG_TYPES[row["event"]](**row) for row in rows]


def tool_results(request: Request) -> list[str]:
    return [json.loads(m["content"])["result"] for m in request.messages if m["role"] == "tool_call_result"]


def has_stop(request: Request) -> bool:
    return any(m["role"] == "user" and m.get("content") == USER_STOP for m in request.messages)


@dataclass(frozen=True)
class Reading:
    reply_context: list[str]
    agent_reply: str | None
    followup_reply: str | None
    reply_status: Literal["succeeded", "aborted"] | None
    errors: list[str]


def read_log(log: Sequence[LogEvent], stopped: bool) -> Reading:
    """Vapi logs each LLM request with the messages it sent. The reply to the stop is the first
    request that holds the stop; with no stop, the first that holds a tool result.
    """
    errors = [e.message for e in log if isinstance(e, LogError)]
    requests = [e for e in log if isinstance(e, Request)]
    anchor = has_stop if stopped else (lambda r: bool(tool_results(r)))

    def completion(span_id: str) -> Completion | None:
        done = [e for e in log if isinstance(e, Completion) and e.span_id == span_id]
        return next((c for c in done if c.succeeded), done[0] if done else None)

    # Vapi can start a request after the stop and drop it, with no completion, once the
    # tool's result lands; the reply is the first request that produced one.
    at = next((i for i, r in enumerate(requests) if anchor(r) and completion(r.span_id)), None)
    if at is None:
        return Reading([], None, None, None, errors)

    reply = completion(requests[at].span_id)
    later = [c.text for r in requests[at + 1:] if (c := completion(r.span_id)) and c.text]
    return Reading(
        reply_context=tool_results(requests[at]),
        agent_reply=reply.text if reply else None,
        followup_reply=" ".join(later) or None,
        reply_status=None if reply is None else "succeeded" if reply.succeeded else "aborted",
        errors=errors,
    )


TOLD_SOURCE = "vapi_call_log_llm_requests"


def told_from(record: dict) -> Told:
    if record["moment"] != Moment.NONE.value and record["reply_status"] is None:
        return Told.NO_ANSWER
    if not record["reply_context"]:
        return Told.NOTHING
    had = record["reply_context"][-1]
    if had in record["tool_outputs"]:
        return Told.RESULT
    if had == PENDING_NOTE:
        return Told.PLACEHOLDER
    if had == ASYNC_RESULT:
        return Told.ASSUMED
    raise ValueError(f"unrecognised tool result in the model's context: {had!r}")


def rederive(record: dict) -> dict:
    """The derived fields, rebuilt from the evidence the record stores."""
    return asdict(read_log(load_log(record["log"]), record["moment"] != Moment.NONE.value))


@dataclass
class RunResult:
    framework: str
    vapi_api: str
    moment: str
    async_tool: bool
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
    call_id: str
    cost_usd: float | None
    webhooks: list[dict[str, Any]]
    log: list[LogEvent]

    @property
    def landed(self) -> bool:
        return self.ledger_count > 0

    @property
    def told(self) -> Told:
        return told_from(self.__dict__)

    @property
    def verdict(self) -> str:
        return verdict(self.tool_called, self.ledger_count, Moment(self.moment), self.told)


@dataclass
class Armed:
    ledger_url: str
    refund_id: str
    marks: Marks
    webhooks: list[dict[str, Any]] = field(default_factory=list)
    replied: asyncio.Event = field(default_factory=asyncio.Event)


class ToolServer:
    """Receives Vapi's tool-calls webhook for whichever run is armed."""

    def __init__(self) -> None:
        self.armed: Armed | None = None
        self.app = web.Application()
        self.app.router.add_post("/tool", self.handle)

    async def handle(self, request: web.Request) -> web.Response:
        received_at = time.time()
        message = (await request.json())["message"]
        armed = self.armed
        if message["type"] != "tool-calls" or armed is None:
            return web.json_response({"results": []})
        results = []
        for call in message["toolCallList"]:
            args = call["function"]["arguments"]
            args = json.loads(args) if isinstance(args, str) else args
            body = await refund.issue_refund(armed.ledger_url, armed.refund_id, int(args["amount_cents"]), armed.marks)
            results.append({"toolCallId": call["id"], "result": json.dumps(body)})
        armed.webhooks.append({"received_at": received_at, "replied_at": time.time(), "results": results})
        armed.replied.set()
        return web.json_response({"results": results})


def assistant(model: str, tool_url: str, async_tool: bool) -> dict[str, Any]:
    return {
        "name": "stopbench-refund",
        "firstMessageMode": "assistant-waits-for-user",
        "maxDurationSeconds": 120,
        "clientMessages": CLIENT_MESSAGES,
        "model": {
            "provider": "openai",
            "model": model,
            "messages": [{"role": "system", "content": SYSTEM_PROMPT}],
            "tools": [{
                "type": "function",
                "async": async_tool,
                "function": {
                    "name": "issue_refund",
                    "description": TOOL_DESCRIPTION,
                    "parameters": {
                        "type": "object",
                        "properties": {"amount_cents": {"type": "integer", "description": "Amount in cents."}},
                        "required": ["amount_cents"],
                    },
                },
                "server": {"url": tool_url},
            }],
        },
    }


class CallSocket:
    """The call's WebSocket transport: silence out, control events in, text turns out."""

    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self.ws = ws
        self.in_progress = asyncio.Event()
        self.turns: dict[str, float] = {}
        self.last_activity = time.time()

    async def say(self, text: str) -> None:
        await self.ws.send_str(json.dumps({
            "type": "add-message",
            "message": {"role": "user", "content": text},
            "triggerResponseEnabled": True,
        }))
        self.last_activity = time.time()

    async def stream_silence(self) -> None:
        while not self.ws.closed:
            await self.ws.send_bytes(SILENCE)
            await asyncio.sleep(0.02)

    async def read(self) -> None:
        async for msg in self.ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            event = json.loads(msg.data)
            match event.get("type"):
                case "status-update" if event.get("status") == "in-progress":
                    self.in_progress.set()
                case "model-output":
                    self.turns.setdefault(event.get("turnId", ""), time.time())
                    self.last_activity = time.time()
                case "tool-calls" | "tool.completed" | "user-interrupted":
                    self.last_activity = time.time()

    async def settled(self, armed: Armed, stopped_at: float | None) -> None:
        """Done once the webhook has replied, a turn has started after the stop, and the model
        has been quiet for QUIET_S.
        """
        deadline = time.time() + LLM_TIMEOUT_S
        while time.time() < deadline:
            answered = stopped_at is None or any(t > stopped_at for t in self.turns.values())
            if armed.replied.is_set() and answered and time.time() - self.last_activity > QUIET_S:
                return
            await asyncio.sleep(0.1)


async def fetch_log(http: aiohttp.ClientSession, call_id: str) -> tuple[list[LogEvent], float | None]:
    deadline = time.time() + LOG_TIMEOUT_S
    while time.time() < deadline:
        async with http.get(f"{API}/call/{call_id}") as resp:
            call = await resp.json()
        url = (call.get("artifact") or {}).get("presignedLogUrl")
        if call.get("status") == "ended" and url:
            async with aiohttp.ClientSession() as plain, plain.get(url) as resp:
                if resp.status == 200:
                    lines = gzip.decompress(await resp.read()).decode().splitlines()
                    raws = [json.loads(line) for line in lines]
                    if any(r["body"] == "call.ended" for r in raws):
                        events = [e for r in sorted(raws, key=lambda r: r["time"]) for e in parse_log_line(r)]
                        return events, call.get("cost")
        await asyncio.sleep(2)
    raise TimeoutError(f"call {call_id} log never appeared")


async def one_run(
    http: aiohttp.ClientSession, server: ToolServer, tool_url: str, ledger_port: int,
    moment: Moment, async_tool: bool, delay_ms: int, run: int, model: str,
) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    ledger_url = f"http://127.0.0.1:{ledger_port}/refunds"
    armed = server.armed = Armed(ledger_url, refund_id, Marks())
    stopped = moment is not Moment.NONE
    interrupted_at = None

    body = {
        "transport": {
            "provider": "vapi.websocket",
            "audioFormat": {"format": "pcm_s16le", "container": "raw", "sampleRate": 16000},
        },
        "assistant": assistant(model, tool_url, async_tool),
    }
    async with http.post(f"{API}/call", json=body) as resp:
        call = await resp.json()
        if resp.status >= 300:
            raise RuntimeError(f"POST /call failed: {resp.status} {call}")
    async with aiohttp.ClientSession() as plain, plain.ws_connect(call["transport"]["websocketCallUrl"]) as ws:
        sock = CallSocket(ws)
        tasks = [asyncio.create_task(sock.read()), asyncio.create_task(sock.stream_silence())]
        try:
            await asyncio.wait_for(sock.in_progress.wait(), LLM_TIMEOUT_S)
            await sock.say(USER_ASK)
            tool_called = await reach_moment(armed.marks, moment)
            if tool_called and stopped:
                interrupted_at = time.time()
                await sock.say(USER_STOP)
            if tool_called:
                await sock.settled(armed, interrupted_at)
            await ws.send_str(json.dumps({"type": "end-call"}))
            await asyncio.wait([tasks[0]], timeout=10)
        finally:
            for task in tasks:
                task.cancel()
    server.armed = None

    log, cost = await fetch_log(http, call["id"])
    rows = (await refund.read_ledger(ledger_url, refund_id))["rows"]
    return RunResult(
        framework="vapi",
        vapi_api=VAPI_API,
        moment=moment.value,
        async_tool=async_tool,
        commit_delay_ms=delay_ms,
        run=run,
        refund_id=refund_id,
        ledger_count=len(rows),
        interrupted_at=interrupted_at,
        received_at=rows[0]["received_at"] if rows else None,
        committed_at=rows[0]["committed_at"] if rows else None,
        llm=model,
        tool_called=tool_called,
        tool_outputs=[r["result"] for w in armed.webhooks for r in w["results"]],
        **asdict(read_log(log, stopped)),
        call_id=call["id"],
        cost_usd=cost,
        webhooks=armed.webhooks,
        log=log,
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--async-tool", nargs="*", choices=["false", "true"], default=["false", "true"])
    parser.add_argument("--commit-delay-ms", nargs="*", type=int, default=[400])
    args = parser.parse_args()
    load_env(Path(".env"))
    if not os.environ.get("VAPI_PRIVATE_KEY"):
        sys.exit("VAPI_PRIVATE_KEY is not set. Put it in .env (gitignored) or the environment.")
    out = args.out or f"runs/vapi-{args.model}.jsonl"

    results = Results(Path(out), TOLD_SOURCE)
    server = ToolServer()
    runner = web.AppRunner(server.app)
    await runner.setup()
    port = free_port()
    await web.TCPSite(runner, "127.0.0.1", port).start()
    headers = {"Authorization": f"Bearer {os.environ['VAPI_PRIVATE_KEY']}", "User-Agent": USER_AGENT}
    total_cost = 0.0
    try:
        with tunnel(port) as public_url:
            async with aiohttp.ClientSession(headers=headers) as http:
                for delay_ms in args.commit_delay_ms:
                    with ledger(results.db, delay_ms) as ledger_port:
                        for async_tool in [a == "true" for a in args.async_tool]:
                            for moment in [Moment(m) for m in args.moments]:
                                for run in range(args.runs):
                                    result = await one_run(
                                        http, server, f"{public_url}/tool", ledger_port,
                                        moment, async_tool, delay_ms, run, args.model,
                                    )
                                    results.add(result)
                                    total_cost += result.cost_usd or 0
                                    later = f'  later: "{result.followup_reply}"' if result.followup_reply else ""
                                    print(f"async={async_tool!s:5} delay={delay_ms:<5} {moment.value:12} #{run}  "
                                          f'{result.verdict}  agent: "{result.agent_reply}"{later}', flush=True)
    finally:
        await runner.cleanup()
    print(f"wrote {results.count} runs to {out} (Vapi cost ${total_cost:.4f})")


if __name__ == "__main__":
    asyncio.run(main())
