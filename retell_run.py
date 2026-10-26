"""Say "stop" to a Retell AI chat agent while its refund function runs.

Retell runs a custom function by POSTing to your URL from its own servers and waiting
for the response (https://docs.retellai.com/build/single-multi-prompt/custom-function).
So the tool body runs in a local aiohttp server behind a cloudflared tunnel.

The agent is a single prompt Retell LLM behind a chat agent, driven with the chat API:
create-chat, then one create-chat-completion per user message
(https://docs.retellai.com/deploy/create-chat-completion,
https://docs.retellai.com/api-references/create-chat-completion). A chat has no interrupt
call, so the stop is what a chat app sends when the user types again: a second
create-chat-completion while the first is still waiting on the function. Retell accepts it
and answers it at once, with the function still running. In a voice call the same thing is
barge-in, which the custom function FAQ says turns the agent to answer the user while the
request to your endpoint "runs to completion".

Retell never cancels the request, so no setting decides whether the refund lands, and none
we found gives the model the result before it answers the stop. The setting that decides
what the caller ends up told is speak_after_execution ("Talk After Action Completed", on by
default; CustomTool in https://docs.retellai.com/api-references/create-retell-llm):

- True (default): when the result lands, Retell calls the LLM again and the agent speaks
  unprompted, after it has already answered the stop.
- False: the result is written to the transcript and nothing more is said, so the stop's
  reply is the last word.

speak_during_execution (off by default) only adds a filler line while the call runs.
Conversation flow agents have a function node setting, wait_for_result, that holds the flow's
next transition until the result is in
(https://docs.retellai.com/build/conversation-flow/function-node). This runner uses the
single prompt agent instead: there the model itself calls issue_refund under SYSTEM_PROMPT,
and it is the agent type Retell recommends starting with
(https://docs.retellai.com/build/choose-agent-type).

What the model had is read from Retell's own record, get-chat's message_with_tool_calls,
which interleaves user and agent messages with tool_call_invocation and tool_call_result
in the order Retell wrote them. The reply to the stop is the agent message the stop's own
completion returned. The result counts as in its context only if it comes before the stop's
user message: Retell starts the stop's turn when it writes that message, and a result that
lands in the ~0.5 s between it and the reply is counted as not seen. That is the weak spot:
Retell does not expose the prompt it sent, so this is order, not the context itself. In the
after_commit runs the result is written 144 to 178 ms after the stop's message and before
the reply, and the reply still says the refund won't happen. The webhook times are from
this machine's clock and only annotate.

    python retell_run.py --runs 5                       # needs RETELL_API_KEY in .env
    python retell_run.py --runs 5 --speak-after-execution false
"""

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web

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
    free_port,
    ledger,
    load_env,
    reach_moment,
    tunnel,
    verdict,
)

API = "https://api.retellai.com"
RETELL_API = "api.retellai.com, OpenAPI spec revision 2026-10-23-8c18b4d"
DEFAULT_MODEL = "gpt-5.6-terra"
TOOL_NAME = "issue_refund"

Message = dict[str, Any]


@dataclass(frozen=True)
class Webhook:
    t_in: float
    t_out: float | None
    tool_call_id: str | None
    args: dict[str, Any]


@dataclass
class Run:
    refund_id: str
    ledger_url: str
    marks: Marks = field(default_factory=Marks)
    webhooks: list[Webhook] = field(default_factory=list)
    open_calls: int = 0
    idle: asyncio.Event = field(default_factory=asyncio.Event)


class ToolServer:
    """Routes each webhook by chat id. The X-Retell-Signature is HMAC'd with the
    workspace's designated webhook key, which need not be RETELL_API_KEY, so it is not
    checked; an unknown chat id gets a 404 and never reaches the ledger.
    """

    def __init__(self) -> None:
        self.runs: dict[str, Run] = {}

    async def handle(self, request: web.Request) -> web.Response:
        t_in = time.time()
        body = await request.json()
        chat = body.get("chat") or {}
        run = self.runs.get(chat.get("chat_id"))
        if run is None or body.get("name") != TOOL_NAME:
            return web.json_response({"error": "unknown chat or tool"}, status=404)
        calls = [m for m in chat.get("message_with_tool_calls") or [] if m["role"] == "tool_call_invocation"]
        tool_call_id = calls[-1]["tool_call_id"] if calls else None
        run.open_calls += 1
        run.idle.clear()
        try:
            result = await refund.issue_refund(run.ledger_url, run.refund_id, body["args"]["amount_cents"], run.marks)
        finally:
            run.webhooks.append(Webhook(t_in, time.time(), tool_call_id, body.get("args") or {}))
            run.open_calls -= 1
            if not run.open_calls:
                run.idle.set()
        return web.json_response(result)


@dataclass(frozen=True)
class Reading:
    tool_outputs: list[str]
    reply_context: list[str]
    agent_reply: str | None
    followup_reply: str | None


def read_transcript(transcript: list[Message], own: list[Message], stopped: bool) -> Reading:
    """own: the messages returned by the completion that answers (the stop's, or the ask's
    when there is no stop). Positions in the transcript decide; timestamps only annotate.
    """
    calls = {m["tool_call_id"] for m in transcript if m["role"] == "tool_call_invocation" and m["name"] == TOOL_NAME}
    outputs = [(i, f"{m['tool_call_id']}: {m['content']}") for i, m in enumerate(transcript)
               if m["role"] == "tool_call_result" and m["tool_call_id"] in calls]
    tool_outputs = [o for _, o in outputs]
    position = {m["message_id"]: i for i, m in enumerate(transcript)}
    replies = sorted(position[m["message_id"]] for m in own if m["role"] == "agent" and m["message_id"] in position)

    if stopped:
        cutoff = next((i for i, m in enumerate(transcript) if m["role"] == "user" and m["content"] == USER_STOP), None)
        reply_at = replies[0] if cutoff is not None and replies else None
    else:
        first_output = outputs[0][0] if outputs else None
        reply_at = next((i for i in replies if first_output is not None and i > first_output), None)
        cutoff = reply_at
    if reply_at is None:
        return Reading(tool_outputs, [], None, None)

    later = [m["content"] for m in transcript[reply_at + 1:] if m["role"] == "agent"]
    return Reading(
        tool_outputs=tool_outputs,
        reply_context=[o for i, o in outputs if i < cutoff],
        agent_reply=transcript[reply_at]["content"],
        followup_reply=" ".join(later) or None,
    )


TOLD_SOURCE = "retell_message_with_tool_calls_order"


def told_from(record: dict) -> Told:
    reading = rederive(record)
    if reading["agent_reply"] is None:
        return Told.NO_ANSWER
    return Told.RESULT if reading["reply_context"] else Told.NOTHING


def rederive(record: dict) -> dict:
    """The derived fields, rebuilt from the evidence the record stores."""
    stopped = record["interrupted_at"] is not None
    own = record["stop_messages"] if stopped else record["ask_messages"]
    return asdict(read_transcript(record["transcript"], own, stopped))


@dataclass
class RunResult:
    framework: str
    retell_api: str
    moment: str
    speak_after_execution: bool
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
    chat_id: str
    ask_messages: list[Message]
    stop_messages: list[Message]
    transcript: list[Message]
    webhooks: list[Webhook]

    @property
    def landed(self) -> bool:
        return self.ledger_count > 0

    @property
    def told(self) -> Told:
        return told_from(self.__dict__)

    @property
    def verdict(self) -> str:
        return verdict(self.tool_called, self.ledger_count, Moment(self.moment), self.told)


class RetellError(Exception):
    pass


async def call(session: aiohttp.ClientSession, method: str, path: str, body: dict | None = None) -> Any:
    async with session.request(method, API + path, json=body) as resp:
        text = await resp.text()
        if resp.status >= 300:
            raise RetellError(f"{method} {path}: {resp.status} {text[:500]}")
        return json.loads(text) if text else None


async def complete(session: aiohttp.ClientSession, chat_id: str, content: str, errors: list[str]) -> list[Message]:
    try:
        reply = await asyncio.wait_for(
            call(session, "POST", "/create-chat-completion", {"chat_id": chat_id, "content": content}), LLM_TIMEOUT_S
        )
        return reply["messages"]
    except (RetellError, TimeoutError, aiohttp.ClientError) as e:
        errors.append(f"completion {content!r}: {type(e).__name__} {e}")
        return []


async def create_agent(session: aiohttp.ClientSession, tool_url: str, model: str, speak_after: bool,
                       created: list[tuple[str, str]]) -> str:
    llm = await call(session, "POST", "/create-retell-llm", {
        "model_config": {"type": "cascading", "primary": {"model": model}},
        "general_prompt": SYSTEM_PROMPT,
        "start_speaker": "user",
        "general_tools": [{
            "type": "custom",
            "name": TOOL_NAME,
            "description": TOOL_DESCRIPTION,
            "url": tool_url,
            "method": "POST",
            "speak_after_execution": speak_after,
            "parameters": {
                "type": "object",
                "properties": {"amount_cents": {"type": "integer", "description": "Amount in cents."}},
                "required": ["amount_cents"],
            },
        }],
    })
    created.append(("/delete-retell-llm/", llm["llm_id"]))
    agent = await call(session, "POST", "/create-chat-agent", {
        "agent_name": f"stopbench-speak-after-{str(speak_after).lower()}",
        "response_engine": {"type": "retell-llm", "llm_id": llm["llm_id"]},
    })
    created.append(("/delete-chat-agent/", agent["agent_id"]))
    return agent["agent_id"]


async def one_run(session: aiohttp.ClientSession, server: ToolServer, agent_id: str, port: int, moment: Moment,
                  speak_after: bool, delay_ms: int, run: int, model: str) -> RunResult:
    refund_id = uuid.uuid4().hex[:12]
    url = f"http://127.0.0.1:{port}/refunds"
    state = Run(refund_id, url)
    state.idle.set()
    errors: list[str] = []
    interrupted_at = None
    stop_messages: list[Message] = []

    chat_id = (await call(session, "POST", "/create-chat", {"agent_id": agent_id}))["chat_id"]
    server.runs[chat_id] = state
    try:
        ask = asyncio.create_task(complete(session, chat_id, USER_ASK, errors))
        tool_called = await reach_moment(state.marks, moment)
        if tool_called and moment is not Moment.NONE:
            interrupted_at = time.time()
            stop_messages = await complete(session, chat_id, USER_STOP, errors)
        ask_messages = await ask
        await asyncio.wait_for(state.idle.wait(), LLM_TIMEOUT_S)
        await asyncio.sleep(SETTLE_S)
        # Read before ending: right after end-chat, get-chat returned an empty message_with_tool_calls.
        chat = await call(session, "GET", f"/get-chat/{chat_id}")
        await call(session, "PATCH", f"/end-chat/{chat_id}")
    finally:
        del server.runs[chat_id]

    transcript = chat.get("message_with_tool_calls") or []
    if not transcript:
        errors.append("get-chat returned no message_with_tool_calls")
    own = stop_messages if interrupted_at is not None else ask_messages
    ledger_rows = await refund.read_ledger(url, refund_id)
    rows = ledger_rows["rows"]
    return RunResult(
        framework="retell",
        retell_api=RETELL_API,
        moment=moment.value,
        speak_after_execution=speak_after,
        commit_delay_ms=delay_ms,
        run=run,
        refund_id=refund_id,
        ledger_count=ledger_rows["count"],
        interrupted_at=interrupted_at,
        received_at=rows[0]["received_at"] if rows else None,
        committed_at=rows[0]["committed_at"] if rows else None,
        llm=model,
        tool_called=tool_called,
        **asdict(read_transcript(transcript, own, interrupted_at is not None)),
        errors=errors,
        chat_id=chat_id,
        ask_messages=ask_messages,
        stop_messages=stop_messages,
        transcript=transcript,
        webhooks=list(state.webhooks),
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--out")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--moments", nargs="*", default=[m.value for m in Moment])
    parser.add_argument("--speak-after-execution", nargs="*", choices=["true", "false"], default=["true", "false"])
    parser.add_argument("--commit-delay-ms", nargs="*", type=int, default=[400])
    args = parser.parse_args()
    load_env(Path(".env"))
    key = os.environ.get("RETELL_API_KEY")
    if not key:
        sys.exit("RETELL_API_KEY is not set. Put it in .env (gitignored) or the environment.")
    out = args.out or f"runs/retell-{args.model}.jsonl"
    results = Results(Path(out), TOLD_SOURCE)

    server = ToolServer()
    app = web.Application()
    app.router.add_post("/tool", server.handle)
    runner = web.AppRunner(app)
    await runner.setup()
    tool_port = free_port()
    await web.TCPSite(runner, "127.0.0.1", tool_port).start()
    created: list[tuple[str, str]] = []
    try:
        with tunnel(tool_port) as public_url:
            async with aiohttp.ClientSession(headers={"Authorization": f"Bearer {key}"}) as session:
                try:
                    agents = {}
                    for speak_after in [a == "true" for a in args.speak_after_execution]:
                        agents[speak_after] = await create_agent(
                            session, f"{public_url}/tool", args.model, speak_after, created
                        )
                    for delay_ms in args.commit_delay_ms:
                        with ledger(results.db, delay_ms) as port:
                            for speak_after, agent_id in agents.items():
                                for moment in [Moment(m) for m in args.moments]:
                                    for run in range(args.runs):
                                        result = await one_run(session, server, agent_id, port, moment,
                                                               speak_after, delay_ms, run, args.model)
                                        results.add(result)
                                        later = f'  later: "{result.followup_reply}"' if result.followup_reply else ""
                                        print(f"speak_after={speak_after!s:5} delay={delay_ms:<5} {moment.value:12} "
                                              f'#{run}  {result.verdict}  agent: "{result.agent_reply}"{later}',
                                              flush=True)
                finally:
                    for path, resource_id in reversed(created):
                        try:
                            await call(session, "DELETE", path + resource_id)
                        except RetellError as e:
                            print(f"could not delete {resource_id}: {e}", file=sys.stderr)
    finally:
        await runner.cleanup()
    print(f"wrote {results.count} runs to {out}")


if __name__ == "__main__":
    asyncio.run(main())
