# Realtime runner

`realtime_run.py` runs an OpenAI Agents SDK 0.24.0 `RealtimeSession` on gpt-realtime-2.1 with text output, says stop with `session.interrupt()` and `session.send_message()`, and records the server's own event stream. From that stream it reads what the model had when it answered the stop, the reply, and any later reply the model made unprompted. The SDK never cancels the tool. `async_tool_calls` decides whether the model answers fast without the result (`true`, the default) or after the tool with the result (`false`).

## Sub-features

- `realtime-async` is `--async-tools true`: a blind reply, then an unprompted second reply.
- `realtime-sync` is `--async-tools false`: one reply that sees the result.
- `realtime-slow` adds `--commit-delay-ms 6000` to show how long the caller waits.

## How to get to it (user POV)

- `.venv-realtime/bin/python realtime_run.py [--runs N] [--moments ...] [--async-tools true false] [--commit-delay-ms ...]`

## Driving it with verify.py

Preconditions:

- `verify.py doctor` prints `doctor: OK`.
- `.env` holds `OPENAI_API_KEY`. Every run here is paid.

- **Blind reply, then a second one.** Run `python3 .claude/skills/verify-stopbench/verify.py run realtime --moments in_flight --async-tools true`. The console shows `BLIND: refund landed, model told nothing`, an `agent:` line and a `later:` line.
- **One reply with the result.** Run `python3 .claude/skills/verify-stopbench/verify.py run realtime --moments in_flight --async-tools false`. The console shows `ok: landed, model told landed` and no `later:` line.
- **Proof.** Each run's `check.txt` ends `check: PASS`, which includes rebuilding every derived field from the stored `wire`.

## Gotchas

- `session.interrupt()` sends nothing on the wire in a text session. Every response should still end `completed`. A `cancelled` status in `wire` means the SDK or server behavior changed.
- With `--async-tools false`, the tool blocks the SDK's reader, so receive times of events queued behind it run late. Only the order of events decides what the model had.
- `NO_REPLY` means the model never answered the stop. The SDK drops a rejected `response.create` without retrying, so look for `errors` in the record.
- The model sometimes says a short "processing" line before the tool call. That text is left out of the `none` baseline's `agent_reply`.
