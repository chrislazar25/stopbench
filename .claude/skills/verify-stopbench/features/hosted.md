# Hosted runners

Four runners drive hosted voice platforms in text sessions and share `.venv-hosted`. Each one creates what it needs on the platform, says stop mid-tool the way that platform's app would, reads what the model had from the platform's own record, and deletes what it created. Gemini and ElevenLabs run the tool in-process as a client tool. Retell and Vapi call it by webhook through a cloudflared quick tunnel (`stopbench.tunnel`).

## Sub-features

- `gemini` is `gemini_run.py` on gemini-3.8-live. The stop is `send_client_content` with `turn_complete`. The axis is the function's `behavior`: `NON_BLOCKING` (default) answers blind, and `BLOCKING` drops the call from the model's context. The reply is read from the audio transcription, because no Live model accepts TEXT output.
- `elevenlabs` is `elevenlabs_run.py` on gpt-6.1-sol. The stop is a `user_message` on the conversation websocket. The axis is the client tool's `interruption_mode`: `allow` (default) writes "Tool execution was abandoned" as the result, and `disable_during_tool` holds the stop until the tool returns.
- `retell` is `retell_run.py` on gpt-5.6-terra. The stop is a second `create-chat-completion` while the first waits on the webhook. The axis is `speak_after_execution`: `true` (default) adds a later reply with the result, and `false` leaves the blind reply as the last word.
- `vapi` is `vapi_run.py` on gpt-6-luna. Each run is a call on the `vapi.websocket` transport. The ask and the stop are `add-message` text frames. The axis is the function tool's `async`: `false` (default) waits for the webhook, and `true` writes "Success." at once (`ASSUMED`).

## How to get to it (user POV)

- `.venv-hosted/bin/python <gemini|elevenlabs|retell|vapi>_run.py [--runs N] [--moments ...] [<axis flag> ...] [--commit-delay-ms ...]`

## Driving it with verify.py

Preconditions:

- `verify.py doctor` prints `doctor: OK`, which includes `.venv-hosted` matching `requirements-hosted.lock.txt` and each platform's key in `.env`.
- `cloudflared` is on the PATH for `retell` and `vapi`.
- Every run here is paid.

- **Whole matrix, one run per cell.** Run `python3 .claude/skills/verify-stopbench/verify.py run <gemini|elevenlabs|retell|vapi>`. It covers both axis values and all four moments.
- **Proof.** `check.txt` ends `check: PASS`. That includes the ledger rows, the moment windows, and rebuilding every derived field from the stored evidence through the runner's `rederive`.

## Gotchas

- Gemini `BLOCKING` sometimes calls `issue_refund` a second time after the stop. Two ledger rows read `DOUBLE`. That is a finding, not a flake.
- ElevenLabs, Retell and Vapi keep each conversation, chat or call in the account history. The runners delete the agents, LLMs and tools they made, but leave those records, because they hold the platform's own logs.
- Retell's `speak_after_execution=false` baseline reads `NO_REPLY`, because the agent never speaks after the result.
- Vapi's Chat API answers 402 without a card on file, which is why the runner uses a websocket call. Cloudflare in front of `api.vapi.ai` answers 403 to Python's default user agent.
- A tunnel that never comes up shows as `NO_CALL` on every webhook run. Check that `cloudflared` runs on its own.
