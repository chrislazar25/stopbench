# LiveKit runner

`livekit_run.py` runs a LiveKit Agents 1.8.8 session with `vad=None` and manual turn detection, says stop the way LiveKit's text input does, and records the refund, every tool output in the session history, and the tool output the model saw when it answered the stop. A stop never cancels the tool. LiveKit waits up to 5 s for it, so the runner varies how long the commit takes.

## Sub-features

- `livekit-400` is a 400 ms commit, inside LiveKit's 5 s wait.
- `livekit-6000` is a 6000 ms commit, which outlives the wait and produces the "still in progress" placeholder.
- `livekit-real` adds `--llm openai --no-chain` for gpt-6.1-sol.
- `livekit-chained` is `--llm openai` without `--no-chain`, which shows the duplicate function_call bug (livekit/agents#5136).

## How to get to it (user POV)

- `.venv-livekit/bin/python livekit_run.py [--runs N] [--moments ...] [--commit-delay-ms 400 6000] [--llm openai] [--no-chain]`

## Driving it with verify.py

Preconditions:

- `verify.py doctor` prints `doctor: OK`.

- **Stop before the request, 400 ms.** Run `python3 .claude/skills/verify-stopbench/verify.py run livekit --moments before_send --commit-delay-ms 400`. The console shows `IGNORED`. In `check.txt` the window passes, because the request reached the ledger about 200 ms after the stop.
- **Placeholder, 6000 ms.** Run `python3 .claude/skills/verify-stopbench/verify.py run livekit --moments in_flight --commit-delay-ms 6000`. The console shows `PENDING: refund landed, model told still in progress`. It takes about 8 s and logs `speech not done in time after interruption`, which is LiveKit's own 5 s timeout firing.
- **Real model, paid.** Run `python3 .claude/skills/verify-stopbench/verify.py run livekit --llm openai --no-chain --moments in_flight --commit-delay-ms 400`. The agent line says the refund was already submitted.
- **Proof.** Each run's `check.txt` ends `check: PASS`.

## Gotchas

- The runner depends on LiveKit internals: `_RUNNING_TOOL_PLACEHOLDER`, `_claim_user_turn`, the `_final` call-id suffix and the 5 s `INTERRUPTION_TIMEOUT`. `assert_internals()` stops the runner at startup if the last three moved. The placeholder text is imported, so only a change in how LiveKit uses it would slip past. After a LiveKit upgrade, rerun the 6000 ms in-flight cell first. A silent change shows as `PENDING` turning into another verdict.
- `reply_context` comes from the first `llm_node` call whose context holds the stop message, or with no stop, the first that holds a tool output. A stopped run where no call saw the stop raises instead of reading as "model told nothing".
- Without `--no-chain`, replies can claim a second refund that the ledger never got. That is the upstream bug, not a runner fault.
