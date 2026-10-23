# Pipecat runner

`pipecat_run.py` runs a Pipecat 1.13.0 pipeline whose LLM calls `issue_refund`, says stop at a chosen moment, and records whether the refund landed and what the model's context held for the tool call. The headline: with the default `cancel_on_interruption=True`, the model is told `CANCELLED` even when the refund landed.

## Sub-features

- `pipecat-default` is `--cancel true`, the default setting.
- `pipecat-no-cancel` is `--cancel false`.
- `pipecat-real` adds `--llm openai` for gpt-6.1-sol through the Responses API.

## How to get to it (user POV)

- `.venv/bin/python pipecat_run.py [--runs N] [--moments ...] [--cancel true false] [--llm openai]`

## Driving it with verify.py

Preconditions:

- `verify.py doctor` prints `doctor: OK`, with no `pipecat_run.py` running.

- **Default setting, three stops.** Run `python3 .claude/skills/verify-stopbench/verify.py run pipecat --moments before_send in_flight after_commit --cancel true`. The console shows `ok: stopped, model told cancelled` for before_send and `LIE: refund landed, model told cancelled` for the other two.
- **No-cancel setting.** Run `python3 .claude/skills/verify-stopbench/verify.py run pipecat --moments before_send in_flight --cancel false`. before_send shows `IGNORED`, and the refund lands in both.
- **Real model, paid.** Run `python3 .claude/skills/verify-stopbench/verify.py run pipecat --llm openai --moments in_flight --cancel true`. The console's agent line says the refund was canceled, and `check.txt` shows `landed` 1.
- **Proof.** Each run's `check.txt` ends `check: PASS`.

## Gotchas

- With `--cancel false`, the verdict reads `ok: landed, model told landed` even when `tool_result_in_context` holds Pipecat's async "still running" placeholder. Read the field, not just the verdict.
- `agent_reply` joins every assistant message after the stop, so a stop reply and a later reply arrive as one string.
- Values for `--cancel` other than the lowercase string `true` count as false.
