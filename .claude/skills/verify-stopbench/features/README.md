# stopbench verification map

This directory is the maintained source for verifying stopbench's user-facing behavior. Read this index, then use the matching feature file as the recipe.

## Baseline preconditions

- The current directory is the stopbench repo root.
- `python3 .claude/skills/verify-stopbench/verify.py doctor` prints `doctor: OK`.
- For paid runs, `.env` holds `OPENAI_API_KEY`. `doctor` lists key names only.

## Driving conventions

- Drive runners only through `verify.py run`, which forces `--runs 1` and writes to `.verify/`, never to `runs/`.
- Pick cells with the runner's own flags, exactly as the feature file lists them.
- Run the scripted cells before any paid cell.

## Proof and skip reporting

- A proof is the run's `.verify/` folder: console output, jsonl, the ledger db beside it and `check.txt` with `check: PASS`.
- Quote the agent's words from the console file when a claim is about what the agent said.
- If a cell can't run (missing key, venv drift), report the command and the unmet precondition. Don't report it as verified through another cell.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behavior, then four H2 sections in this order: `Sub-features`, `How to get to it (user POV)`, `Driving it with verify.py` (starting with `Preconditions:`), and `Gotchas`.

## Features

Driven end to end through `verify.py` on 2026-10-22: ledger and pipecat (scripted). The livekit and realtime recipes are written from the runners' code and their committed runs, and have not yet been driven through `verify.py`.


- [Ledger](./ledger.md) covers the fake payments API: commit after hang-up and the refund lookup.
- [Pipecat runner](./pipecat.md) covers the four moments under both `cancel_on_interruption` settings, scripted and with a real model.
- [LiveKit runner](./livekit.md) covers the four moments at 400 ms and 6000 ms commits, scripted and with a real model.
- [Realtime runner](./realtime.md) covers the OpenAI Agents SDK session under both `async_tool_calls` settings, real model only.
- [Hosted runners](./hosted.md) covers Gemini Live, ElevenLabs Agents, Retell and Vapi, each under its own interrupt setting. All four were driven through `verify.py` on 2026-10-23.
