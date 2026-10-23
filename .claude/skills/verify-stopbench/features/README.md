# stopbench verification map

This directory is the maintained source for verifying stopbench's user-facing behavior. Read this index, then use the matching feature file as the recipe.

## Baseline preconditions

- The current directory is the stopbench repo root.
- `python3 .claude/skills/verify-stopbench/verify.py doctor` prints `doctor: OK`.
- For paid runs, `.env` holds `OPENAI_API_KEY`. `doctor` lists key names only.
- No runner of the kind you drive is already running.

## Driving conventions

- Drive runners only through `verify.py run`, which forces `--runs 1` and writes to `.verify/`, never to `runs/`.
- Pick cells with the runner's own flags, exactly as the feature file lists them.
- Run the scripted cells before any paid cell.

## Proof and skip reporting

- A proof is the `.verify/<timestamp>/` folder: console output, jsonl, ledger db snapshot and `check.txt` with `check: PASS`.
- Quote the agent's words from the console file when a claim is about what the agent said.
- If a cell can't run (missing key, runner busy), report the command and the unmet precondition. Don't report it as verified through another cell.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behavior, then four H2 sections in this order: `Sub-features`, `How to get to it (user POV)`, `Driving it with verify.py` (starting with `Preconditions:`), and `Gotchas`.

## Features

- [Ledger](./ledger.md) covers the fake payments API: commit after hang-up and the refund lookup.
- [Pipecat runner](./pipecat.md) covers the four moments under both `cancel_on_interruption` settings, scripted and with a real model.
- [LiveKit runner](./livekit.md) covers the four moments at 400 ms and 6000 ms commits, scripted and with a real model.
- [Realtime runner](./realtime.md) covers the OpenAI Agents SDK session under both `async_tool_calls` settings, real model only.
