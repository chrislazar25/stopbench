---
name: verify-stopbench
description: Drive the stopbench CLI runners (pipecat_run.py, livekit_run.py, realtime_run.py) and the fake payments ledger the way a user does, then prove the result against the ledger's SQLite rows. Use before claiming a runner change works, before adding README numbers, or when a run looks wrong.
---

# Verify stopbench

stopbench is a set of command-line runners. Each one starts `ledger.py` (a fake payments API with a SQLite ledger), tells a voice agent "Wait, stop. Don't do the refund." at one of four moments while its refund tool runs, and writes one JSON line per run. A user touches two things: the runner commands and the jsonl they write. The ledger's HTTP API is the third thing worth driving on its own.

Every command below runs from the repo root. The helper is stdlib-only and runs with any `python3`:

```sh
python3 .claude/skills/verify-stopbench/verify.py doctor
python3 .claude/skills/verify-stopbench/verify.py ledger
python3 .claude/skills/verify-stopbench/verify.py run <pipecat|livekit|realtime> [runner flags]
python3 .claude/skills/verify-stopbench/verify.py check <file.jsonl> <pipecat|livekit|realtime>
```

## Launch

There is no server to keep alive. Launch means building each runner's venv once. Python 3.11 is required.

```sh
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
python3.11 -m venv .venv-livekit && .venv-livekit/bin/pip install -r requirements-livekit.txt
python3.11 -m venv .venv-realtime && .venv-realtime/bin/pip install -r requirements-realtime.txt
```

Each `run` starts its own `ledger.py` on a free port and stops it when the matrix ends. The runner is ready when it prints its first result line. The realtime runner and every `--llm openai` run need `OPENAI_API_KEY` in `.env`, and they spend money (cents per run).

## Doctor

Run `verify.py doctor` first, and again whenever anything looks off. It is read-only. It checks:

- the current directory is the repo root;
- each venv exists and has the exact pinned versions from its requirements file;
- no runner of the same kind is already running;
- which key names `.env` holds (names only, never values).

`doctor: OK` means every instance is worth driving.

## Drive

`verify.py run <name> [flags]` runs the runner with `--runs 1` and your flags, writes to `.verify/<timestamp>/<name>.jsonl`, saves the console output, snapshots that runner's ledger db, then runs `check`. Pass the runner's own flags to pick cells:

| Runner | Cell flags | Cost |
|---|---|---|
| `pipecat` | `--moments ...`, `--cancel true false`, `--llm openai` | free when scripted |
| `livekit` | `--moments ...`, `--commit-delay-ms 400 6000`, `--llm openai --no-chain` | free when scripted |
| `realtime` | `--moments ...`, `--async-tools true false`, `--commit-delay-ms ...` | paid, real model only |

`verify.py ledger` drives the ledger alone. It starts its own `ledger.py` with a db inside the evidence folder, posts a refund, hangs up after 0.1 s, and reads the refund back.

The feature map in `features/` has the recipe for each one. Start with `features/README.md`.

## Evidence

Proof goes to `.verify/<timestamp>/`, which is gitignored and never deleted by cleanup:

- `<name>.console.txt` holds the runner's output, including the verdict and the agent's words for each run;
- `<name>.jsonl` holds the records;
- the ledger db snapshot is the payments side of the story;
- `<name>.check.txt` holds the check's per-cell table and every failure.

`check` proves three things for each record, and exits 1 if any fails:

- The ledger's row count for the record's `refund_id` equals `ledger_count`.
- The stop landed in its moment's window, measured from the ledger's own timestamps. `before_send` means the request arrived after the stop or never arrived. `in_flight` means the request arrived before the stop and committed after it. `after_commit` means the refund committed before the stop.
- For realtime, `read_wire` rebuilds every derived field from the stored server events.

A verdict string alone is not proof. Read the agent's words in the console file too, because no code classifies what the agent said.

## Cleanup

Each runner terminates the ledger it started. `verify.py ledger` terminates and waits for the one it started. Never kill ledgers or runners by name: another session may be mid-matrix. Each runner deletes its own `runs/ledger*.db` at start, which is why `doctor` and `run` refuse while a runner of the same kind is running. Delete old `.verify/` folders by hand when you no longer need them.

## Isolation

Different runners can run side by side, since each has its own venv, ledger port and db. Two runs of the same runner cannot: the second deletes the first's ledger db. Never pass `--out` pointing at a committed `runs/*.jsonl`, because the runners overwrite the file.
