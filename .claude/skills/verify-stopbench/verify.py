#!/usr/bin/env python3
"""Drive stopbench the way a user does and keep the proof.

Run from the repo root with any python3 (stdlib only):

    python3 .claude/skills/verify-stopbench/verify.py doctor
    python3 .claude/skills/verify-stopbench/verify.py ledger
    python3 .claude/skills/verify-stopbench/verify.py run pipecat --moments in_flight --cancel true
    python3 .claude/skills/verify-stopbench/verify.py check .verify/<id>/pipecat.jsonl pipecat

Evidence goes to .verify/<id>/ (gitignored) and survives cleanup.
"""

import json
import re
import sqlite3
import subprocess
import sys
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path.cwd()
RUNNERS = {
    "pipecat": {"script": "pipecat_run.py", "venv": ".venv", "req": "requirements.txt",
                "db": "runs/ledger.db", "axis": "cancel_on_interruption", "paid": False},
    "livekit": {"script": "livekit_run.py", "venv": ".venv-livekit", "req": "requirements-livekit.txt",
                "db": "runs/ledger-livekit.db", "axis": "commit_delay_ms", "paid": False},
    "realtime": {"script": "realtime_run.py", "venv": ".venv-realtime", "req": "requirements-realtime.txt",
                 "db": "runs/ledger-realtime.db", "axis": "async_tool_calls", "paid": True},
}
# Where each stop moment must land relative to the ledger, in ms from the stop. The harness
# builds these windows (stopbench.py), so a run outside one measured the wrong moment.
WINDOWS = {
    "before_send": lambda recv, commit: recv is None or recv > 0,
    "in_flight": lambda recv, commit: recv is not None and recv < 0 < commit,
    "after_commit": lambda recv, commit: commit is not None and commit < 0,
}


def python(name: str) -> Path:
    return ROOT / RUNNERS[name]["venv"] / "bin" / "python"


def running(script: str) -> list[str]:
    out = subprocess.run(["pgrep", "-fl", script], capture_output=True, text=True).stdout
    return [line for line in out.splitlines() if "verify.py" not in line and "pgrep" not in line]


def env_names() -> set[str]:
    env = ROOT / ".env"
    if not env.exists():
        return set()
    return {m.group(1) for m in re.finditer(r"^([A-Z_]+)=", env.read_text(), re.M)}


def doctor() -> int:
    problems = []
    if not (ROOT / "stopbench.py").exists():
        print("FAIL not at the stopbench repo root (no stopbench.py here)")
        return 1
    for name, r in RUNNERS.items():
        py = python(name)
        if not py.exists():
            problems.append(f"{name}: missing {r['venv']} (python3.11 -m venv {r['venv']} && {r['venv']}/bin/pip install -r {r['req']})")
            continue
        for line in (ROOT / r["req"]).read_text().split():
            pkg, _, want = line.partition("==")
            pkg = pkg.split("[")[0]
            have = subprocess.run([py, "-m", "pip", "show", pkg], capture_output=True, text=True).stdout
            got = re.search(r"^Version: (.+)$", have, re.M)
            status = "ok" if got and got.group(1) == want else f"MISMATCH (have {got.group(1) if got else 'none'})"
            print(f"{name:9} {pkg}=={want}: {status}")
            if status != "ok":
                problems.append(f"{name}: {pkg} {status}")
        busy = running(r["script"])
        if busy:
            problems.append(f"{name}: already running, don't drive it now: {busy}")
    names = env_names()
    print(f"keys in .env: {sorted(names) or 'none'}")
    if "OPENAI_API_KEY" not in names:
        problems.append("OPENAI_API_KEY missing from .env (needed for realtime and --llm openai)")
    for p in problems:
        print(f"FAIL {p}")
    print("doctor: OK" if not problems else f"doctor: {len(problems)} problem(s)")
    return 1 if problems else 0


def evidence_dir() -> Path:
    d = ROOT / ".verify" / time.strftime("%Y%m%d-%H%M%S")
    d.mkdir(parents=True, exist_ok=True)
    return d


def ledger() -> int:
    """Start our own ledger, POST a refund and hang up mid-commit, then check it committed."""
    out = evidence_dir()
    db = out / "ledger.db"
    sys.path.insert(0, str(ROOT))
    from stopbench import free_port

    port = free_port()
    proc = subprocess.Popen([sys.executable, "ledger.py", "--port", str(port), "--db", str(db),
                             "--commit-delay-ms", "400"], stdout=subprocess.PIPE, text=True)
    lines = []
    try:
        ready = proc.stdout.readline().strip()
        lines.append(f"ledger: {ready!r}")
        body = json.dumps({"refund_id": "verify-hangup", "amount_cents": 4000}).encode()
        req = urllib.request.Request(f"http://127.0.0.1:{port}/refunds", data=body,
                                     headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=0.1)
            lines.append("POST returned before the commit delay: unexpected")
        except Exception as e:
            lines.append(f"POST hung up after 0.1 s: {type(e).__name__}")
        time.sleep(0.6)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/refunds/verify-hangup") as resp:
            got = json.load(resp)
        lines.append(f"GET /refunds/verify-hangup: {json.dumps(got)}")
        ok = got["count"] == 1
        lines.append("ledger: PASS (committed although the client hung up)" if ok else "ledger: FAIL")
    finally:
        proc.terminate()
        proc.wait(timeout=5)
    (out / "ledger.txt").write_text("\n".join(lines) + "\n")
    print("\n".join(lines))
    print(f"evidence: {out}")
    return 0 if ok else 1


def run(name: str, extra: list[str]) -> int:
    r = RUNNERS[name]
    if running(r["script"]):
        print(f"FAIL {r['script']} is already running. It deletes {r['db']} at start, so a second run would wipe its ledger.")
        return 1
    if r["paid"] or "--llm" in extra:
        print("note: this calls a paid API")
    out = evidence_dir()
    jsonl = out / f"{name}.jsonl"
    cmd = [str(python(name)), r["script"], "--runs", "1", *extra, "--out", str(jsonl)]
    print("$ " + " ".join(cmd))
    with open(out / f"{name}.console.txt", "w") as log:
        code = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT).returncode
    print((out / f"{name}.console.txt").read_text().rstrip())
    if code != 0:
        print(f"FAIL runner exited {code}")
        return 1
    snapshot = out / Path(r["db"]).name
    snapshot.write_bytes((ROOT / r["db"]).read_bytes())
    return check(jsonl, name, snapshot)


def check(jsonl: Path, name: str, db: Path | None = None) -> int:
    """Each record: ledger agrees, the stop landed in its moment's window, realtime fields re-derive."""
    r = RUNNERS[name]
    db = db or ROOT / r["db"]
    recs = [json.loads(line) for line in open(jsonl)]
    con = sqlite3.connect(db)
    failures = []
    cells = defaultdict(list)
    for rec in recs:
        n = con.execute("SELECT count(*) FROM refunds WHERE refund_id = ?", (rec["refund_id"],)).fetchone()[0]
        if n != rec["ledger_count"]:
            failures.append(f"{rec['refund_id']}: ledger has {n}, record says {rec['ledger_count']}")
        stop = rec.get("interrupted_at")
        if rec["moment"] in WINDOWS and stop and rec.get("tool_called", True):
            ms = lambda t: None if t is None else (t - stop) * 1000
            if not WINDOWS[rec["moment"]](ms(rec["received_at"]), ms(rec["committed_at"])):
                failures.append(f"{rec['refund_id']}: stop outside the {rec['moment']} window")
        cells[(rec[r["axis"]], rec["moment"])].append(rec)
    if name == "realtime":
        failures += rederive_realtime(jsonl)
    lines = [f"{'axis':8} {'moment':13} {'n':>2} {'landed':>6}  verdicts"]
    for (axis, moment), rs in sorted(cells.items(), key=str):
        verdicts = dict(Counter(x["verdict"].split(":")[0] for x in rs))
        lines.append(f"{str(axis):8} {moment:13} {len(rs):>2} {sum(x['landed'] for x in rs):>6}  {verdicts}")
    lines += [f"FAIL {f}" for f in failures]
    lines.append(f"check: {'PASS' if not failures else 'FAIL'} ({len(recs)} records, db {db})")
    report = "\n".join(lines)
    (jsonl.parent / f"{jsonl.stem}.check.txt").write_text(report + "\n")
    print(report)
    return 1 if failures else 0


def rederive_realtime(jsonl: Path) -> list[str]:
    code = (
        "import json, sys\n"
        "from realtime_run import load_wire, read_wire\n"
        "bad = 0\n"
        "for line in open(sys.argv[1]):\n"
        "    r = json.loads(line)\n"
        "    reading = read_wire(load_wire(r['wire']), r['moment'] != 'none')\n"
        "    bad += any(r[k] != v for k, v in reading.__dict__.items())\n"
        "print(bad)\n"
    )
    out = subprocess.run([str(python("realtime")), "-c", code, str(jsonl)], capture_output=True, text=True)
    if out.returncode != 0:
        return [f"read_wire re-derivation crashed: {out.stderr.strip()[-300:]}"]
    bad = int(out.stdout.strip())
    return [f"{bad} realtime record(s) don't re-derive from their wire"] if bad else []


def main() -> int:
    args = sys.argv[1:]
    match args:
        case ["doctor"]:
            return doctor()
        case ["ledger"]:
            return ledger()
        case ["run", name, *extra] if name in RUNNERS:
            return run(name, extra)
        case ["check", path, name] if name in RUNNERS:
            return check(Path(path), name)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
