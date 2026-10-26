#!/usr/bin/env python3
"""Drive stopbench the way a user does and keep the proof.

Run from the repo root with any python3 (stdlib only):

    python3 .claude/skills/verify-stopbench/verify.py doctor
    python3 .claude/skills/verify-stopbench/verify.py ledger
    python3 .claude/skills/verify-stopbench/verify.py run pipecat --moments in_flight --cancel true
    python3 .claude/skills/verify-stopbench/verify.py check .verify/<id>/pipecat.jsonl pipecat [.verify/<id>/pipecat.db]

Runners: pipecat, livekit, realtime, gemini, elevenlabs, retell, vapi.

Evidence goes to .verify/<id>/ (gitignored) and survives cleanup.
"""

import json
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.request
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path.cwd()
HOSTED = {"venv": ".venv-hosted", "req": "requirements-hosted.txt", "lock": "requirements-hosted.lock.txt", "paid": True}
RUNNERS = {
    "pipecat": {"script": "pipecat_run.py", "module": "pipecat_run", "venv": ".venv", "req": "requirements.txt", "lock": "requirements.lock.txt",
                "axis": "cancel_on_interruption", "paid": False, "key": None},
    "livekit": {"script": "livekit_run.py", "module": "livekit_run", "venv": ".venv-livekit", "req": "requirements-livekit.txt", "lock": "requirements-livekit.lock.txt",
                "axis": "commit_delay_ms", "paid": False, "key": None},
    "realtime": {"script": "realtime_run.py", "module": "realtime_run", "venv": ".venv-realtime", "req": "requirements-realtime.txt", "lock": "requirements-realtime.lock.txt",
                 "axis": "async_tool_calls", "paid": True, "key": "OPENAI_API_KEY"},
    "gemini": {"script": "gemini_run.py", "module": "gemini_run", **HOSTED, "axis": "behavior", "key": "GOOGLE_API_KEY"},
    "elevenlabs": {"script": "elevenlabs_run.py", "module": "elevenlabs_run", **HOSTED, "axis": "interruption_mode", "key": "ELEVENLABS_API_KEY"},
    "retell": {"script": "retell_run.py", "module": "retell_run", **HOSTED, "axis": "speak_after_execution", "key": "RETELL_API_KEY"},
    "vapi": {"script": "vapi_run.py", "module": "vapi_run", **HOSTED, "axis": "async_tool", "key": "VAPI_PRIVATE_KEY"},
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
    checked = set()
    for name, r in RUNNERS.items():
        if r["venv"] in checked:
            continue
        checked.add(r["venv"])
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
        frozen = set(subprocess.run([py, "-m", "pip", "freeze"], capture_output=True, text=True).stdout.split())
        locked = set((ROOT / r["lock"]).read_text().split())
        if frozen != locked:
            drift = sorted(f"+{p}" for p in frozen - locked) + sorted(f"-{p}" for p in locked - frozen)
            problems.append(f"{name}: venv differs from {r['lock']}: {' '.join(drift)}")
        else:
            print(f"{name:9} matches {r['lock']} ({len(locked)} packages)")
    names = env_names()
    print(f"keys in .env: {sorted(names) or 'none'}")
    for name, r in RUNNERS.items():
        if r["key"] and r["key"] not in names:
            problems.append(f"{r['key']} missing from .env (needed for {name})")
    for p in problems:
        print(f"FAIL {p}")
    print("doctor: OK" if not problems else f"doctor: {len(problems)} problem(s)")
    return 1 if problems else 0


def evidence_dir(label: str) -> Path:
    (ROOT / ".verify").mkdir(exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=f"{time.strftime('%Y%m%d-%H%M%S')}-{label}-", dir=ROOT / ".verify"))


def ledger() -> int:
    """Start our own ledger, POST a refund and hang up mid-commit, then check it committed."""
    out = evidence_dir("ledger")
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
    if r["paid"] or "--llm" in extra:
        print("note: this calls a paid API")
    out = evidence_dir(name)
    jsonl = out / f"{name}.jsonl"
    cmd = [str(python(name)), r["script"], "--runs", "1", *extra, "--out", str(jsonl)]
    print("$ " + " ".join(cmd))
    with open(out / f"{name}.console.txt", "w") as log:
        code = subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT).returncode
    print((out / f"{name}.console.txt").read_text().rstrip())
    if code != 0:
        print(f"FAIL runner exited {code}")
        return 1
    return check(jsonl, name, jsonl.with_suffix(".db"), out)


def check(jsonl: Path, name: str, db: Path | None = None, out: Path | None = None) -> int:
    sys.path.insert(0, str(ROOT))
    from stopbench import Moment, Told, verdict

    r = RUNNERS[name]
    recs = [json.loads(line) for line in open(jsonl)]
    if db is None and jsonl.with_suffix(".db").exists():
        db = jsonl.with_suffix(".db")
    con = sqlite3.connect(db) if db else None
    failures = []
    cells = defaultdict(list)
    for rec in recs:
        n = rec["ledger_count"]
        if con:
            n = con.execute("SELECT count(*) FROM refunds WHERE refund_id = ?", (rec["refund_id"],)).fetchone()[0]
            if n != rec["ledger_count"]:
                failures.append(f"{rec['refund_id']}: ledger has {n}, record says {rec['ledger_count']}")
        stop = rec.get("interrupted_at")
        if rec["moment"] in WINDOWS and stop and rec.get("tool_called", True):
            ms = lambda t: None if t is None else (t - stop) * 1000
            if not WINDOWS[rec["moment"]](ms(rec["received_at"]), ms(rec["committed_at"])):
                failures.append(f"{rec['refund_id']}: stop outside the {rec['moment']} window")
        expected = verdict(rec["tool_called"], n, Moment(rec["moment"]), Told(rec["told"]))
        if rec["verdict"] != expected or rec["landed"] != (n > 0):
            failures.append(f"{rec['refund_id']}: record says {rec['verdict']!r}, the ledger and told give {expected!r}")
        cells[(rec[r["axis"]], rec["moment"])].append(rec)
    failures += rederive(jsonl, name)
    lines = [f"{'axis':8} {'moment':13} {'n':>2} {'landed':>6}  verdicts"]
    for (axis, moment), rs in sorted(cells.items(), key=str):
        verdicts = dict(Counter(x["verdict"].split(":")[0] for x in rs))
        lines.append(f"{str(axis):8} {moment:13} {len(rs):>2} {sum(x['landed'] for x in rs):>6}  {verdicts}")
    lines += [f"FAIL {f}" for f in failures]
    ledger = f"ledger {db}" if db else "ledger NOT checked: no db given, counts taken from the records"
    lines.append(f"check: {'PASS' if not failures else 'FAIL'} ({len(recs)} records, {ledger})")
    report = "\n".join(lines)
    (out or evidence_dir(f"check-{name}")).joinpath(f"{jsonl.stem}.check.txt").write_text(report + "\n")
    print(report)
    return 1 if failures else 0


REDERIVE = """
import json, sys
import {module} as runner
bad = []
for line in open(sys.argv[1]):
    r = json.loads(line)
    if hasattr(runner, "rederive"):
        if any(r[k] != v for k, v in runner.rederive(r).items()):
            bad.append(f"{{r['refund_id']}}: fields don't re-derive from the stored evidence")
    if r["told"] != runner.told_from(r).value or r["told_source"] != runner.TOLD_SOURCE:
        bad.append(f"{{r['refund_id']}}: told {{r['told']!r}} doesn't follow from the record")
print(json.dumps(bad))
"""


def rederive(jsonl: Path, name: str) -> list[str]:
    """Runs in the runner's venv: importing the runner imports its framework."""
    code = REDERIVE.format(module=RUNNERS[name]["module"])
    out = subprocess.run([str(python(name)), "-c", code, str(jsonl)], capture_output=True, text=True, cwd=ROOT)
    if out.returncode != 0:
        return [f"re-derivation crashed: {out.stderr.strip()[-300:]}"]
    return json.loads(out.stdout.strip().splitlines()[-1])


def main() -> int:
    args = sys.argv[1:]
    match args:
        case ["doctor"]:
            return doctor()
        case ["ledger"]:
            return ledger()
        case ["run", name, *extra] if name in RUNNERS:
            return run(name, extra)
        case ["check", path, name, *db] if name in RUNNERS and len(db) <= 1:
            return check(Path(path), name, Path(db[0]) if db else None)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
