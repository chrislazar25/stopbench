"""Print the numbers behind the README's results tables, straight from the records.

Standard library only, so anyone can run it on the committed files:

    python3 tables.py runs/pipecat.jsonl
    python3 tables.py runs/livekit.jsonl runs/livekit-gpt-6.1-sol-unchained.jsonl   # cells pooled across files
    python3 tables.py --second-refund runs/livekit-gpt-6.1-sol.jsonl runs/livekit-chain-control.jsonl

Times are milliseconds from the stop, so a negative number happened before it. Realtime
records also get when the reply to the stop started, and when a later reply started,
read from the server events they keep.
"""

import json
import sys
from collections import Counter, defaultdict

from stopbench import USER_STOP, Moment

SECOND_REFUND = ["twice", "two $40 refund", "second $40 refund", "second refund", "second request", "duplicate refund"]

AXIS = {"pipecat": "cancel_on_interruption", "livekit": "commit_delay_ms", "openai-agents-realtime": "async_tool_calls"}
MOMENT_ORDER = [m.value for m in Moment]


def span(values: list[float | None]) -> str:
    values = [v for v in values if v is not None]
    if not values:
        return ""
    lo, hi = round(min(values)), round(max(values))
    return f"{lo}" if lo == hi else f"{lo} to {hi}"


def reply_starts(record: dict) -> list[float]:
    wire = record["wire"]
    anchor = next((i for i, e in enumerate(wire)
                   if e["event"] == "item" and e["kind"] == "user" and e["text"] == USER_STOP), None)
    if anchor is None:
        return []
    return [e["t"] for e in wire[anchor + 1:] if e["event"] == "response_start"]


def since_stop(record: dict, t: float | None) -> float | None:
    stop = record["interrupted_at"]
    return None if t is None or stop is None else (t - stop) * 1000


def verdict_label(verdict: str) -> str:
    """ "ok: landed, ..." and "ok: stopped" stay apart; the rest go by their first word."""
    return verdict.split(",")[0] if verdict.startswith("ok") else verdict.split(":")[0]


def counts(values) -> str:
    return ", ".join(f"{k} {v}" for k, v in Counter(values).items())


def table(records: list[dict]) -> str:
    framework = records[0]["framework"]
    axis = AXIS[framework]
    realtime = framework == "openai-agents-realtime"
    # Pooled files must not merge runs that differ in commit delay; a different model is
    # pooled on purpose (scripted + real LiveKit) and shown in the "runs" column.
    delay = "commit_delay_ms" if axis != "commit_delay_ms" and "commit_delay_ms" in records[0] else None
    cells = defaultdict(list)
    for r in records:
        cells[(r[axis], r[delay] if delay else 0, MOMENT_ORDER.index(r["moment"]))].append(r)

    head = [axis, *([delay] if delay else []), "stop", "runs", "landed", "model had", "verdicts",
            "request received", "refund saved"]
    if realtime:
        head += ["reply starts", "later reply starts"]
    rows = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for (setting, delay_ms, moment_index), rs in sorted(cells.items()):
        moment = MOMENT_ORDER[moment_index]
        stopped = moment != Moment.NONE.value

        def times(key):
            return [since_stop(r, r[key]) for r in rs] if stopped else []

        row = [
            str(setting), *([str(delay_ms)] if delay else []), moment, counts(r["llm"] for r in rs),
            f"{sum(r['landed'] for r in rs)}/{len(rs)}",
            counts(r["told"] for r in rs), counts(verdict_label(r["verdict"]) for r in rs),
            span(times("received_at")), span(times("committed_at")),
        ]
        if realtime:
            starts = [reply_starts(r) for r in rs] if stopped else []
            row += [span([since_stop(r, s[0]) for r, s in zip(rs, starts) if s]),
                    span([since_stop(r, s[1]) for r, s in zip(rs, starts) if len(s) > 1])]
        rows.append("| " + " | ".join(row) + " |")
    return "\n".join(rows)


def replies(record: dict) -> str:
    # runs/livekit-chain-control.jsonl keeps one "reply" per run; runner records split it in two.
    parts = [record.get("reply"), record.get("agent_reply"), record.get("followup_reply")]
    return " ".join(p for p in parts if p).replace("\u2019", "'").lower()


def second_refund(paths: list[str]):
    for path in paths:
        groups = defaultdict(list)
        for line in open(path):
            r = json.loads(line)
            groups[r.get("mode") or ("baseline" if r["moment"] == Moment.NONE.value else "stop")].append(r)
        counts = ", ".join(
            f"{name} {sum(any(p in replies(r) for p in SECOND_REFUND) for r in rs)}/{len(rs)}"
            for name, rs in sorted(groups.items())
        )
        print(f"{path}: replies telling the caller about a second refund: {counts}")


def main(paths: list[str]):
    records = [json.loads(line) for p in paths for line in open(p)]
    frameworks = {r.get("framework") for r in records}
    if len(frameworks) != 1 or not frameworks <= AXIS.keys():
        sys.exit(f"pass runner records from one framework at a time, got {sorted(map(str, frameworks))}"
                 " (runs/livekit-chain-control.jsonl only works with --second-refund)")
    print(f"{len(records)} records from {', '.join(paths)}\n")
    print(table(records))


if __name__ == "__main__":
    match sys.argv[1:]:
        case ["--second-refund", *paths] if paths:
            second_refund(paths)
        case [_, *_] as paths:
            main(paths)
        case _:
            sys.exit(__doc__)
