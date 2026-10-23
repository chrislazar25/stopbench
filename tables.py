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

# A reply that contains any of these, ignoring case, tells the caller about a second refund.
SECOND_REFUND = ["twice", "two $40 refund", "second $40 refund", "second refund", "second request", "duplicate refund"]

AXIS = {"pipecat": "cancel_on_interruption", "livekit": "commit_delay_ms", "openai-agents-realtime": "async_tool_calls"}
MOMENT_ORDER = [m.value for m in Moment]


def span(values: list[float]) -> str:
    if not values:
        return ""
    lo, hi = round(min(values)), round(max(values))
    return f"{lo}" if lo == hi else f"{lo} to {hi}"


def reply_starts(record: dict) -> list[float]:
    """Start times of the responses after the stop's user item, as the server sent them."""
    wire = record["wire"]
    anchor = next((i for i, e in enumerate(wire)
                   if e["event"] == "item" and e["kind"] == "user" and e["text"] == USER_STOP), None)
    if anchor is None:
        return []
    return [e["t"] for e in wire[anchor + 1:] if e["event"] == "response_start"]


def since_stop(record: dict, t: float | None) -> float | None:
    stop = record["interrupted_at"]
    return None if t is None or stop is None else (t - stop) * 1000


def table(records: list[dict]) -> str:
    framework = records[0]["framework"]
    axis = AXIS[framework]
    realtime = framework == "openai-agents-realtime"
    cells = defaultdict(list)
    for r in records:
        cells[(r[axis], r["moment"])].append(r)

    head = [axis, "stop", "n", "landed", "model had", "verdicts", "request received", "refund saved"]
    if realtime:
        head += ["reply starts", "later reply starts"]
    rows = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for (setting, moment), rs in sorted(cells.items(), key=lambda c: (str(c[0][0]), MOMENT_ORDER.index(c[0][1]))):
        stopped = moment != Moment.NONE.value

        def times(key):
            return [x for r in rs if (x := since_stop(r, r[key])) is not None] if stopped else []

        row = [
            str(setting), moment, str(len(rs)), f"{sum(r['landed'] for r in rs)}/{len(rs)}",
            ", ".join(f"{k} {v}" for k, v in Counter(r["told"] for r in rs).items()),
            ", ".join(f"{k} {v}" for k, v in Counter(r["verdict"].split(":")[0] for r in rs).items()),
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
    frameworks = {r["framework"] for r in records}
    if len(frameworks) != 1:
        sys.exit(f"pass files from one framework at a time, got {sorted(frameworks)}")
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
