"""Fake refund API with a real SQLite ledger.

Runs as its own process, like a payments provider. A request that arrives is
committed even if the client hangs up while waiting, because that is what real
payment APIs do.

    python ledger.py --port 8765 --db runs/ledger.db --commit-delay-ms 400
"""

import argparse
import json
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

SCHEMA = """
CREATE TABLE IF NOT EXISTS refunds (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    refund_id TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    received_at REAL NOT NULL,
    committed_at REAL NOT NULL
)
"""


def make_handler(db_path: str, commit_delay_s: float, lock: threading.Lock):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def _send(self, code: int, body: dict):
            data = json.dumps(body).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            try:
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                pass

        def do_POST(self):
            if self.path != "/refunds":
                return self._send(404, {"error": "not found"})
            received_at = time.time()
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            time.sleep(commit_delay_s)
            committed_at = time.time()
            with lock, sqlite3.connect(db_path) as db:
                db.execute(
                    "INSERT INTO refunds (refund_id, amount_cents, received_at, committed_at)"
                    " VALUES (?, ?, ?, ?)",
                    (body["refund_id"], body["amount_cents"], received_at, committed_at),
                )
            self._send(201, {"status": "committed", "refund_id": body["refund_id"]})

        def do_GET(self):
            prefix = "/refunds/"
            if not self.path.startswith(prefix):
                return self._send(404, {"error": "not found"})
            refund_id = self.path[len(prefix):]
            with lock, sqlite3.connect(db_path) as db:
                rows = db.execute(
                    "SELECT received_at, committed_at FROM refunds WHERE refund_id = ?",
                    (refund_id,),
                ).fetchall()
            self._send(
                200,
                {
                    "refund_id": refund_id,
                    "count": len(rows),
                    "rows": [{"received_at": r[0], "committed_at": r[1]} for r in rows],
                },
            )

    return Handler


def exit_with(owner: int):
    """A runner killed without cleanup would leave this ledger serving forever."""
    while True:
        try:
            os.kill(owner, 0)
        except ProcessLookupError:
            os._exit(0)
        time.sleep(0.5)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", default="runs/ledger.db")
    parser.add_argument("--commit-delay-ms", type=int, default=400)
    parser.add_argument("--owner-pid", type=int, help="exit when this process is gone")
    args = parser.parse_args()

    with sqlite3.connect(args.db) as db:
        db.execute(SCHEMA)

    if args.owner_pid:
        threading.Thread(target=exit_with, args=(args.owner_pid,), daemon=True).start()
    handler = make_handler(args.db, args.commit_delay_ms / 1000, threading.Lock())
    server = ThreadingHTTPServer(("127.0.0.1", args.port), handler)
    print(f"ledger listening on {args.port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
