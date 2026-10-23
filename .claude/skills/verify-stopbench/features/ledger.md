# Ledger

`ledger.py` plays the payment provider. It accepts a refund, waits its commit delay, and commits the refund even if the caller hung up, the way real payment APIs do. A lookup returns how many times a refund id was committed.

## Sub-features

- `ledger-commit` commits a POSTed refund after the commit delay.
- `ledger-hangup` still commits when the client disconnects during the delay.
- `ledger-lookup` returns the count and timestamps for one refund id.

## How to get to it (user POV)

- Every runner starts it as a child process on a free port.
- `python3 ledger.py --port <port> --db <path> --commit-delay-ms <ms>` starts it by hand.
- `POST /refunds` with `{"refund_id", "amount_cents"}` and `GET /refunds/<refund_id>`.

## Driving it with verify.py

Preconditions:

- `verify.py doctor` prints `doctor: OK`.

- **Hang up mid-commit.** Run `python3 .claude/skills/verify-stopbench/verify.py ledger`. The output shows `POST hung up after 0.1 s: TimeoutError`.
- **Look the refund up.** The same command GETs `/refunds/verify-hangup`. The output shows `"count": 1` with `committed_at` about 400 ms after `received_at`.
- **Proof.** `ledger: PASS (committed although the client hung up)`, and `.verify/<timestamp>/ledger.txt` plus `ledger.db` hold the same facts.

## Gotchas

- The ledger replies 201 with a flat body. Platforms that need 200 or a wrapped body (Vapi) can't call it directly.
- `committed_at` is taken just before the INSERT, not after it.
- The ledger has no auth. Don't expose it through a tunnel without something in front of it.
