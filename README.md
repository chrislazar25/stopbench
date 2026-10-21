# stopbench

When a caller says "wait, stop" while a voice agent is in the middle of a tool call, does the action still happen? And does the agent tell the caller the truth about it?

stopbench measures that, one framework at a time, with a fake refund tool that keeps a real ledger.

Each run interrupts at one of three moments:

1. After the agent decides to call the tool, before the request is sent.
2. After the request is sent, before the refund is saved.
3. After the refund is saved, before the agent replies.

Each run records two facts: whether the refund landed, and what the agent told the caller.

## Results: Pipecat 1.13.0

80 runs, 10 per cell. Every cell gave the same verdict all 10 times.

| `cancel_on_interruption` | Stop arrives | Refund landed | Model's context says | Verdict |
|---|---|---|---|---|
| `True` (default) | never (baseline) | 10/10 | the real result | ok |
| `True` (default) | before the request is sent | 0/10 | `CANCELLED` | ok, stopped |
| `True` (default) | while the request is in flight | 10/10 | `CANCELLED` | **wrong: refund landed, model told cancelled** |
| `True` (default) | after the refund is saved | 10/10 | `CANCELLED` | **wrong: refund landed, model told cancelled** |
| `False` | never (baseline) | 10/10 | the real result | ok |
| `False` | before the request is sent | 10/10 | the real result | **ignored: stop came first, refund landed anyway** |
| `False` | while the request is in flight | 10/10 | the real result | ok, told the truth |
| `False` | after the refund is saved | 10/10 | the real result | ok, told the truth |

In the in-flight cells the ledger received the request 50 ms before the stop and saved the refund 351 to 355 ms after it. In the after-saved cells it saved the refund 2 to 6 ms before the stop.

Neither setting does both jobs. The default stops what it can, then tells the model `CANCELLED` even when the refund already landed. `False` tells the truth, but a stop never stops anything.

Why, in the Pipecat 1.13.0 source:

- `LLMService._handle_interruptions` (`services/llm_service.py`) cancels the tool's asyncio task on every interruption when `cancel_on_interruption=True`. Cancelling the task does not recall a request that already left.
- `_handle_function_call_cancel` (`processors/aggregators/llm_response_universal.py`) then writes `CANCELLED` as the tool result. It does not know whether the tool's side effect happened.

What this run does not cover yet: a real LLM (so what the agent actually says to the caller), real audio and voice activity detection, and LiveKit Agents.

## Run it

```sh
python3.11 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python pipecat_run.py --runs 10
```

`ledger.py` starts as a separate process with a 400 ms commit delay. Each run's record, with timestamps, goes to `runs/pipecat.jsonl`.

## Related

[tellr](https://github.com/chrislazar25/tellr) checks whether a voice payment still matches what the caller authorized when it commits.
