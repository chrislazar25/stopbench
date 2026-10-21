# stopbench

When a caller says "wait, stop" while a voice agent is in the middle of a tool call, does the action still happen? And does the agent tell the caller the truth about it?

stopbench measures that, one framework at a time, with a fake refund tool that keeps a real ledger.

Each run interrupts at one of three moments:

1. After the agent decides to call the tool, before the request is sent.
2. After the request is sent, before the refund is saved.
3. After the refund is saved, before the agent replies.

Each run records two facts: whether the refund landed, and what the agent told the caller.

## Status

Work in progress. Pipecat first, then LiveKit Agents. No results yet.

## Related

[tellr](https://github.com/chrislazar25/tellr) checks whether a voice payment still matches what the caller authorized when it commits.
