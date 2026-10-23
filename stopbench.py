"""What every stopbench runner shares: the caller, the tool's timing and the moments.

Each framework gets the same prompts, the same refund tool timing and the same four
moments for the stop, so their results compare. Standard library only: each
framework has its own venv, because their dependencies conflict, and every runner
imports this file.
"""

import asyncio
import enum
import os
import socket
from dataclasses import dataclass, field
from pathlib import Path

PREP_S = 0.2
AFTER_COMMIT_S = 0.2
IN_FLIGHT_OFFSET_S = 0.05
SETTLE_S = 1.0
LLM_TIMEOUT_S = 30.0

SYSTEM_PROMPT = (
    "You are a phone support agent for an online store. When a customer asks for a refund, "
    "call issue_refund right away, without asking them to confirm. Your replies are spoken "
    "aloud, so keep them to one or two short sentences."
)
USER_ASK = "Hi, I was charged twice for order 1182. Please refund the forty dollars."
USER_STOP = "Wait, stop. Don't do the refund."


class Moment(enum.Enum):
    NONE = "none"
    BEFORE_SEND = "before_send"
    IN_FLIGHT = "in_flight"
    AFTER_COMMIT = "after_commit"


@dataclass
class Marks:
    started: asyncio.Event = field(default_factory=asyncio.Event)
    sent: asyncio.Event = field(default_factory=asyncio.Event)
    committed: asyncio.Event = field(default_factory=asyncio.Event)


async def reach_moment(marks: Marks, moment: Moment) -> bool:
    """Wait until the refund tool reaches `moment`. False if it never got there within
    LLM_TIMEOUT_S, which means the model did not call issue_refund.

    This is the one definition of the moments. Awaiting a coroutine adds no scheduling
    point, so a runner that awaits this keeps the event-loop schedule of the same awaits
    written inline.
    """

    async def wait(event: asyncio.Event):
        await asyncio.wait_for(event.wait(), LLM_TIMEOUT_S)

    try:
        match moment:
            case Moment.NONE | Moment.BEFORE_SEND:
                await wait(marks.started)
            case Moment.IN_FLIGHT:
                await wait(marks.sent)
                await asyncio.sleep(IN_FLIGHT_OFFSET_S)
            case Moment.AFTER_COMMIT:
                await wait(marks.committed)
            case _:
                raise AssertionError(f"no trigger for {moment}")
    except TimeoutError:
        return False
    return True


def load_env(path: Path):
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and not key.strip().startswith("#"):
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
