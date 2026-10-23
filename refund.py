"""Not in stopbench.py, which stays standard library only; this needs aiohttp."""

import asyncio

import aiohttp

from stopbench import AFTER_COMMIT_S, PREP_S, Marks


async def issue_refund(url: str, refund_id: str, amount_cents: int, marks: Marks) -> dict:
    marks.started.set()
    await asyncio.sleep(PREP_S)
    async with aiohttp.ClientSession() as session:
        marks.sent.set()
        payload = {"refund_id": refund_id, "amount_cents": amount_cents}
        async with session.post(url, json=payload) as resp:
            body = await resp.json()
    marks.committed.set()
    await asyncio.sleep(AFTER_COMMIT_S)
    return body


async def read_ledger(url: str, refund_id: str) -> dict:
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{url}/{refund_id}") as resp:
            return await resp.json()
