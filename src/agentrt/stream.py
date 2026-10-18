"""Event stream with fencing, dedup, and resume-from-sequence.

StreamHub is the in-process stand-in for the delivery tier (gRPC/Socket.IO fan-out).
  * publish() rejects events carrying a stale fencing token (zombie worker)
  * publish() drops duplicate sequence numbers (replay after crash re-emits events)
  * subscribe(from_seq) replays history then tails live events (reconnect support)
serve() exposes it over a newline-delimited-JSON TCP protocol so you can test reconnects.
"""
from __future__ import annotations

import asyncio
import json
from collections import defaultdict


class StaleFence(Exception):
    pass


class StreamHub:
    def __init__(self):
        self.events: dict[str, list[dict]] = defaultdict(list)
        self.max_token: dict[str, int] = defaultdict(int)
        self.subs: dict[str, list[asyncio.Queue]] = defaultdict(list)

    def publish(self, session_id: str, event: dict, token: int) -> bool:
        if token < self.max_token[session_id]:
            raise StaleFence(f"{session_id}: token {token} < {self.max_token[session_id]}")
        self.max_token[session_id] = token
        log = self.events[session_id]
        if log and event["seq"] <= log[-1]["seq"]:
            return False                              # duplicate (idempotent publish)
        log.append(event)
        for q in self.subs[session_id]:
            q.put_nowait(event)
        return True

    async def subscribe(self, session_id: str, from_seq: int = 1):
        q: asyncio.Queue = asyncio.Queue()
        # No awaits between snapshot and registration => no gap, no overlap.
        backlog = [e for e in self.events[session_id] if e["seq"] >= from_seq]
        self.subs[session_id].append(q)
        try:
            for e in backlog:
                yield e
                if e["kind"] == "final":
                    return
            while True:
                e = await q.get()
                yield e
                if e["kind"] == "final":
                    return
        finally:
            self.subs[session_id].remove(q)


async def serve(hub: StreamHub, host: str = "127.0.0.1", port: int = 0) -> asyncio.Server:
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        gen = None
        try:
            req = json.loads(await reader.readline())
            gen = hub.subscribe(req["session_id"], req.get("from_seq", 1))
            async for ev in gen:
                writer.write((json.dumps(ev) + "\n").encode())
                await writer.drain()
        except (ConnectionError, asyncio.CancelledError):
            pass
        finally:
            if gen is not None:
                await gen.aclose()
            writer.close()

    return await asyncio.start_server(handle, host, port)
