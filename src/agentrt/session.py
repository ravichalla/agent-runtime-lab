"""One agent session = one lease + four cooperating tasks in an asyncio.TaskGroup:

    heartbeat     renews the lease every ttl/3; failure raises LeaseLost
    poller        pulls messages from the session's FIFO queue into a bounded inbox
    publisher     drains the bounded outbox to the stream, carrying the fencing token
    orchestrator  runs the state machine; when DONE, drains outbox and ends the group

Structured concurrency gives the failure semantics we want: if ANY task raises
(lease lost, stale fence, tool crash), the siblings are cancelled and the error
surfaces as an ExceptionGroup. The lease is only released on clean completion; on
failure it is left to expire, exactly like a crashed process.

Blocking the event loop (sync CPU work, sync I/O) starves the heartbeat and loses the
lease. All boto3 calls therefore go through asyncio.to_thread.
"""
from __future__ import annotations

import asyncio

from .bus import SessionQueue
from .lease import LeaseLost, LeaseManager
from .orchestrator import Orchestrator, new_state
from .store import CheckpointStore
from .stream import StaleFence, StreamHub


class _Finished(Exception):
    """Internal sentinel: orchestrator completed; tear the group down cleanly."""


class SessionRunner:
    def __init__(self, session_id: str, *, lease_mgr: LeaseManager, store: CheckpointStore,
                 queue: SessionQueue, tools, planner, hub: StreamHub,
                 poll_interval: float = 0.05, tool_timeout: float = 5.0, queue_size: int = 16):
        self.sid, self.lease_mgr, self.store, self.queue = session_id, lease_mgr, store, queue
        self.tools, self.planner, self.hub = tools, planner, hub
        self.poll_interval, self.tool_timeout = poll_interval, tool_timeout
        self.inbox: asyncio.Queue = asyncio.Queue(maxsize=queue_size)    # backpressure
        self.outbox: asyncio.Queue = asyncio.Queue(maxsize=queue_size)   # backpressure
        self.lease = None
        self.state: dict = {}
        self._ver = 0

    async def run(self) -> dict:
        """Raises LeaseHeld if another worker owns the session; ExceptionGroup on failure."""
        self.lease = await asyncio.to_thread(self.lease_mgr.acquire, self.sid)
        self.state, self._ver = await asyncio.to_thread(self.store.load, self.sid)
        self.state = self.state or new_state()
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._heartbeat())
                tg.create_task(self._poll())
                tg.create_task(self._publish())
                tg.create_task(self._orchestrate())
        except* _Finished:
            pass
        await asyncio.to_thread(self.lease_mgr.release, self.lease)   # clean exit only
        return self.state

    async def _heartbeat(self):
        while True:
            await asyncio.sleep(self.lease_mgr.ttl / 3)
            self.lease = await asyncio.to_thread(self.lease_mgr.renew, self.lease)

    async def _poll(self):
        while True:
            msgs = await asyncio.to_thread(self.queue.receive)
            for m in msgs:
                await self.inbox.put(m)
            if not msgs:
                await asyncio.sleep(self.poll_interval)

    async def _publish(self):
        while True:
            ev = await self.outbox.get()
            try:
                self.hub.publish(self.sid, ev, self.lease.token)
            except StaleFence as e:
                raise LeaseLost(str(e)) from e
            self.outbox.task_done()

    async def _commit(self, state: dict, events=(), acks=()):
        self._ver = await asyncio.to_thread(self.store.save, self.lease, state, self._ver)
        for receipt in acks:                       # ack only AFTER durable checkpoint
            await asyncio.to_thread(self.queue.ack, receipt)
        for ev in events:
            await self.outbox.put(ev)

    async def _orchestrate(self):
        for ev in self.state["events"]:            # recovery: re-publish; sink dedupes by seq
            await self.outbox.put(ev)
        orch = Orchestrator(self.sid, self.planner, self.tools, self.inbox, self._commit,
                            self.tool_timeout)
        await orch.run(self.state)
        await self.outbox.join()
        raise _Finished
