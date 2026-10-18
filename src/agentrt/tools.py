"""Stand-in for external tool APIs, with idempotency keys.

Crash recovery is at-least-once: a worker can die AFTER a tool call succeeded but
BEFORE the checkpoint recorded it. Replay re-issues the call; the idempotency key
makes the external side effect happen once.
"""
from __future__ import annotations

import asyncio
from collections import Counter


class SimulatedCrash(Exception):
    """Raised by the fake service to emulate the worker dying at an awkward moment."""


class FakeToolService:
    def __init__(self):
        self.effects: dict[str, dict] = {}        # idem_key -> result (the real side effect)
        self.calls: Counter[str] = Counter()      # idem_key -> number of attempts
        self.crash_after_effect: set[str] = set()
        self.block_forever: set[str] = set()

    async def call(self, name: str, args: dict, idem_key: str) -> dict:
        self.calls[idem_key] += 1
        if idem_key in self.effects:               # replay: return the recorded result
            return self.effects[idem_key]
        if idem_key in self.block_forever:
            await asyncio.Event().wait()
        await asyncio.sleep(0.01)                  # pretend network latency
        result = {"tool": name, "ok": True, "ref": f"{name}-{len(self.effects) + 1}"}
        self.effects[idem_key] = result
        if idem_key in self.crash_after_effect:
            self.crash_after_effect.discard(idem_key)
            raise SimulatedCrash(idem_key)
        return result
