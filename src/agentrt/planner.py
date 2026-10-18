"""Fake planner (stand-in for an LLM call).

Key idea: planning is non-deterministic, so the plan is CHECKPOINTED once and never
regenerated on replay. Swap `plan` for a real model call when you want to extend the lab.
"""
from __future__ import annotations

import asyncio


class FakePlanner:
    async def plan(self, goal: str) -> list[dict]:
        await asyncio.sleep(0.01)
        return [
            {"kind": "tool", "name": "lookup_order", "args": {"goal": goal}},
            {"kind": "ask_human", "question": f"Approve action for: {goal}?"},
            {"kind": "tool", "name": "issue_refund", "args": {"goal": goal}},
        ]
