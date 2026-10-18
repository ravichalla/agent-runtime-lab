"""Agent state machine. All durable progress lives in `state` (a JSON-able dict).

    NEW -> PLANNING -> EXECUTING <-> WAITING_HUMAN, then EXECUTING -> DONE

Rules that make replay safe:
  1. Every transition is followed by `commit` (checkpoint -> ack consumed messages
     -> enqueue events). Nothing is acked or published before it is durable.
  2. The plan is checkpointed, so replay never re-asks the LLM.
  3. Tool calls use idempotency key "<session>:<step>", so replaying a step whose
     result was not yet checkpointed cannot double the side effect.
  4. Events carry a per-session sequence number stored in the checkpoint, and the
     full event log is kept in state (a tiny transactional outbox) so a recovering
     worker can re-publish; the sink dedupes by seq.
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

NEW, PLANNING, EXECUTING, WAITING, DONE = "NEW", "PLANNING", "EXECUTING", "WAITING_HUMAN", "DONE"

Commit = Callable[..., Awaitable[None]]  # commit(state, events=(), acks=())


def new_state() -> dict:
    return {"phase": NEW, "goal": None, "plan": [], "step": 0, "results": [],
            "answers": [], "seq": 0, "seen": [], "events": []}


def emit(state: dict, kind: str, data: dict) -> dict:
    state["seq"] += 1
    ev = {"seq": state["seq"], "kind": kind, "data": data}
    state["events"].append(ev)
    return ev


class Orchestrator:
    def __init__(self, session_id: str, planner, tools, inbox: asyncio.Queue,
                 commit: Commit, tool_timeout: float = 5.0):
        self.sid, self.planner, self.tools = session_id, planner, tools
        self.inbox, self.commit, self.tool_timeout = inbox, commit, tool_timeout

    async def _next_new_message(self, state: dict):
        """Wait for a message we have not consumed yet; ack redeliveries of old ones."""
        while True:
            msg = await self.inbox.get()
            if msg.msg_id in state["seen"]:
                await self.commit(state, acks=[msg.receipt])   # redelivery after crash
                continue
            state["seen"].append(msg.msg_id)
            return msg

    async def run(self, state: dict) -> dict:
        while state["phase"] != DONE:
            phase = state["phase"]

            if phase == NEW:
                msg = await self._next_new_message(state)
                state["goal"], state["phase"] = msg.text, PLANNING
                await self.commit(state, acks=[msg.receipt])

            elif phase == PLANNING:
                state["plan"] = await self.planner.plan(state["goal"])
                state["phase"] = EXECUTING
                ev = emit(state, "plan", {"steps": len(state["plan"])})
                await self.commit(state, events=[ev])

            elif phase == EXECUTING:
                i = state["step"]
                if i >= len(state["plan"]):
                    state["phase"] = DONE
                    ev = emit(state, "final", {"results": state["results"], "answers": state["answers"]})
                    await self.commit(state, events=[ev])
                    continue
                step = state["plan"][i]
                if step["kind"] == "tool":
                    async with asyncio.timeout(self.tool_timeout):
                        res = await self.tools.call(step["name"], step["args"], f"{self.sid}:{i}")
                    state["results"].append(res)
                    state["step"] = i + 1
                    ev = emit(state, "tool_result", {"step": i, "tool": step["name"], "result": res})
                    await self.commit(state, events=[ev])
                else:  # ask_human: park the session until an answer arrives
                    state["phase"] = WAITING
                    ev = emit(state, "question", {"step": i, "text": step["question"]})
                    await self.commit(state, events=[ev])

            elif phase == WAITING:
                msg = await self._next_new_message(state)
                state["answers"].append(msg.text)
                state["step"] += 1
                state["phase"] = EXECUTING
                await self.commit(state, acks=[msg.receipt])
        return state
