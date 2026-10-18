import asyncio

import pytest

from agentrt.lease import LeaseHeld, LeaseLost
from agentrt.orchestrator import Orchestrator, new_state
from agentrt.bus import Msg
from agentrt.tools import SimulatedCrash


async def wait_for(pred, timeout=5.0):
    async def _w():
        while not pred():
            await asyncio.sleep(0.02)
    await asyncio.wait_for(_w(), timeout)


def kinds(env, sid):
    return [e["kind"] for e in env.hub.events[sid]]


def seqs(env, sid):
    return [e["seq"] for e in env.hub.events[sid]]


async def test_happy_path_with_human_in_the_loop(env):
    r = env.runner("s1", "A")
    q = r.queue
    task = asyncio.create_task(r.run())
    q.send("refund order 42", "m1")
    await wait_for(lambda: "question" in kinds(env, "s1"))      # parked, waiting on a human
    assert not task.done()
    q.send("approved", "m2")
    state = await asyncio.wait_for(task, 10)

    assert kinds(env, "s1") == ["plan", "tool_result", "question", "tool_result", "final"]
    assert seqs(env, "s1") == [1, 2, 3, 4, 5]
    assert state["answers"] == ["approved"] and len(env.tools.effects) == 2
    # lease released on clean exit -> another worker can take over immediately
    assert env.leases("B").acquire("s1").token == 2


async def test_crash_after_tool_effect_recovers_without_double_side_effect(env):
    env.tools.crash_after_effect.add("s2:0")          # tool succeeds, worker dies before checkpoint
    a = env.runner("s2", "A")
    a.queue.send("refund order 7", "m1")
    with pytest.raises(ExceptionGroup) as ei:
        await asyncio.wait_for(a.run(), 10)
    assert ei.value.subgroup(SimulatedCrash)

    # B cannot take over until A's lease expires (A crashed: no release)
    b = env.runner("s2", "B")
    with pytest.raises(LeaseHeld):
        await b.run()
    await asyncio.sleep(0.7)

    task = asyncio.create_task(b.run())
    await wait_for(lambda: "question" in kinds(env, "s2"))
    b.queue.send("approved", "m2")
    await asyncio.wait_for(task, 10)

    assert env.tools.calls["s2:0"] == 2               # attempted twice (replay)...
    assert len([k for k in env.tools.effects if k.startswith("s2:")]) == 2   # ...effect once per step
    assert seqs(env, "s2") == [1, 2, 3, 4, 5]         # no gaps, no duplicates
    assert kinds(env, "s2")[-1] == "final"


async def test_worker_dies_while_waiting_on_human_and_resumes_elsewhere(env):
    a = env.runner("s3", "A")
    a.queue.send("refund order 9", "m1")
    ta = asyncio.create_task(a.run())
    await wait_for(lambda: "question" in kinds(env, "s3"))
    ta.cancel()                                        # hard kill: no release, no cleanup of lease
    with pytest.raises(asyncio.CancelledError):
        await ta
    await asyncio.sleep(0.7)                           # lease expires

    b = env.runner("s3", "B")
    tb = asyncio.create_task(b.run())
    b.queue.send("approved", "m2")
    state = await asyncio.wait_for(tb, 10)
    assert state["phase"] == "DONE" and state["plan"] and len(state["results"]) == 2
    assert seqs(env, "s3") == [1, 2, 3, 4, 5]


async def test_losing_the_lease_tears_down_the_whole_session(env):
    """Simulate a takeover while A is alive but stalled: heartbeat fails -> group cancelled."""
    a = env.runner("s4", "A")
    ta = asyncio.create_task(a.run())
    await wait_for(lambda: a.lease is not None)
    # Another worker forcibly takes the item (as if A's lease had expired during a stall).
    env.ddb.update_item(
        TableName="agent_sessions", Key={"session_id": {"S": "s4"}},
        UpdateExpression="SET owner_id = :o ADD fencing_token :one",
        ExpressionAttributeValues={":o": {"S": "B"}, ":one": {"N": "1"}},
    )
    with pytest.raises(ExceptionGroup) as ei:
        await asyncio.wait_for(ta, 5)
    assert ei.value.subgroup(LeaseLost)


async def test_stale_publisher_is_rejected_by_stream_fencing(env):
    env.hub.publish("s5", {"seq": 1, "kind": "plan", "data": {}}, token=2)
    from agentrt.stream import StaleFence
    with pytest.raises(StaleFence):
        env.hub.publish("s5", {"seq": 2, "kind": "x", "data": {}}, token=1)


async def test_orchestrator_dedupes_redelivered_messages(env):
    """SQS may redeliver (visibility timeout < processing). Seen ids are ack'd, not re-processed."""
    inbox: asyncio.Queue = asyncio.Queue()
    acks, commits = [], []

    async def commit(state, events=(), acks=()):
        commits.append(len(state["events"]))
        acks_all.extend(acks)

    acks_all = acks
    state = new_state()
    state["seen"] = ["m1"]                       # m1 was consumed in a previous incarnation
    for m in (Msg("m1", "old goal", "r1"), Msg("m2", "new goal", "r2")):
        inbox.put_nowait(m)
    orch = Orchestrator("s6", env.planner, env.tools, inbox, commit)
    task = asyncio.create_task(orch.run(state))
    await wait_for(lambda: state["goal"] is not None)
    task.cancel()
    assert state["goal"] == "new goal" and acks == ["r1", "r2"]
