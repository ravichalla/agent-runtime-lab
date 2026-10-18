import asyncio

import pytest

from agentrt.lease import LeaseHeld, LeaseLost


def test_exclusive_ownership(env):
    a, b = env.leases("A"), env.leases("B")
    lease = a.acquire("s1")
    assert lease.token == 1
    with pytest.raises(LeaseHeld):
        b.acquire("s1")


async def test_expiry_takeover_bumps_fencing_token(env):
    a, b = env.leases("A", ttl=0.3), env.leases("B", ttl=0.3)
    la = a.acquire("s1")
    await asyncio.sleep(0.4)
    lb = b.acquire("s1")
    assert lb.token == la.token + 1


async def test_renew_keeps_lease_alive(env):
    a, b = env.leases("A", ttl=0.5), env.leases("B", ttl=0.5)
    la = a.acquire("s1")
    for _ in range(4):
        await asyncio.sleep(0.2)
        la = a.renew(la)
    with pytest.raises(LeaseHeld):
        b.acquire("s1")


async def test_zombie_worker_is_fenced_everywhere(env):
    """A stalls (GC pause / blocked loop), B takes over, A wakes up and tries to act."""
    a, b = env.leases("A", ttl=0.3), env.leases("B", ttl=0.3)
    la = a.acquire("s1")
    v = env.store.save(la, {"who": "A"}, 0)
    await asyncio.sleep(0.4)            # A's lease expires while it is "paused"
    lb = b.acquire("s1")

    with pytest.raises(LeaseLost):
        a.renew(la)                     # heartbeat rejected
    with pytest.raises(LeaseLost):
        env.store.save(la, {"who": "A-zombie"}, v)   # checkpoint rejected by fencing token

    v2 = env.store.save(lb, {"who": "B"}, v)         # new owner proceeds
    assert env.store.load("s1") == ({"who": "B"}, v2)


def test_checkpoint_version_conflict_detected(env):
    a = env.leases("A")
    la = a.acquire("s1")
    env.store.save(la, {"n": 1}, 0)
    with pytest.raises(LeaseLost):
        env.store.save(la, {"n": 2}, 0)   # stale expected_version


def test_graceful_release_allows_immediate_takeover(env):
    a, b = env.leases("A"), env.leases("B")
    la = a.acquire("s1")
    a.release(la)
    assert b.acquire("s1").token == 2
