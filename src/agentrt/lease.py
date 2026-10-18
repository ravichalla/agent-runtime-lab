"""Lease-based session ownership using DynamoDB conditional writes.

Item shape (one per session):
    session_id     S   partition key
    owner_id       S   worker currently holding the lease
    lease_expiry   N   epoch seconds; lease is takeable once expiry < now
    fencing_token  N   incremented on every successful acquire (monotonic)
    ckpt_version   N   optimistic-concurrency counter for the checkpoint
    ckpt_json      S   JSON checkpoint (see store.py)

Correctness rule: *liveness* depends on clocks (TTL), *safety* depends only on the
fencing token. A paused/partitioned worker that wakes up after losing its lease
is rejected by every conditional write that includes its (now stale) token.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import Callable

from botocore.exceptions import ClientError


class LeaseHeld(Exception):
    """Someone else owns a live lease on this session."""


class LeaseLost(Exception):
    """We no longer own the session (expired, taken over, or fenced out)."""


@dataclass(frozen=True)
class Lease:
    session_id: str
    owner_id: str
    token: int
    expires_at: float


def cond_failed(e: ClientError) -> bool:
    return e.response["Error"]["Code"] == "ConditionalCheckFailedException"


def num(x: float) -> dict:
    return {"N": f"{x:.6f}"}


class LeaseManager:
    def __init__(self, ddb, table: str, owner_id: str, ttl: float = 3.0,
                 clock: Callable[[], float] = time.time):
        self.ddb, self.table, self.owner_id, self.ttl, self.clock = ddb, table, owner_id, ttl, clock

    def acquire(self, session_id: str) -> Lease:
        now = self.clock()
        try:
            r = self.ddb.update_item(
                TableName=self.table,
                Key={"session_id": {"S": session_id}},
                UpdateExpression=(
                    "SET owner_id = :me, lease_expiry = :exp, "
                    "ckpt_version = if_not_exists(ckpt_version, :zero) "
                    "ADD fencing_token :one"
                ),
                ConditionExpression="attribute_not_exists(owner_id) OR lease_expiry < :now",
                ExpressionAttributeValues={
                    ":me": {"S": self.owner_id}, ":exp": num(now + self.ttl), ":now": num(now),
                    ":zero": {"N": "0"}, ":one": {"N": "1"},
                },
                ReturnValues="UPDATED_NEW",
            )
        except ClientError as e:
            if cond_failed(e):
                raise LeaseHeld(session_id) from e
            raise
        token = int(r["Attributes"]["fencing_token"]["N"])
        return Lease(session_id, self.owner_id, token, now + self.ttl)

    def renew(self, lease: Lease) -> Lease:
        """Heartbeat. Fails if the lease already expired, even if nobody took it (conservative)."""
        now = self.clock()
        try:
            self.ddb.update_item(
                TableName=self.table,
                Key={"session_id": {"S": lease.session_id}},
                UpdateExpression="SET lease_expiry = :exp",
                ConditionExpression=(
                    "owner_id = :me AND fencing_token = :tok AND lease_expiry > :now"
                ),
                ExpressionAttributeValues={
                    ":me": {"S": lease.owner_id}, ":tok": {"N": str(lease.token)},
                    ":exp": num(now + self.ttl), ":now": num(now),
                },
            )
        except ClientError as e:
            if cond_failed(e):
                raise LeaseLost(f"{lease.session_id}: renew rejected (token {lease.token})") from e
            raise
        return replace(lease, expires_at=now + self.ttl)

    def release(self, lease: Lease) -> None:
        """Graceful handoff. Best-effort: if we've already lost it, that's fine."""
        try:
            self.ddb.update_item(
                TableName=self.table,
                Key={"session_id": {"S": lease.session_id}},
                UpdateExpression="SET lease_expiry = :zero",
                ConditionExpression="owner_id = :me AND fencing_token = :tok",
                ExpressionAttributeValues={
                    ":me": {"S": lease.owner_id}, ":tok": {"N": str(lease.token)},
                    ":zero": {"N": "0"},
                },
            )
        except ClientError as e:
            if not cond_failed(e):
                raise
