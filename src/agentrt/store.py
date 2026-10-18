"""Checkpoint store. The checkpoint lives on the SAME item as the lease, so a single
conditional UpdateItem atomically checks ownership (fencing token) AND version.

Trade-off: DynamoDB items cap at 400 KB. For big transcripts, keep a pointer to S3
in the item and write the blob first (content-addressed), then flip the pointer here.
"""
from __future__ import annotations

import json

from botocore.exceptions import ClientError

from .lease import Lease, LeaseLost, cond_failed


class CheckpointStore:
    def __init__(self, ddb, table: str):
        self.ddb, self.table = ddb, table

    def load(self, session_id: str) -> tuple[dict, int]:
        r = self.ddb.get_item(TableName=self.table, Key={"session_id": {"S": session_id}},
                              ConsistentRead=True)
        item = r.get("Item")
        if not item or "ckpt_json" not in item:
            return {}, int(item["ckpt_version"]["N"]) if item else 0
        return json.loads(item["ckpt_json"]["S"]), int(item["ckpt_version"]["N"])

    def save(self, lease: Lease, state: dict, expected_version: int) -> int:
        try:
            self.ddb.update_item(
                TableName=self.table,
                Key={"session_id": {"S": lease.session_id}},
                UpdateExpression="SET ckpt_json = :s, ckpt_version = :nv",
                ConditionExpression=(
                    "owner_id = :me AND fencing_token = :tok AND ckpt_version = :v"
                ),
                ExpressionAttributeValues={
                    ":s": {"S": json.dumps(state)},
                    ":me": {"S": lease.owner_id}, ":tok": {"N": str(lease.token)},
                    ":v": {"N": str(expected_version)}, ":nv": {"N": str(expected_version + 1)},
                },
            )
        except ClientError as e:
            if cond_failed(e):
                raise LeaseLost(f"{lease.session_id}: checkpoint fenced out (token {lease.token})") from e
            raise
        return expected_version + 1
