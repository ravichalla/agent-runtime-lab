"""SQS FIFO wrapper, one FIFO queue per session (MessageGroupId = session id).

FIFO semantics worth knowing for the interview:
  * Ordering is per MessageGroupId; while a message in a group is in flight
    (received, not deleted) later messages in that group are NOT delivered.
  * Deduplication window is 5 minutes (MessageDeduplicationId).
  * Visibility timeout shorter than processing time => redelivery => consumers
    must be idempotent. We dedupe on message id inside the checkpoint.
  * Ack (delete) only AFTER the checkpoint that consumed the message is durable.

Queue-per-session keeps this lab simple. At millions of sessions you would use a
shared FIFO queue (group = session) plus a dispatcher that routes to the lease owner.
"""
from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Msg:
    msg_id: str      # stable id (our dedup id) -> idempotency across redelivery
    text: str
    receipt: str     # SQS receipt handle, used to ack


class SessionQueue:
    def __init__(self, sqs, session_id: str, visibility_timeout: int = 5, prefix: str = "agent"):
        self.sqs, self.session_id = sqs, session_id
        self.url = sqs.create_queue(
            QueueName=f"{prefix}-{session_id}.fifo",
            Attributes={"FifoQueue": "true", "VisibilityTimeout": str(visibility_timeout)},
        )["QueueUrl"]

    def send(self, text: str, dedup_id: str) -> None:
        self.sqs.send_message(
            QueueUrl=self.url,
            MessageBody=json.dumps({"id": dedup_id, "text": text}),
            MessageGroupId=self.session_id,
            MessageDeduplicationId=dedup_id,
        )

    def receive(self, max_messages: int = 5) -> list[Msg]:
        # WaitTimeSeconds=0 (short poll) keeps threads from lingering at shutdown;
        # use long polling (up to 20s) in production to cut empty receives.
        r = self.sqs.receive_message(QueueUrl=self.url, MaxNumberOfMessages=max_messages,
                                     WaitTimeSeconds=0)
        out = []
        for m in r.get("Messages", []):
            body = json.loads(m["Body"])
            out.append(Msg(body["id"], body["text"], m["ReceiptHandle"]))
        return out

    def ack(self, receipt: str) -> None:
        self.sqs.delete_message(QueueUrl=self.url, ReceiptHandle=receipt)
