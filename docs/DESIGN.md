# Design decisions

## Ownership: lease + fencing token
- One DynamoDB item per session. `acquire` = conditional update (`no owner OR expired`),
  atomically `ADD fencing_token 1`. `renew` requires owner AND token match AND not expired.
- **Safety comes from the token, liveness from the TTL.** Clock skew or a long GC pause can
  make two workers briefly *believe* they own a session; the token ensures only one can
  *write*. Every durable write (checkpoint) and every outward effect (stream publish) carries it.
- Heartbeat every TTL/3. Heartbeat failure raises, which cancels the entire session TaskGroup.
- Chosen over Redis/Redlock (weaker guarantees under partitions) and etcd/ZooKeeper (stronger,
  but another system to run). DynamoDB conditional writes are strongly consistent per item.

## Checkpointing
- Checkpoint shares the lease item, so ownership and version are verified in ONE conditional write.
- Limit: 400 KB/item. Production: blob to S3 (content-addressed), pointer in the item.
- Plan (LLM output) is checkpointed so replay never regenerates it (non-determinism).

## Delivery semantics: at-least-once + idempotency = effectively-once
- Crash window: tool succeeded, checkpoint not written. Replay re-calls with key `<session>:<step>`.
- Messages: ack to SQS only after the checkpoint that consumed them. Redelivery is deduped by
  message id stored in the checkpoint (`seen`).
- Events: sequence numbers live in the checkpoint; the event log is also in state (tiny outbox)
  so a recovering worker re-publishes everything; the sink drops seq <= last. Trim by acked seq in prod.

## Concurrency model
- Per session: heartbeat, poller, publisher, orchestrator in one `asyncio.TaskGroup`.
- Any failure cancels siblings -> `ExceptionGroup` (handle with `except*`). Clean finish uses a
  sentinel exception after `outbox.join()`. Lease is released only on clean finish (crash = expiry).
- Bounded queues (inbox/outbox) give backpressure: slow publisher stalls orchestrator, not memory.
- All blocking I/O via `asyncio.to_thread`; a blocked event loop starves the heartbeat and loses
  the lease. (aioboto3 is the alternative.)

## Known simplifications (say these out loud in an interview)
- Queue per session (does not scale to millions; use shared FIFO + dispatcher).
- In-process `StreamHub` stands in for the gRPC/Socket.IO fan-out tier and its own sharding.
- Workers hold a session while parked on a human (see stretch goal 1).
- Tool service and planner are fakes. No auth, tenancy, rate limits, or metrics.
- No Kafka runtime (needs a broker); the event log is shaped to be published to a keyed topic.
