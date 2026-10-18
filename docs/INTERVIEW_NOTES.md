# Interview notes

Only claim what you built. If you did this lab, say "I built a model of this to learn the failure
modes" and describe it accurately. Don't present it as production experience.

## 60-second pitch
"Agent sessions are long-running, stateful, and call slow, non-deterministic things. The runtime
needs single-writer ownership, durable progress, and replay that is safe. I use a DynamoDB lease with
a fencing token for ownership, checkpoint after every transition, ack messages only after the
checkpoint, and idempotency keys for tools, which gives effectively-once effects on at-least-once plumbing."

## Questions to rehearse (with where the answer lives)
1. Worker dies mid tool call? -> crash test; idempotency key; replay from checkpoint.
2. Two workers think they own a session? -> fencing token on every write; `test_zombie_worker...`.
3. Why not just rely on TTL? -> clocks/pauses; TTL is liveness only.
4. Heartbeat interval vs TTL? -> TTL/3; tradeoff of detection time vs write cost at scale.
5. Why SQS FIFO? Limits? -> per-group ordering, 5-min dedup, in-flight blocks the group, throughput caps.
6. Where does Kafka fit vs SQS? -> high-volume event streams/analytics fan-out vs per-session commands.
7. Slow client on the stream? -> bounded outbox backpressure; resume-from-seq on reconnect.
8. Human takes 3 days to answer? -> stretch goal 1 (release lease, wake on message).
9. What breaks first at 10M sessions? -> heartbeat write volume, hot items, queue-per-session, item size.
10. How do you test this? -> deterministic fakes, chaos cancel/kill, invariants (contiguous seq, one effect/step).

## Asyncio facts to know cold
- TaskGroup (3.11+): first failure cancels siblings, raises ExceptionGroup; use `except*`.
- Never swallow `CancelledError`; do cleanup in `finally`.
- Blocking calls stall every task on the loop; use `to_thread` / async clients.
- `asyncio.timeout()` for per-step deadlines; bounded `asyncio.Queue` for backpressure.
- One loop per process; scale out with processes; GIL irrelevant for I/O-bound work, relevant for CPU-bound parsing.

## Map to your real experience (use these as bridges, truthfully)
- Kafka consumers, offset commits, rebalances -> message pipeline and redelivery dedup.
- Elekta WebRTC real-time streaming -> resumable streaming, backpressure, latency.
- Go controllers / reconcile loops -> state machine that converges from checkpointed state.
- Centene batch-to-event-driven migration -> migrating with replay-safe semantics.
