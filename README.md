# agent-runtime-lab

A small, fully tested model of the runtime behind long-running AI agent sessions:
lease-based ownership, crash recovery via checkpoints, ordered messaging, structured
concurrency, and resumable streaming. No LLM or real AWS needed (moto mocks DynamoDB/SQS).

```bash
pip install -e '.[dev]'
pytest -q                  # 14 tests incl. crash / zombie-worker scenarios
python scripts/demo.py     # crash mid-tool-call -> recover on another worker
```

## Map: interview topic -> code -> test

| Topic | Code | Proving test |
|---|---|---|
| DynamoDB lease with conditional writes, fencing tokens | `lease.py`, `store.py` | `test_lease.py::test_zombie_worker_is_fenced_everywhere` |
| Heartbeat + crash recovery via checkpoint | `session.py`, `orchestrator.py` | `test_crash_after_tool_effect_recovers_...` |
| asyncio TaskGroup, 4 tasks/session, cancellation on failure | `session.py` | `test_losing_the_lease_tears_down_the_whole_session` |
| Agent state machine, human-in-the-loop wait/resume | `orchestrator.py` | `test_happy_path_...`, `test_worker_dies_while_waiting_...` |
| SQS FIFO ordering, ack-after-checkpoint, redelivery dedup | `bus.py` | `test_orchestrator_dedupes_redelivered_messages` |
| Idempotent tool calls (effectively-once) | `tools.py` | crash test asserts 2 attempts, 1 effect |
| Streaming with seq numbers, fencing, resume-from-offset | `stream.py` | `test_stream.py` |

Read `docs/DESIGN.md` (decisions and trade-offs) and `docs/INTERVIEW_NOTES.md` (talking points,
follow-up questions, honest gaps) before the interview.

## Stretch goals (do these to go beyond the lab)
1. **Idle release:** when parked in WAITING_HUMAN, release the lease and stop the tasks; a wake-up
   message re-acquires and resumes. (Workers should not hold memory for sessions waiting hours.)
2. **Shared FIFO queue + dispatcher** instead of queue-per-session; route by lease owner.
3. **Kafka:** publish the event log to a topic keyed by session id; write a consumer that
   handles rebalances and commits offsets after processing (need a broker; use Docker).
4. **Real DynamoDB Local** (Docker) and a latency benchmark of heartbeat cost at 10k sessions.
5. **Real LLM planner** behind `FakePlanner`'s interface, with per-step timeouts and retries.
6. **gRPC/Socket.IO** frontend over `StreamHub.subscribe` with resume-from-seq.
7. **Chaos harness:** randomly cancel workers during a soak test; assert invariants
   (contiguous seqs, one effect per step, exactly one final event).
8. **Blocked-loop test:** run a sync `time.sleep` inside a tool to show lease loss, then fix it with `to_thread`.
