"""End-to-end demo on mocked AWS: crash mid-tool-call, recover on another worker, finish.

    pip install -e '.[dev]' && python scripts/demo.py
"""
import asyncio
import os

os.environ.update(AWS_ACCESS_KEY_ID="x", AWS_SECRET_ACCESS_KEY="x", AWS_DEFAULT_REGION="us-east-1")

import boto3
from moto import mock_aws

from agentrt.bus import SessionQueue
from agentrt.infra import SESSIONS_TABLE, create_tables
from agentrt.lease import LeaseManager
from agentrt.planner import FakePlanner
from agentrt.session import SessionRunner
from agentrt.store import CheckpointStore
from agentrt.stream import StreamHub
from agentrt.tools import FakeToolService, SimulatedCrash


async def main():
    ddb, sqs = boto3.client("dynamodb"), boto3.client("sqs")
    create_tables(ddb)
    hub, tools, store = StreamHub(), FakeToolService(), CheckpointStore(ddb, SESSIONS_TABLE)
    tools.crash_after_effect.add("demo:0")

    def runner(owner):
        return SessionRunner("demo", lease_mgr=LeaseManager(ddb, SESSIONS_TABLE, owner, ttl=0.6),
                             store=store, queue=SessionQueue(sqs, "demo", visibility_timeout=1),
                             tools=tools, planner=FakePlanner(), hub=hub)

    a = runner("worker-A")
    a.queue.send("refund order 42", "m1")
    try:
        await a.run()
    except* SimulatedCrash:
        print("worker-A crashed after the tool call succeeded but before checkpointing")

    await asyncio.sleep(0.7)                       # let A's lease expire
    b = runner("worker-B")
    task = asyncio.create_task(b.run())
    while not any(e["kind"] == "question" for e in hub.events["demo"]):
        await asyncio.sleep(0.02)
    print("agent is waiting on a human; sending approval")
    b.queue.send("approved", "m2")
    await task

    for e in hub.events["demo"]:
        print(f"  seq={e['seq']} {e['kind']}")
    print("tool attempts:", dict(tools.calls), "| side effects:", len(tools.effects))


with mock_aws():
    asyncio.run(main())
