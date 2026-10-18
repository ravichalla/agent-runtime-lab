import os

import boto3
import pytest
from moto import mock_aws

from agentrt.bus import SessionQueue
from agentrt.infra import SESSIONS_TABLE, create_tables
from agentrt.lease import LeaseManager
from agentrt.planner import FakePlanner
from agentrt.session import SessionRunner
from agentrt.store import CheckpointStore
from agentrt.stream import StreamHub
from agentrt.tools import FakeToolService

TTL = 0.6


@pytest.fixture(autouse=True)
def _fake_aws_env(monkeypatch):
    for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(k, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def aws():
    with mock_aws():
        ddb = boto3.client("dynamodb", region_name="us-east-1")
        sqs = boto3.client("sqs", region_name="us-east-1")
        create_tables(ddb)
        yield ddb, sqs


class Env:
    """Wires a fleet of 'workers' sharing DynamoDB, SQS, the stream hub and the tool service."""

    def __init__(self, ddb, sqs):
        self.ddb, self.sqs = ddb, sqs
        self.hub, self.tools, self.planner = StreamHub(), FakeToolService(), FakePlanner()
        self.store = CheckpointStore(ddb, SESSIONS_TABLE)

    def leases(self, owner: str, ttl: float = TTL) -> LeaseManager:
        return LeaseManager(self.ddb, SESSIONS_TABLE, owner, ttl=ttl)

    def queue(self, sid: str) -> SessionQueue:
        return SessionQueue(self.sqs, sid, visibility_timeout=1)

    def runner(self, sid: str, owner: str) -> SessionRunner:
        return SessionRunner(sid, lease_mgr=self.leases(owner), store=self.store,
                             queue=self.queue(sid), tools=self.tools, planner=self.planner,
                             hub=self.hub)


@pytest.fixture
def env(aws):
    return Env(*aws)
