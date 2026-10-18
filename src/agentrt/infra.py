"""Table provisioning helper (used by tests, demo, and DynamoDB Local)."""
from __future__ import annotations

SESSIONS_TABLE = "agent_sessions"


def create_tables(ddb, table: str = SESSIONS_TABLE) -> None:
    """One item per session: lease fields + fencing token + checkpoint snapshot."""
    ddb.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "session_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "session_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
