import asyncio
import json

from agentrt.stream import StreamHub, serve


def ev(seq, kind="tool_result"):
    return {"seq": seq, "kind": kind, "data": {}}


def test_publish_is_idempotent_on_seq():
    hub = StreamHub()
    assert hub.publish("s", ev(1), 1) is True
    assert hub.publish("s", ev(1), 1) is False      # replay after crash
    assert [e["seq"] for e in hub.events["s"]] == [1]


async def read_n(reader, n):
    return [json.loads(await asyncio.wait_for(reader.readline(), 3)) for _ in range(n)]


async def test_reconnect_resumes_from_sequence_without_gaps_or_dupes():
    hub = StreamHub()
    server = await serve(hub)
    port = server.sockets[0].getsockname()[1]

    for i in (1, 2, 3):
        hub.publish("s", ev(i), 1)

    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(json.dumps({"session_id": "s", "from_seq": 1}).encode() + b"\n")
    await w.drain()
    first = await read_n(r, 2)
    w.close()                                      # client drops after seeing seq 1..2
    assert [e["seq"] for e in first] == [1, 2]

    hub.publish("s", ev(4), 1)                     # events keep flowing while disconnected
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(json.dumps({"session_id": "s", "from_seq": 3}).encode() + b"\n")
    await w.drain()
    hub.publish("s", ev(5, "final"), 1)            # and live events after reconnect
    rest = await read_n(r, 3)
    assert [e["seq"] for e in rest] == [3, 4, 5]
    w.close()
    server.close()
    await server.wait_closed()
