"""Tests for nadi_kit — shared NADI federation transport."""

import json
import time
from pathlib import Path

from nadi_kit import NadiMessage, NadiNode, NadiTransport


def test_message_roundtrip():
    msg = NadiMessage(source="a", target="b", operation="heartbeat", payload={"x": 1})
    d = msg.to_dict()
    restored = NadiMessage.from_dict(d)
    assert restored.source == "a"
    assert restored.target == "b"
    assert restored.operation == "heartbeat"
    assert restored.payload == {"x": 1}


def test_message_expiry():
    fresh = NadiMessage(source="a", target="b", operation="x", payload={})
    assert not fresh.is_expired

    expired = NadiMessage(source="a", target="b", operation="x", payload={}, timestamp=1.0, ttl_s=1.0)
    assert expired.is_expired


def test_message_from_dict_extra_fields():
    d = {"source": "a", "target": "b", "operation": "x", "payload": {}, "extra_field": True}
    msg = NadiMessage.from_dict(d)
    assert msg.source == "a"


def test_message_from_dict_missing_fields():
    msg = NadiMessage.from_dict({})
    assert msg.source == "unknown"
    assert msg.operation == "unknown"


def test_transport_read_write(tmp_path: Path):
    t = NadiTransport(tmp_path)
    assert t.stats() == {"inbox": 0, "outbox": 0}

    msgs = [NadiMessage(source="a", target="b", operation="test", payload={"n": i}) for i in range(3)]
    added = t.append_to_outbox(msgs)
    assert added == 3
    assert t.stats()["outbox"] == 3

    read = t.read_outbox()
    assert len(read) == 3
    assert read[0].payload == {"n": 0}


def test_transport_dedup(tmp_path: Path):
    t = NadiTransport(tmp_path)
    msg = NadiMessage(source="a", target="b", operation="test", payload={}, timestamp=100.0, ttl_s=999999)
    t.append_to_outbox([msg])
    t.append_to_outbox([msg])  # duplicate
    assert t.stats()["outbox"] == 1


def test_transport_buffer_cap(tmp_path: Path):
    t = NadiTransport(tmp_path)
    msgs = [
        NadiMessage(source="a", target="b", operation="test", payload={"n": i})
        for i in range(200)
    ]
    t.append_to_outbox(msgs)
    assert t.stats()["outbox"] == 144  # NADI_BUFFER_SIZE


def test_transport_clear_expired(tmp_path: Path):
    t = NadiTransport(tmp_path)
    fresh = NadiMessage(source="a", target="b", operation="test", payload={})
    stale = NadiMessage(source="x", target="y", operation="old", payload={}, timestamp=1.0, ttl_s=1.0)
    # Write both (bypass expiry filter by writing raw)
    t._atomic_write(t.inbox_path, [fresh.to_dict(), stale.to_dict()])
    assert t.stats()["inbox"] == 2
    result = t.clear_expired()
    assert result["inbox"] == 1
    assert t.stats()["inbox"] == 1


def test_node_from_peer_json(tmp_path: Path):
    fed_dir = tmp_path / "data" / "federation"
    fed_dir.mkdir(parents=True)
    peer = {
        "identity": {"city_id": "test-node", "repo": "kimeisele/test-node"},
        "capabilities": ["testing"],
    }
    peer_path = fed_dir / "peer.json"
    peer_path.write_text(json.dumps(peer))
    (fed_dir / "nadi_inbox.json").write_text("[]")
    (fed_dir / "nadi_outbox.json").write_text("[]")

    node = NadiNode.from_peer_json(peer_path)
    assert node.agent_id == "test-node"
    assert node.repo == "kimeisele/test-node"
    assert node.capabilities == ["testing"]


def test_node_emit_and_receive(tmp_path: Path):
    fed_dir = tmp_path
    (fed_dir / "nadi_inbox.json").write_text("[]")
    (fed_dir / "nadi_outbox.json").write_text("[]")

    node = NadiNode("sender", fed_dir, repo="kimeisele/sender")
    node.set_peers(["peer-a", "peer-b"])

    msgs = node.emit("status", {"ok": True}, target="peer-a")
    assert len(msgs) == 1
    assert msgs[0].target == "peer-a"
    assert node.transport.stats()["outbox"] == 1


def test_node_broadcast(tmp_path: Path):
    fed_dir = tmp_path
    (fed_dir / "nadi_inbox.json").write_text("[]")
    (fed_dir / "nadi_outbox.json").write_text("[]")

    node = NadiNode("sender", fed_dir, repo="kimeisele/sender")
    node.set_peers(["a", "b", "c"])

    msgs = node.emit("ping", {})
    assert len(msgs) == 3
    assert {m.target for m in msgs} == {"a", "b", "c"}


def test_node_heartbeat(tmp_path: Path):
    fed_dir = tmp_path
    (fed_dir / "nadi_inbox.json").write_text("[]")
    (fed_dir / "nadi_outbox.json").write_text("[]")

    node = NadiNode("me", fed_dir, repo="kimeisele/me", capabilities=["a", "b"])
    node.set_peers(["them"])

    msgs = node.heartbeat(health=0.9)
    assert len(msgs) == 1
    assert msgs[0].operation == "heartbeat"
    assert msgs[0].payload["agent_id"] == "me"
    assert msgs[0].payload["health"] == 0.9
    assert msgs[0].payload["capabilities"] == ["a", "b"]


def test_node_process_inbox(tmp_path: Path):
    fed_dir = tmp_path
    (fed_dir / "nadi_outbox.json").write_text("[]")

    # Pre-fill inbox
    inbox_msg = NadiMessage(source="peer", target="me", operation="ping", payload={"v": 42})
    (fed_dir / "nadi_inbox.json").write_text(json.dumps([inbox_msg.to_dict()]))

    received = []
    node = NadiNode("me", fed_dir)
    node.on("ping", lambda msg: received.append(msg.payload))

    count = node.process_inbox()
    assert count == 1
    assert received == [{"v": 42}]

    # Second call should not re-process
    count2 = node.process_inbox()
    assert count2 == 0


def test_node_load_peers_from_seeds(tmp_path: Path):
    fed_dir = tmp_path
    (fed_dir / "nadi_inbox.json").write_text("[]")
    (fed_dir / "nadi_outbox.json").write_text("[]")

    seeds = [
        "https://raw.githubusercontent.com/kimeisele/agent-city/main/.well-known/agent-federation.json",
        "https://raw.githubusercontent.com/kimeisele/steward/main/.well-known/agent-federation.json",
    ]
    (fed_dir / "authority-descriptor-seeds.json").write_text(json.dumps(seeds))

    node = NadiNode("agent-city", fed_dir)
    peers = node.load_peers_from_seeds()
    assert "steward" in peers
    assert "agent-city" not in peers  # self excluded
