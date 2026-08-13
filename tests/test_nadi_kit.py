"""Tests for nadi_kit — shared NADI federation transport."""

import json
from pathlib import Path

import pytest

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
    msg = NadiMessage(source="a", target="b", operation="test", payload={})
    t.append_to_outbox([msg])
    t.append_to_outbox([msg])  # duplicate
    assert t.stats()["outbox"] == 1


def test_transport_buffer_cap(tmp_path: Path):
    t = NadiTransport(tmp_path)
    msgs = [
        NadiMessage(source="a", target="b", operation="test", payload={"n": i})
        for i in range(200)
    ]
    with pytest.raises(BufferError, match="refusing lossy append"):
        t.append_to_outbox(msgs)
    assert t.stats()["outbox"] == 0


def test_transport_overflow_preserves_existing_messages(tmp_path: Path):
    t = NadiTransport(tmp_path)
    existing = [NadiMessage(source="a", target="b", operation="x", payload={"n": i}) for i in range(143)]
    assert t.append_to_outbox(existing) == 143
    additions = [NadiMessage(source="a", target="b", operation="x", payload={"n": i}) for i in range(2)]
    with pytest.raises(BufferError):
        t.append_to_outbox(additions)
    assert [message.id for message in t.read_outbox()] == [message.id for message in existing]


def test_transport_deduplicates_within_one_append(tmp_path: Path):
    t = NadiTransport(tmp_path)
    message = NadiMessage(source="a", target="b", operation="x", payload={})
    assert t.append_to_outbox([message, message]) == 1
    assert t.stats()["outbox"] == 1


def test_inbox_accepts_complete_pull_larger_than_outbox_capacity(tmp_path: Path):
    transport = NadiTransport(tmp_path)
    messages = [
        NadiMessage(source="peer", target="me", operation="x", payload={"n": index})
        for index in range(200)
    ]
    assert transport.append_to_inbox(messages) == 200
    assert len(transport.read_inbox()) == 200


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


def test_emit_signs_every_message_and_uses_node_id_as_source(tmp_path: Path):
    """Regression for the heartbeat-bleeding incident.

    Until this fix, NadiMessage carried no signature and source was the
    agent_name string — so steward's PROTECTED-op gateway rejected every
    inbound heartbeat (10k+ drops/day across the federation).
    """
    import base64
    import hashlib

    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    fed_dir = tmp_path
    (fed_dir / "nadi_inbox.json").write_text("[]")
    (fed_dir / "nadi_outbox.json").write_text("[]")

    node = NadiNode("sender", fed_dir)
    node.set_peers(["peer-a"])

    msgs = node.emit("status", {"ok": True}, target="peer-a")
    msg = msgs[0]

    # source MUST be the cryptographic node_id, not the agent_name
    assert msg.source == node.node_id
    assert msg.source != "sender"

    # payload_hash + signature MUST be populated
    assert msg.payload_hash, "payload_hash empty — gateway will reject"
    assert msg.signature, "signature empty — gateway will reject"

    # payload_hash recomputes deterministically from canonical JSON
    canonical = {k: v for k, v in msg.to_dict().items()
                 if k not in ("payload_hash", "signature")}
    expected_hash = hashlib.sha256(
        json.dumps(canonical, sort_keys=True).encode()
    ).hexdigest()
    assert msg.payload_hash == expected_hash

    # signature verifies via the same primitive steward uses
    pk = Ed25519PublicKey.from_public_bytes(bytes.fromhex(node.public_key))
    pk.verify(base64.b64decode(msg.signature.encode()), msg.payload_hash.encode())


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


def test_same_source_and_timestamp_with_distinct_ids_both_dispatch(tmp_path: Path):
    t = 1234.0
    first = NadiMessage(source="peer", target="me", operation="ping", payload={"n": 1}, timestamp=t, ttl_s=9999999999)
    second = NadiMessage(source="peer", target="me", operation="ping", payload={"n": 2}, timestamp=t, ttl_s=9999999999)
    (tmp_path / "nadi_inbox.json").write_text(json.dumps([first.to_dict(), second.to_dict()]))
    (tmp_path / "nadi_outbox.json").write_text("[]")
    received = []
    node = NadiNode("me", tmp_path)
    node.on("ping", lambda message: received.append(message.payload["n"]))
    assert node.process_inbox() == 2
    assert received == [1, 2]


def test_duplicate_id_in_one_inbox_dispatches_once(tmp_path: Path):
    message = NadiMessage(source="peer", target="me", operation="ping", payload={})
    (tmp_path / "nadi_inbox.json").write_text(json.dumps([message.to_dict(), message.to_dict()]))
    (tmp_path / "nadi_outbox.json").write_text("[]")
    calls = []
    node = NadiNode("me", tmp_path)
    node.on("ping", lambda received: calls.append(received.id))
    assert node.process_inbox() == 1
    assert calls == [message.id]


def test_failed_handler_is_retryable(tmp_path: Path):
    message = NadiMessage(source="peer", target="me", operation="ping", payload={})
    (tmp_path / "nadi_inbox.json").write_text(json.dumps([message.to_dict()]))
    (tmp_path / "nadi_outbox.json").write_text("[]")
    node = NadiNode("me", tmp_path)
    node.on("ping", lambda _message: (_ for _ in ()).throw(RuntimeError("retry me")))
    assert node.process_inbox() == 0
    calls = []
    node.on("ping", lambda received: calls.append(received.id))
    assert node.process_inbox() == 1
    assert calls == [message.id]


def test_legacy_message_without_id_has_stable_identity():
    raw = {"source": "peer", "target": "me", "operation": "ping", "payload": {}, "timestamp": 1234.0}
    assert NadiMessage.from_dict(raw).id == NadiMessage.from_dict(raw).id


def test_processed_trim_keeps_exact_newest_entries(tmp_path: Path):
    (tmp_path / "nadi_inbox.json").write_text("[]")
    (tmp_path / "nadi_outbox.json").write_text("[]")
    node = NadiNode("me", tmp_path)
    node._processed.update((("peer", str(index)), None) for index in range(5000))
    for index in range(5000, 5001):
        message = NadiMessage(source="peer", target="me", operation="noop", payload={}, id=str(index))
        node.transport._atomic_write(node.transport.inbox_path, [message.to_dict()])
        node.process_inbox()
    assert list(node._processed) == [("peer", str(index)) for index in range(2501, 5001)]


def test_partial_relay_failure_keeps_only_failed_target_in_outbox(tmp_path: Path, monkeypatch):
    (tmp_path / "nadi_inbox.json").write_text("[]")
    (tmp_path / "nadi_outbox.json").write_text("[]")
    node = NadiNode("me", tmp_path)
    to_a = NadiMessage(source="me", target="a", operation="work", payload={})
    to_b = NadiMessage(source="me", target="b", operation="work", payload={})
    node.transport.append_to_outbox([to_a, to_b])
    monkeypatch.setattr(node.relay, "_read_hub_file_with_sha", lambda _path: ([], None))

    def write(path, _data, *, sha=None):
        if "_to_b.json" in path:
            raise RuntimeError("target b unavailable")

    monkeypatch.setattr(node.relay, "_write_hub_file", write)
    result = node.sync()
    assert result["pushed"] == 1
    assert [message.id for message in node.transport.read_outbox()] == [to_b.id]


def test_existing_hub_message_acknowledges_local_duplicate(tmp_path: Path, monkeypatch):
    (tmp_path / "nadi_inbox.json").write_text("[]")
    (tmp_path / "nadi_outbox.json").write_text("[]")
    node = NadiNode("me", tmp_path)
    message = NadiMessage(source="me", target="a", operation="work", payload={})
    node.transport.append_to_outbox([message])
    monkeypatch.setattr(
        node.relay,
        "_read_hub_file_with_sha",
        lambda _path: ([message.to_dict()], "sha"),
    )
    monkeypatch.setattr(
        node.relay,
        "_write_hub_file",
        lambda *_args, **_kwargs: pytest.fail("duplicate must not rewrite mailbox"),
    )
    result = node.sync()
    assert result["pushed"] == 0
    assert node.transport.read_outbox() == []


def test_full_hub_mailbox_evicts_old_existing_and_reports_it(tmp_path: Path, monkeypatch):
    (tmp_path / "nadi_inbox.json").write_text("[]")
    (tmp_path / "nadi_outbox.json").write_text("[]")
    node = NadiNode("me", tmp_path)
    existing = [NadiMessage(source="me", target="a", operation="old", payload={"n": index}) for index in range(144)]
    message = NadiMessage(source="me", target="a", operation="new", payload={})
    written = []
    monkeypatch.setattr(node.relay, "_read_hub_file_with_sha", lambda _path: ([m.to_dict() for m in existing], "sha"))
    monkeypatch.setattr(node.relay, "_write_hub_file", lambda _path, data, *, sha=None: written.extend(data))
    report = node.relay.push_to_hub_report([message])
    assert report.pushed == 1
    assert len(report.evicted_keys) == 1
    assert len(written) == 144
    assert (message.source, message.target, message.id) in report.acknowledged_keys
    assert message.id in {entry["id"] for entry in written}


def test_hub_normalizes_existing_duplicate_keys_before_capacity(tmp_path: Path, monkeypatch):
    (tmp_path / "nadi_inbox.json").write_text("[]")
    (tmp_path / "nadi_outbox.json").write_text("[]")
    node = NadiNode("me", tmp_path)
    existing = [NadiMessage(source="me", target="a", operation="old", payload={"n": index}) for index in range(143)]
    duplicate = existing[0].to_dict()
    duplicate["payload"] = {"n": "newest duplicate"}
    message = NadiMessage(source="me", target="a", operation="new", payload={})
    written = []
    monkeypatch.setattr(
        node.relay,
        "_read_hub_file_with_sha",
        lambda _path: ([m.to_dict() for m in existing] + [duplicate], "sha"),
    )
    monkeypatch.setattr(node.relay, "_write_hub_file", lambda _path, data, *, sha=None: written.extend(data))
    report = node.relay.push_to_hub_report([message])
    assert report.pushed == 1
    assert len(written) == 144
    assert len({(entry["source"], entry["target"], entry["id"]) for entry in written}) == 144


def test_acknowledgement_is_scoped_by_source_target_and_id(tmp_path: Path):
    transport = NadiTransport(tmp_path)
    shared_id = "shared-id"
    successful = NadiMessage(source="me", target="a", operation="x", payload={}, id=shared_id)
    failed = NadiMessage(source="me", target="b", operation="x", payload={}, id=shared_id)
    transport.append_to_outbox([successful, failed])
    transport.acknowledge_outbox({(successful.source, successful.target, successful.id)})
    assert [(m.target, m.id) for m in transport.read_outbox()] == [("b", shared_id)]


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


def test_keystore_loads_raw_hex_format(tmp_path):
    """NodeKeyStore must accept raw-hex secrets (Genesis-Hook format) so
    nodes don't generate fresh ephemeral keys per workflow run when the
    NODE_PRIVATE_KEY secret is stored as a 32-byte hex string."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from cryptography.hazmat.primitives import serialization

    from nadi_kit import NodeKeyStore, _derive_node_id

    sk = Ed25519PrivateKey.generate()
    priv_hex = sk.private_bytes(
        serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
        serialization.NoEncryption()
    ).hex()
    pub_hex = sk.public_key().public_bytes(
        serialization.Encoding.Raw, serialization.PublicFormat.Raw
    ).hex()

    p = tmp_path / "raw.json"
    p.write_text(priv_hex)  # raw 32-byte hex, no JSON wrapper

    ks_a = NodeKeyStore(p)
    ks_a.ensure_keys()
    assert ks_a.private_key == priv_hex
    assert ks_a.public_key == pub_hex
    assert ks_a.node_id == _derive_node_id(pub_hex)

    # Critical: reload yields SAME identity (no ephemeral regeneration)
    ks_b = NodeKeyStore(p)
    ks_b.ensure_keys()
    assert ks_b.node_id == ks_a.node_id


def test_keystore_falls_back_to_ephemeral_on_garbage(tmp_path, caplog):
    """If the file is neither JSON nor valid 32-byte hex, log a WARNING
    and generate a fresh key — better than silent rotation."""
    import logging

    from nadi_kit import NodeKeyStore

    p = tmp_path / "garbage.json"
    p.write_text("not json and not hex")

    with caplog.at_level(logging.WARNING, logger="nadi_kit"):
        ks = NodeKeyStore(p)
        ks.ensure_keys()
    assert ks.private_key  # fresh keypair generated
    assert any("format unrecognised" in r.message for r in caplog.records)
