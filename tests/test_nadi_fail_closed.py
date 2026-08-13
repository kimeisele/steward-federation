"""Failure-path oracles for local durable NADI state."""

import pytest

from nadi_kit import NadiMessage, NadiTransport


def test_append_reports_atomic_write_failure(tmp_path, monkeypatch):
    transport = NadiTransport(tmp_path)
    monkeypatch.setattr("nadi_kit.os.replace", lambda *_args: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        transport.append_to_outbox([
            NadiMessage(source="a", target="b", operation="work", payload={})
        ])


def test_ack_reports_atomic_write_failure_and_does_not_claim_removal(tmp_path, monkeypatch):
    transport = NadiTransport(tmp_path)
    message = NadiMessage(source="a", target="b", operation="work", payload={})
    transport.append_to_outbox([message])
    monkeypatch.setattr("nadi_kit.os.replace", lambda *_args: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        transport.acknowledge_outbox({message.id})

