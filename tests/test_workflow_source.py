"""The heartbeat must execute the reviewed checkout, never mutable remote code."""

from pathlib import Path


def test_heartbeat_installs_checkout_without_remote_source_fetch():
    workflow = Path(".github/workflows/hub-heartbeat.yml").read_text(encoding="utf-8")
    assert "python -m pip install ." in workflow
    assert "raw.githubusercontent.com" not in workflow
    assert "curl" not in workflow
    assert "FEDERATION_PAT" not in workflow.split("- name: Run NADI heartbeat cycle", 1)[0]

