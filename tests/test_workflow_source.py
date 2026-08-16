"""The heartbeat must execute the reviewed checkout, never mutable remote code."""

from pathlib import Path


def test_heartbeat_installs_checkout_without_remote_source_fetch():
    workflow = Path(".github/workflows/hub-heartbeat.yml").read_text(encoding="utf-8")
    assert "python -m pip install ." in workflow
    assert "raw.githubusercontent.com" not in workflow
    assert "curl" not in workflow
    assert "FEDERATION_PAT" not in workflow.split("- name: Run NADI heartbeat cycle", 1)[0]


def test_heartbeat_checkout_is_shallow():
    """The heartbeat must not re-introduce a full-history checkout.

    The workflow only needs the current tree: no step calls git log,
    describe, show, rev-parse of ancestors, or tags, and nadi_kit.py only
    uses gh api. The race-recovery rebase (git pull --rebase origin main)
    works from a shallow clone because git fetches the required depth.
    """
    workflow = Path(".github/workflows/hub-heartbeat.yml").read_text(encoding="utf-8")
    checkout = workflow.split("actions/checkout@v4", 1)[1]
    assert "fetch-depth: 1" in checkout

