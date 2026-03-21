"""
nadi_kit — Shared NADI federation transport for agent mesh nodes.

Canonical source: github.com/kimeisele/steward-federation
Protocol: NADI (Network Addressed Delivery Infrastructure)

Install: pip install git+https://github.com/kimeisele/steward-federation.git
Vendor:  copy nadi_kit.py into your repo

Usage:
    from nadi_kit import NadiNode

    node = NadiNode.from_peer_json("data/federation/peer.json")
    node.on("heartbeat", lambda msg: print(f"← {msg.source}"))
    node.emit("status", {"health": 1.0}, target="steward")
    node.sync()  # pull → process → flush → push
"""

from __future__ import annotations

import base64
import json
import logging
import os
import subprocess
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Callable

__all__ = ["NadiMessage", "NadiTransport", "NadiHubRelay", "NadiNode"]
__version__ = "0.1.0"

log = logging.getLogger("nadi_kit")

# ── Constants ────────────────────────────────────────────────────────────────

NADI_BUFFER_SIZE: int = 144
NADI_DEFAULT_TTL_S: float = 7200.0  # 2 hours (survives ~4 heartbeat windows)
NADI_HEARTBEAT_TTL_S: float = 900.0  # 15 minutes
HUB_REPO: str = "kimeisele/steward-federation"
HUB_NADI_DIR: str = "nadi"
MIN_RELAY_INTERVAL_S: float = 30.0


# ── NadiMessage ──────────────────────────────────────────────────────────────


@dataclass
class NadiMessage:
    """A single NADI federation message."""

    source: str
    target: str
    operation: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    priority: int = 1  # 0=low, 1=normal, 2=high, 3=critical
    correlation_id: str = ""
    ttl_s: float = NADI_DEFAULT_TTL_S
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def is_expired(self) -> bool:
        return time.time() > self.timestamp + self.ttl_s

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> NadiMessage:
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in d.items() if k in known}
        # Ensure required fields
        filtered.setdefault("source", "unknown")
        filtered.setdefault("target", "*")
        filtered.setdefault("operation", "unknown")
        filtered.setdefault("payload", {})
        return cls(**filtered)


# ── NadiTransport (local file I/O) ──────────────────────────────────────────


class NadiTransport:
    """Atomic file-based NADI transport (self-hosted semantics).

    Each node owns:
      - outbox: messages TO send (we write, relay reads)
      - inbox:  messages received (relay writes, we read)
    """

    def __init__(self, federation_dir: str | Path) -> None:
        self.federation_dir = Path(federation_dir)
        self.outbox_path = self.federation_dir / "nadi_outbox.json"
        self.inbox_path = self.federation_dir / "nadi_inbox.json"

    def read_inbox(self) -> list[NadiMessage]:
        raw = self._atomic_read(self.inbox_path)
        msgs = [NadiMessage.from_dict(d) for d in raw]
        return [m for m in msgs if not m.is_expired]

    def read_outbox(self) -> list[NadiMessage]:
        raw = self._atomic_read(self.outbox_path)
        msgs = [NadiMessage.from_dict(d) for d in raw]
        return [m for m in msgs if not m.is_expired]

    def append_to_outbox(self, messages: list[NadiMessage]) -> int:
        return self._atomic_append(self.outbox_path, messages)

    def append_to_inbox(self, messages: list[NadiMessage]) -> int:
        return self._atomic_append(self.inbox_path, messages)

    def clear_outbox(self) -> int:
        old = self._atomic_read(self.outbox_path)
        self._atomic_write(self.outbox_path, [])
        return len(old)

    def clear_inbox(self) -> int:
        old = self._atomic_read(self.inbox_path)
        self._atomic_write(self.inbox_path, [])
        return len(old)

    def clear_expired(self) -> dict[str, int]:
        """Remove expired messages from both files."""
        now = time.time()
        result = {}
        for name, path in [("inbox", self.inbox_path), ("outbox", self.outbox_path)]:
            raw = self._atomic_read(path)
            alive = [d for d in raw if now <= d.get("timestamp", 0) + d.get("ttl_s", NADI_DEFAULT_TTL_S)]
            removed = len(raw) - len(alive)
            if removed > 0:
                self._atomic_write(path, alive)
            result[name] = removed
        return result

    def stats(self) -> dict[str, int]:
        return {
            "inbox": len(self._atomic_read(self.inbox_path)),
            "outbox": len(self._atomic_read(self.outbox_path)),
        }

    # ── Internal ─────────────────────────────────────────────────────────

    def _atomic_read(self, path: Path) -> list[dict]:
        if not path.exists():
            return []
        try:
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                return []
            data = json.loads(text)
            return data if isinstance(data, list) else []
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("nadi read error %s: %s", path.name, exc)
            return []

    def _atomic_write(self, path: Path, data: list[dict]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = None
        try:
            tmp = tempfile.NamedTemporaryFile(
                mode="w", suffix=".tmp", dir=path.parent, delete=False, encoding="utf-8"
            )
            json.dump(data, tmp, indent=2, default=str)
            tmp.flush()
            os.fsync(tmp.fileno())
            tmp.close()
            os.replace(tmp.name, path)
        except OSError as exc:
            log.error("nadi write error %s: %s", path.name, exc)
            if tmp and os.path.exists(tmp.name):
                os.unlink(tmp.name)

    def _atomic_append(self, path: Path, messages: list[NadiMessage]) -> int:
        existing = self._atomic_read(path)
        seen = {(d.get("source"), d.get("timestamp")) for d in existing}
        new = [m.to_dict() for m in messages if (m.source, m.timestamp) not in seen]
        merged = (existing + new)[-NADI_BUFFER_SIZE:]
        self._atomic_write(path, merged)
        return len(new)


# ── NadiHubRelay (GitHub API) ───────────────────────────────────────────────


class NadiHubRelay:
    """Push/pull messages via steward-federation hub on GitHub.

    Hub layout:
      nadi/{source}_to_{target}.json — per-peer mailbox (one writer)
    """

    def __init__(self, agent_id: str, *, hub_repo: str = HUB_REPO) -> None:
        self.agent_id = agent_id
        self.hub_repo = hub_repo
        self._last_pull: float = 0.0
        self._last_push: float = 0.0

    def pull_from_hub(self) -> list[NadiMessage]:
        """Read messages addressed to us from hub per-peer mailboxes."""
        if time.time() - self._last_pull < MIN_RELAY_INTERVAL_S:
            log.debug("relay pull throttled")
            return []

        messages: list[NadiMessage] = []
        try:
            listing = self._gh_api(f"repos/{self.hub_repo}/contents/{HUB_NADI_DIR}")
            if not listing:
                return []

            suffix = f"_to_{self.agent_id}.json"
            for entry in listing:
                name = entry.get("name", "")
                if not name.endswith(suffix):
                    continue
                raw = self._read_hub_file(f"{HUB_NADI_DIR}/{name}")
                for d in raw:
                    try:
                        msg = NadiMessage.from_dict(d)
                        if not msg.is_expired:
                            messages.append(msg)
                    except Exception as exc:
                        log.warning("skip malformed hub message: %s", exc)

            self._last_pull = time.time()
            log.info("pulled %d messages from hub", len(messages))
        except Exception as exc:
            log.warning("relay pull failed: %s", exc)

        return messages

    def push_to_hub(self, messages: list[NadiMessage]) -> int:
        """Write messages to hub per-peer mailboxes. Returns count pushed."""
        if not messages:
            return 0
        if time.time() - self._last_push < MIN_RELAY_INTERVAL_S:
            log.debug("relay push throttled")
            return 0

        by_target: dict[str, list[dict]] = {}
        for msg in messages:
            by_target.setdefault(msg.target, []).append(msg.to_dict())

        pushed = 0
        for target, batch in by_target.items():
            filename = f"{HUB_NADI_DIR}/{self.agent_id}_to_{target}.json"
            try:
                existing, sha = self._read_hub_file_with_sha(filename)
                seen = {(d.get("source"), d.get("timestamp")) for d in existing}
                new = [d for d in batch if (d.get("source"), d.get("timestamp")) not in seen]
                if not new:
                    continue
                merged = (existing + new)[-NADI_BUFFER_SIZE:]
                self._write_hub_file(filename, merged, sha=sha)
                pushed += len(new)
            except Exception as exc:
                log.warning("relay push to %s failed: %s", target, exc)

        self._last_push = time.time()
        return pushed

    def discover_hub_peers(self) -> list[str]:
        """Discover peers from hub nadi/ directory listing."""
        try:
            listing = self._gh_api(f"repos/{self.hub_repo}/contents/{HUB_NADI_DIR}")
            if not listing:
                return []
            peers = set()
            for entry in listing:
                name = entry.get("name", "")
                if "_to_" in name and name.endswith(".json"):
                    parts = name.replace(".json", "").split("_to_")
                    peers.update(parts)
            peers.discard(self.agent_id)
            return sorted(peers)
        except Exception:
            return []

    # ── Internal GitHub API ──────────────────────────────────────────────

    def _gh_api(self, endpoint: str) -> Any:
        result = subprocess.run(
            ["gh", "api", endpoint],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            log.debug("gh api %s failed: %s", endpoint, result.stderr.strip())
            return None
        return json.loads(result.stdout)

    def _read_hub_file(self, filepath: str) -> list[dict]:
        data = self._gh_api(f"repos/{self.hub_repo}/contents/{filepath}")
        if not data or "content" not in data:
            return []
        try:
            content = base64.b64decode(data["content"]).decode("utf-8")
            parsed = json.loads(content)
            return parsed if isinstance(parsed, list) else []
        except Exception as exc:
            log.warning("hub file parse error %s: %s", filepath, exc)
            return []

    def _read_hub_file_with_sha(self, filepath: str) -> tuple[list[dict], str | None]:
        data = self._gh_api(f"repos/{self.hub_repo}/contents/{filepath}")
        if not data:
            return [], None
        sha = data.get("sha")
        if "content" not in data:
            return [], sha
        try:
            content = base64.b64decode(data["content"]).decode("utf-8")
            parsed = json.loads(content)
            return (parsed if isinstance(parsed, list) else []), sha
        except Exception:
            return [], sha

    def _write_hub_file(self, filepath: str, data: list[dict], *, sha: str | None = None) -> None:
        content_b64 = base64.b64encode(
            json.dumps(data, indent=2, default=str).encode("utf-8")
        ).decode("ascii")
        args = [
            "gh", "api", f"repos/{self.hub_repo}/contents/{filepath}",
            "-X", "PUT",
            "-f", f"message=nadi: {self.agent_id} relay update",
            "-f", f"content={content_b64}",
        ]
        if sha:
            args.extend(["-f", f"sha={sha}"])

        result = subprocess.run(args, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise RuntimeError(f"hub write {filepath}: {result.stderr.strip()}")


# ── NadiNode ─────────────────────────────────────────────────────────────────


class NadiNode:
    """Complete NADI federation node — emit, receive, relay, process.

    Usage:
        node = NadiNode.from_peer_json("data/federation/peer.json")
        node.on("heartbeat", my_handler)
        node.emit("status", {"health": 0.95})
        node.sync()
    """

    def __init__(
        self,
        agent_id: str,
        federation_dir: str | Path,
        *,
        repo: str = "",
        capabilities: list[str] | None = None,
        hub_repo: str = HUB_REPO,
    ) -> None:
        self.agent_id = agent_id
        self.repo = repo
        self.capabilities = capabilities or []
        self.transport = NadiTransport(federation_dir)
        self.relay = NadiHubRelay(agent_id, hub_repo=hub_repo)
        self._handlers: dict[str, Callable[[NadiMessage], None]] = {}
        self._peers: list[str] = []
        self._processed: set[tuple[str, float]] = set()

    @classmethod
    def from_peer_json(cls, path: str | Path, *, hub_repo: str = HUB_REPO) -> NadiNode:
        """Create a NadiNode from a peer.json descriptor file."""
        path = Path(path).resolve()
        data = json.loads(path.read_text(encoding="utf-8"))

        identity = data.get("identity", {})
        agent_id = (
            identity.get("city_id")
            or identity.get("agent_id")
            or data.get("agent_id", "unknown")
        )
        repo = identity.get("repo", data.get("repo", ""))
        capabilities = data.get("capabilities", [])

        # federation_dir is the parent directory of peer.json
        fed_dir = path.parent

        return cls(
            agent_id=agent_id,
            federation_dir=fed_dir,
            repo=repo,
            capabilities=capabilities,
            hub_repo=hub_repo,
        )

    # ── Handler Registration ─────────────────────────────────────────────

    def on(self, operation: str, handler: Callable[[NadiMessage], None]) -> None:
        """Register a handler for a NADI operation type."""
        self._handlers[operation] = handler

    # ── Emit ─────────────────────────────────────────────────────────────

    def emit(
        self,
        operation: str,
        payload: dict[str, Any],
        *,
        target: str = "*",
        priority: int = 1,
        ttl_s: float = NADI_DEFAULT_TTL_S,
        correlation_id: str = "",
    ) -> list[NadiMessage]:
        """Queue message(s) to outbox. target='*' expands to all known peers."""
        targets = self._resolve_targets(target)
        messages = []
        for t in targets:
            msg = NadiMessage(
                source=self.agent_id,
                target=t,
                operation=operation,
                payload=payload,
                priority=priority,
                ttl_s=ttl_s,
                correlation_id=correlation_id,
            )
            messages.append(msg)

        if messages:
            self.transport.append_to_outbox(messages)
            log.info("emit %s → %s (%d targets)", operation, target, len(messages))

        return messages

    def heartbeat(self, *, health: float = 1.0, version: str = "0.1.0") -> list[NadiMessage]:
        """Emit a heartbeat broadcast to all peers."""
        return self.emit(
            "heartbeat",
            {
                "agent_id": self.agent_id,
                "health": health,
                "timestamp": time.time(),
                "capabilities": self.capabilities,
                "repo": self.repo,
                "version": version,
            },
            target="*",
            priority=1,
            ttl_s=NADI_HEARTBEAT_TTL_S,
        )

    # ── Receive & Process ────────────────────────────────────────────────

    def receive(self) -> list[NadiMessage]:
        """Read inbox, return unprocessed messages sorted by priority."""
        messages = self.transport.read_inbox()
        unprocessed = [m for m in messages if (m.source, m.timestamp) not in self._processed]
        unprocessed.sort(key=lambda m: -m.priority)
        return unprocessed

    def process_inbox(self) -> int:
        """Read inbox, dispatch to handlers. Returns count processed."""
        messages = self.receive()
        processed = 0
        for msg in messages:
            key = (msg.source, msg.timestamp)
            handler = self._handlers.get(msg.operation)
            if handler:
                try:
                    handler(msg)
                    processed += 1
                except Exception as exc:
                    log.error("handler %s failed: %s", msg.operation, exc)
            else:
                log.debug("no handler for op=%s from %s", msg.operation, msg.source)
            self._processed.add(key)

            # Prevent unbounded growth
            if len(self._processed) > 5000:
                self._processed = set(list(self._processed)[-2500:])

        return processed

    # ── Sync (full cycle) ────────────────────────────────────────────────

    def sync(self) -> dict[str, int]:
        """Full sync: pull from hub → process inbox → flush outbox → push to hub."""
        stats = {"pulled": 0, "processed": 0, "pushed": 0, "expired": 0}

        # 1. Pull from hub → local inbox
        try:
            hub_messages = self.relay.pull_from_hub()
            if hub_messages:
                added = self.transport.append_to_inbox(hub_messages)
                stats["pulled"] = added
        except Exception as exc:
            log.warning("hub pull failed: %s", exc)

        # 2. Process inbox
        stats["processed"] = self.process_inbox()

        # 3. Push outbox → hub
        try:
            outbox = self.transport.read_outbox()
            if outbox:
                stats["pushed"] = self.relay.push_to_hub(outbox)
                if stats["pushed"] > 0:
                    self.transport.clear_outbox()
        except Exception as exc:
            log.warning("hub push failed: %s", exc)

        # 4. Clean expired
        expired = self.transport.clear_expired()
        stats["expired"] = sum(expired.values())

        return stats

    # ── Peers ────────────────────────────────────────────────────────────

    def set_peers(self, peers: list[str]) -> None:
        """Set known peer list for broadcast resolution."""
        self._peers = [p for p in peers if p != self.agent_id]

    def load_peers_from_seeds(self, seeds_path: str | Path | None = None) -> list[str]:
        """Load peers from authority-descriptor-seeds.json or hub directory."""
        if seeds_path is None:
            seeds_path = self.transport.federation_dir / "authority-descriptor-seeds.json"
        seeds_path = Path(seeds_path)

        if seeds_path.exists():
            try:
                data = json.loads(seeds_path.read_text(encoding="utf-8"))
                seeds = data if isinstance(data, list) else data.get("seeds", [])
                peers = []
                for seed in seeds:
                    if isinstance(seed, str):
                        # URL: .../kimeisele/{repo}/...
                        parts = seed.split("/")
                        for i, p in enumerate(parts):
                            if p == "kimeisele" and i + 1 < len(parts):
                                peers.append(parts[i + 1])
                                break
                    elif isinstance(seed, dict):
                        aid = seed.get("agent_id") or seed.get("repo", "").split("/")[-1]
                        if aid:
                            peers.append(aid)
                self._peers = [p for p in peers if p != self.agent_id]
                return self._peers
            except Exception as exc:
                log.warning("load seeds failed: %s", exc)

        # Fallback: discover from hub
        discovered = self.relay.discover_hub_peers()
        if discovered:
            self._peers = discovered
            return self._peers

        # Last resort: well-known federation peers
        self._peers = [
            p
            for p in ["steward", "agent-city", "agent-world", "agent-internet", "steward-protocol"]
            if p != self.agent_id
        ]
        return self._peers

    def _resolve_targets(self, target: str) -> list[str]:
        """Resolve target string to concrete peer list."""
        if target == "*":
            if not self._peers:
                self.load_peers_from_seeds()
            return self._peers
        return [target]

    # ── Stats ────────────────────────────────────────────────────────────

    def stats(self) -> dict[str, Any]:
        ts = self.transport.stats()
        return {
            "agent_id": self.agent_id,
            "repo": self.repo,
            "capabilities": self.capabilities,
            "peers": self._peers,
            "inbox": ts["inbox"],
            "outbox": ts["outbox"],
            "processed": len(self._processed),
        }


# ── CLI ──────────────────────────────────────────────────────────────────────


def _find_peer_json() -> Path:
    """Find peer.json in common locations."""
    candidates = [
        Path("data/federation/peer.json"),
        Path("peer.json"),
    ]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError("No peer.json found. Run from repo root or specify --peer-json.")


def cli_main() -> int:
    """CLI entry point: nadi_kit send|recv|sync|stats|heartbeat"""
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(name)s %(message)s")

    parser = argparse.ArgumentParser(prog="nadi_kit", description="NADI federation toolkit")
    parser.add_argument("--peer-json", type=Path, help="Path to peer.json")
    sub = parser.add_subparsers(dest="command")

    p_send = sub.add_parser("send", help="Send a message")
    p_send.add_argument("--to", required=True, help="Target agent or '*'")
    p_send.add_argument("--op", required=True, help="Operation name")
    p_send.add_argument("--payload", default="{}", help="JSON payload")
    p_send.add_argument("--priority", type=int, default=1)

    sub.add_parser("sync", help="Full sync cycle (pull/process/push)")
    sub.add_parser("stats", help="Show inbox/outbox stats")

    p_hb = sub.add_parser("heartbeat", help="Emit heartbeat")
    p_hb.add_argument("--health", type=float, default=1.0)

    sub.add_parser("recv", help="Read and display inbox")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return 1

    peer_json = args.peer_json or _find_peer_json()
    node = NadiNode.from_peer_json(peer_json)

    if args.command == "send":
        payload = json.loads(args.payload)
        msgs = node.emit(args.op, payload, target=args.to, priority=args.priority)
        print(f"queued {len(msgs)} message(s) to outbox")

    elif args.command == "sync":
        result = node.sync()
        print(json.dumps(result, indent=2))

    elif args.command == "stats":
        print(json.dumps(node.stats(), indent=2))

    elif args.command == "heartbeat":
        msgs = node.heartbeat(health=args.health)
        print(f"queued {len(msgs)} heartbeat(s)")

    elif args.command == "recv":
        messages = node.receive()
        for m in messages:
            age = time.time() - m.timestamp
            print(f"  [{m.operation}] {m.source} → {m.target} (pri={m.priority}, age={age:.0f}s)")
            if m.payload:
                print(f"    {json.dumps(m.payload, indent=4)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
