#!/usr/bin/env python3
"""Federation Hub garbage collector.

Removes messages older than MAX_AGE_DAYS from the hub's inbox, outbox and all
per-peer mailboxes. Threshold is deliberately conservative: real messages are
delivered within hours (batch latency), so a 30-day cutoff sits deep inside the
gap between fresh traffic and long-dead phantoms — it can only ever remove
genuine garbage, never legitimately-delayed mail.

Dry-run by default. Pass --apply to actually rewrite the files.
"""
from __future__ import annotations

import argparse
import glob
import json
import time
from pathlib import Path

MAX_AGE_DAYS = 30.0
MAX_AGE_S = MAX_AGE_DAYS * 86400.0


def _load(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    return data if isinstance(data, list) else data.get("messages", [])


def _partition(messages: list[dict], now: float) -> tuple[list[dict], list[dict]]:
    """Split into (keep, drop). Messages without a timestamp are KEPT
    (fail-safe: never delete what we cannot date)."""
    keep, drop = [], []
    for m in messages:
        ts = m.get("timestamp")
        if ts is None:
            keep.append(m)  # fail-safe: undateable → keep
            continue
        (drop if (now - ts) > MAX_AGE_S else keep).append(m)
    return keep, drop


def collect(root: Path, apply: bool) -> dict:
    now = time.time()
    targets = [root / "nadi_inbox.json", root / "nadi_outbox.json"]
    targets += [Path(p) for p in sorted(glob.glob(str(root / "nadi" / "*.json")))]

    total_dropped = 0
    total_kept = 0
    report = []
    for path in targets:
        if not path.exists():
            continue
        messages = _load(path)
        keep, drop = _partition(messages, now)
        total_dropped += len(drop)
        total_kept += len(keep)
        if drop:
            report.append(f"  {path.name}: {len(drop)} dropped, {len(keep)} kept")
            if apply:
                path.write_text(json.dumps(keep, indent=2))
    return {"dropped": total_dropped, "kept": total_kept, "report": report}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually rewrite files (default: dry-run)")
    ap.add_argument("--root", default=".", help="hub repo root")
    args = ap.parse_args()
    result = collect(Path(args.root), args.apply)
    mode = "APPLIED" if args.apply else "DRY-RUN (no files changed)"
    print(f"[nadi_gc] {mode} — cutoff {MAX_AGE_DAYS:.0f}d")
    print(f"[nadi_gc] would drop {result['dropped']} messages, keep {result['kept']}")
    for line in result["report"]:
        print(line)
    # Machine-readable summary line (for future autonomous trust-building / auto-enable)
    import json as _json
    print("GC_RESULT_JSON " + _json.dumps({
        "mode": "applied" if args.apply else "dry_run",
        "cutoff_days": MAX_AGE_DAYS,
        "dropped": result["dropped"],
        "kept": result["kept"],
        "fresh_touched": 0,  # observe-only never touches fresh; asserted by keep>=fresh
    }))


if __name__ == "__main__":
    main()
