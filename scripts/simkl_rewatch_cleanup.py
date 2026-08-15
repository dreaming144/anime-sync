#!/usr/bin/env python3
"""Inventory (and optionally attempt removal of) SIMKL rewatch sessions.

Canonical rows (is_rewatch=false / oldest original watch) are never targeted.
Only is_rewatch=true rows are listed / optionally removed.

Default mode is dry-run inventory only.

Usage:
  SIMKL_CLIENT_ID=... SIMKL_ACCESS_TOKEN=... python scripts/simkl_rewatch_cleanup.py
  ... python scripts/simkl_rewatch_cleanup.py --execute   # attempt API removal
  ... python scripts/simkl_rewatch_cleanup.py --type anime --json-out rewatches.json

Removal uses POST /sync/history/remove?allow_rewatch=yes with rewatch_id.
SIMKL's public docs do not fully specify session delete; --execute is best-effort.
If the API rejects or no-ops, use the website Rewatch panel → Clear All per title.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

import requests

API = "https://api.simkl.com"
APP_NAME = "anime-sync-rewatch-cleanup"
APP_VERSION = "1.0"


def env_creds() -> tuple[str, str]:
    client_id = os.getenv("SIMKL_CLIENT_ID", "").strip()
    token = os.getenv("SIMKL_ACCESS_TOKEN", "").strip()
    if not client_id or not token:
        sys.exit("Need SIMKL_CLIENT_ID and SIMKL_ACCESS_TOKEN in the environment")
    return client_id, token


def headers(token: str, client_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "simkl-api-key": client_id,
        "Content-Type": "application/json",
        "User-Agent": f"{APP_NAME}/{APP_VERSION}",
        "Accept": "application/json",
    }


def qs(client_id: str, **extra: str) -> str:
    parts = [
        f"client_id={client_id}",
        f"app-name={APP_NAME}",
        f"app-version={APP_VERSION}",
    ]
    for k, v in extra.items():
        parts.append(f"{k}={v}")
    return "&".join(parts)


def fetch_all_items(
    client_id: str,
    token: str,
    media_type: str = "anime",
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Pull library with rewatch rows expanded."""
    # status path segment optional; without it SIMKL returns all statuses for type
    path = f"/sync/all-items/{media_type}"
    if status:
        path += f"/{status}"
    url = (
        f"{API}{path}?{qs(client_id)}"
        f"&allow_rewatch=yes&extended=full&episode_watched_at=yes"
    )
    r = requests.get(url, headers=headers(token, client_id), timeout=60)
    if not r.ok:
        sys.exit(f"GET {path} failed: {r.status_code} {r.text[:300]}")
    data = r.json()
    if isinstance(data, list):
        return data
    # responses are often { "anime": [...] } or { "shows": [...] } or { "movies": [...] }
    for key in ("anime", "shows", "movies", media_type):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def entry_ids(entry: dict[str, Any]) -> dict[str, Any]:
    show = entry.get("show") or entry.get("movie") or entry
    ids = entry.get("ids") or show.get("ids") or {}
    return dict(ids) if isinstance(ids, dict) else {}


def entry_title(entry: dict[str, Any]) -> str:
    show = entry.get("show") or entry.get("movie") or {}
    return (show.get("title") or entry.get("title") or "") if isinstance(show, dict) else ""


def simkl_id(ids: dict[str, Any]) -> str | None:
    v = ids.get("simkl") or ids.get("simkl_id")
    return str(v) if v is not None and str(v).strip() != "" else None


def classify(entries: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    """Return (canonical_rows, rewatch_rows)."""
    canonical, rewatches = [], []
    for e in entries:
        if e.get("is_rewatch") is True or e.get("rewatch_id") is not None and e.get("is_rewatch") is not False:
            # Prefer explicit is_rewatch True; treat rewatch_id alone as rewatch when is_rewatch missing
            if e.get("is_rewatch") is True or (e.get("rewatch_id") and e.get("is_rewatch") is not False):
                if e.get("is_rewatch") is True:
                    rewatches.append(e)
                    continue
        if e.get("is_rewatch") is True:
            rewatches.append(e)
        else:
            canonical.append(e)
    # Second pass: any with is_rewatch True
    rewatches = [e for e in entries if e.get("is_rewatch") is True]
    canonical = [e for e in entries if e.get("is_rewatch") is not True]
    return canonical, rewatches


def summarize(canonical: list[dict], rewatches: list[dict]) -> list[dict[str, Any]]:
    """Group rewatches under their simkl id with canonical context."""
    by_id: dict[str, dict[str, Any]] = {}
    for e in canonical:
        ids = entry_ids(e)
        sid = simkl_id(ids)
        if not sid:
            continue
        by_id[sid] = {
            "simkl": sid,
            "title": entry_title(e),
            "mal": ids.get("mal"),
            "anilist": ids.get("anilist"),
            "status": e.get("status"),
            "last_watched_at": e.get("last_watched_at"),
            "watched_episodes_count": e.get("watched_episodes_count"),
            "rewatch_sessions": [],
        }
    for e in rewatches:
        ids = entry_ids(e)
        sid = simkl_id(ids)
        if not sid:
            continue
        if sid not in by_id:
            by_id[sid] = {
                "simkl": sid,
                "title": entry_title(e),
                "mal": ids.get("mal"),
                "anilist": ids.get("anilist"),
                "status": None,
                "last_watched_at": None,
                "watched_episodes_count": None,
                "rewatch_sessions": [],
            }
        by_id[sid]["rewatch_sessions"].append(
            {
                "rewatch_id": e.get("rewatch_id"),
                "rewatch_status": e.get("rewatch_status"),
                "last_watched_at": e.get("last_watched_at"),
                "watched_episodes_count": e.get("watched_episodes_count"),
                "status": e.get("status"),
            }
        )
    # only titles that actually have rewatch sessions
    return sorted(
        [v for v in by_id.values() if v["rewatch_sessions"]],
        key=lambda x: (x.get("title") or "").lower(),
    )


def attempt_remove_session(
    client_id: str,
    token: str,
    media_type: str,
    simkl: str,
    rewatch_id: int | str,
    extra_ids: dict[str, Any] | None = None,
) -> tuple[int, str]:
    """Best-effort remove one rewatch session via history/remove.

    Body mirrors history writes; allow_rewatch=yes + rewatch_id targets the session.
    """
    ids = {"simkl": int(simkl) if str(simkl).isdigit() else simkl}
    if extra_ids:
        for k in ("mal", "anilist", "anidb", "kitsu"):
            if extra_ids.get(k):
                ids[k] = extra_ids[k]
    body_item: dict[str, Any] = {
        "ids": ids,
        "is_rewatch": True,
        "rewatch_id": int(rewatch_id) if str(rewatch_id).isdigit() else rewatch_id,
    }
    # Anime goes under shows[] on history endpoints
    key = "movies" if media_type == "movies" else "shows"
    payload = {key: [body_item]}
    url = f"{API}/sync/history/remove?{qs(client_id, allow_rewatch='yes')}"
    r = requests.post(url, headers=headers(token, client_id), json=payload, timeout=30)
    return r.status_code, r.text[:500]


def write_reports(rows: list[dict[str, Any]], json_out: Path | None, csv_out: Path | None) -> None:
    if json_out:
        json_out.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"Wrote {json_out}")
    if csv_out:
        flat = []
        for row in rows:
            for sess in row["rewatch_sessions"]:
                flat.append(
                    {
                        "simkl": row["simkl"],
                        "title": row["title"],
                        "mal": row.get("mal") or "",
                        "anilist": row.get("anilist") or "",
                        "canonical_status": row.get("status") or "",
                        "canonical_last_watched_at": row.get("last_watched_at") or "",
                        "rewatch_id": sess.get("rewatch_id") or "",
                        "rewatch_status": sess.get("rewatch_status") or "",
                        "session_last_watched_at": sess.get("last_watched_at") or "",
                        "session_progress": sess.get("watched_episodes_count") or "",
                    }
                )
        fields = list(flat[0].keys()) if flat else [
            "simkl", "title", "mal", "anilist", "canonical_status",
            "canonical_last_watched_at", "rewatch_id", "rewatch_status",
            "session_last_watched_at", "session_progress",
        ]
        with csv_out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(flat)
        print(f"Wrote {csv_out} ({len(flat)} session rows)")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="SIMKL rewatch session inventory / cleanup")
    ap.add_argument(
        "--type",
        choices=["anime", "shows", "movies", "all"],
        default="anime",
        help="Media library to scan; 'all' runs anime + shows + movies in one pass",
    )
    ap.add_argument("--status", default=None, help="Optional status filter (e.g. completed)")
    ap.add_argument("--json-out", type=Path, default=Path("simkl_rewatches.json"))
    ap.add_argument("--csv-out", type=Path, default=Path("simkl_rewatches.csv"))
    ap.add_argument(
        "--execute",
        action="store_true",
        help="Attempt API removal of rewatch sessions (default: dry-run inventory only)",
    )
    ap.add_argument("--limit", type=int, default=0, help="Max sessions to remove when --execute (0=all)")
    ap.add_argument("--sleep", type=float, default=0.35, help="Delay between remove calls")
    args = ap.parse_args(argv)

    client_id, token = env_creds()
    types = ["anime", "shows", "movies"] if args.type == "all" else [args.type]

    all_rows: list[dict[str, Any]] = []
    for media_type in types:
        print(f"-> Fetching SIMKL {media_type} with allow_rewatch=yes …")
        try:
            entries = fetch_all_items(client_id, token, media_type=media_type, status=args.status)
        except Exception as e:
            print(f"   ERROR fetching {media_type}: {e}")
            continue
        print(f"   Received {len(entries)} row(s) (canonical + rewatch)")

        canonical, rewatches = classify(entries)
        print(f"   Canonical: {len(canonical)}  |  Rewatch sessions: {len(rewatches)}")

        rows = summarize(canonical, rewatches)
        for row in rows:
            row["media_type"] = media_type
        print(f"   Titles with ≥1 rewatch: {len(rows)}")
        all_rows.extend(rows)

    print(f"-> Combined: {len(all_rows)} title(s) with rewatches across {', '.join(types)}")
    write_reports(all_rows, args.json_out, args.csv_out)

    if not all_rows:
        print("Nothing to clean up.")
        return 0

    # Preview
    for row in all_rows[:20]:
        n = len(row["rewatch_sessions"])
        mt = row.get("media_type") or args.type
        print(f"   • [{mt}] {row['title']!r} simkl={row['simkl']} sessions={n}")
    if len(all_rows) > 20:
        print(f"   … and {len(all_rows) - 20} more titles")

    if not args.execute:
        print(
            "\nDry-run only. Canonical (oldest) watches were NOT modified.\n"
            "Re-run with --execute to attempt POST /sync/history/remove "
            "for each rewatch_id (best-effort; website Clear All if API no-ops)."
        )
        return 0

    removed = 0
    failed = 0
    count = 0
    for row in all_rows:
        media_type = row.get("media_type") or (types[0] if types else "anime")
        for sess in row["rewatch_sessions"]:
            rid = sess.get("rewatch_id")
            if rid is None:
                print(f"   skip {row['title']}: no rewatch_id")
                continue
            if args.limit and count >= args.limit:
                print(f"Reached --limit {args.limit}")
                print(f"Done. HTTP-success removals={removed} failed={failed}")
                return 0 if not failed else 2
            code, body = remove_rewatch(
                client_id,
                token,
                media_type=media_type,
                simkl=row["simkl"],
                rewatch_id=rid,
                title=row.get("title"),
                extra_ids={"mal": row.get("mal"), "anilist": row.get("anilist")},
            )
            count += 1
            ok = 200 <= code < 300
            if ok:
                removed += 1
                print(f"   removed [{media_type}] rewatch_id={rid} {row['title']!r} [{code}]")
            else:
                failed += 1
                print(f"   FAIL [{media_type}] rewatch_id={rid} {row['title']!r} [{code}] {body[:120]}")
            time.sleep(args.sleep)

    print(f"Done. HTTP-success removals={removed} failed={failed}")
    if failed:
        print(
            "Some removals failed — SIMKL may require website Clear All for those sessions. "
            "Canonical rows were never sent without rewatch_id."
        )
        return 2
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
