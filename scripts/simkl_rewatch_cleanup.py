#!/usr/bin/env python3
"""Inventory (and optionally attempt removal of) SIMKL rewatch sessions.

Canonical rows (is_rewatch=false / oldest original watch) are never targeted.
Only is_rewatch=true rows are listed / optionally removed.

Default mode is dry-run inventory only.

Usage:
  SIMKL_CLIENT_ID=... SIMKL_ACCESS_TOKEN=... python scripts/simkl_rewatch_cleanup.py
  ... python scripts/simkl_rewatch_cleanup.py --execute
  ... python scripts/simkl_rewatch_cleanup.py --type anime --json-out rewatches.json
  ... python scripts/simkl_rewatch_cleanup.py --type all --execute

Removal uses POST /sync/history/remove?allow_rewatch=yes with rewatch_id.
SIMKL's public docs do not fully specify session delete; --execute is best-effort.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests

API = "https://api.simkl.com"
APP_NAME = "anime-sync-rewatch-cleanup"
APP_VERSION = "1.1"


class SimklAPIError(Exception):
    """Non-recoverable or exhausted SIMKL API failure."""

    def __init__(self, message: str, *, status: int | None = None, body: str = "", kind: str = ""):
        super().__init__(message)
        self.status = status
        self.body = body
        self.kind = kind or classify_status(status)


def classify_status(status: int | None) -> str:
    if status is None:
        return "network"
    if status in (401, 403):
        return "auth"
    if status == 404:
        return "not_found"
    if status == 412:
        return "client_or_quota"
    if status == 429:
        return "rate_limit"
    if status in (500, 502, 503, 504):
        return "server"
    if 400 <= status < 500:
        return "client"
    return "unknown"


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


def api_request(
    method: str,
    url: str,
    *,
    token: str,
    client_id: str,
    json_body: dict | None = None,
    timeout: float = 45.0,
    max_retries: int = 4,
    retry_on: tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Response:
    """HTTP with retries for rate-limit/server/network errors."""
    last_exc: Exception | None = None
    last_resp: requests.Response | None = None
    for attempt in range(max_retries):
        try:
            resp = requests.request(
                method,
                url,
                headers=headers(token, client_id),
                json=json_body,
                timeout=timeout,
            )
            last_resp = resp
            if resp.status_code in retry_on and attempt < max_retries - 1:
                ra = resp.headers.get("Retry-After") or resp.headers.get("retry-after")
                wait = 0.0
                if ra is not None:
                    try:
                        wait = float(ra)
                    except ValueError:
                        wait = 0.0
                if wait <= 0:
                    wait = min(60.0, (2 ** attempt) + 0.5)
                kind = classify_status(resp.status_code)
                print(
                    f"   API {resp.status_code} ({kind}) — retry in {wait:.1f}s "
                    f"(attempt {attempt + 1}/{max_retries}) {url[:72]}"
                )
                time.sleep(wait)
                continue
            return resp
        except requests.Timeout as e:
            last_exc = e
            if attempt >= max_retries - 1:
                break
            wait = min(30.0, (2 ** attempt) + 0.5)
            print(f"   API timeout — retry in {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
        except requests.RequestException as e:
            last_exc = e
            if attempt >= max_retries - 1:
                break
            wait = min(30.0, (2 ** attempt) + 0.5)
            print(f"   API network error: {e} — retry in {wait:.1f}s (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait)
    if last_resp is not None:
        return last_resp
    raise SimklAPIError(
        f"SIMKL request failed after {max_retries} attempts: {last_exc}",
        status=None,
        body=str(last_exc or ""),
        kind="network",
    )


def fetch_all_items(
    client_id: str,
    token: str,
    media_type: str = "anime",
    status: str | None = None,
) -> list[dict[str, Any]]:
    """Pull library with rewatch rows expanded. Raises SimklAPIError on hard failure."""
    path = f"/sync/all-items/{media_type}"
    if status:
        path += f"/{status}"
    url = (
        f"{API}{path}?{qs(client_id)}"
        f"&allow_rewatch=yes&extended=full&episode_watched_at=yes"
    )
    r = api_request("GET", url, token=token, client_id=client_id, timeout=60.0)
    if r.status_code in (401, 403):
        raise SimklAPIError(
            f"SIMKL auth failed ({r.status_code}) — check SIMKL_ACCESS_TOKEN / CLIENT_ID",
            status=r.status_code,
            body=r.text[:400],
        )
    if not r.ok:
        raise SimklAPIError(
            f"SIMKL fetch {media_type} failed: HTTP {r.status_code} ({classify_status(r.status_code)})",
            status=r.status_code,
            body=r.text[:500],
        )
    try:
        data = r.json()
    except ValueError as e:
        raise SimklAPIError(
            f"SIMKL fetch {media_type}: invalid JSON ({e})",
            status=r.status_code,
            body=r.text[:300],
        ) from e
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("anime", "shows", "movies", media_type):
            if key in data and isinstance(data[key], list):
                return data[key]
        if not data:
            return []
        print(f"   warn: unexpected fetch keys={list(data)[:8]}")
    return []


def entry_ids(entry: dict[str, Any]) -> dict[str, Any]:
    show = entry.get("show") or entry.get("movie") or entry
    ids = entry.get("ids") or (show.get("ids") if isinstance(show, dict) else {}) or {}
    return dict(ids) if isinstance(ids, dict) else {}


def entry_title(entry: dict[str, Any]) -> str:
    show = entry.get("show") or entry.get("movie") or entry
    if isinstance(show, dict):
        return str(show.get("title") or entry.get("title") or "")
    return str(entry.get("title") or "")


def simkl_id(ids: dict[str, Any]) -> str | None:
    v = ids.get("simkl") or ids.get("simkl_id")
    return str(v) if v is not None else None


def classify(entries: list[dict[str, Any]]) -> tuple[list[dict], list[dict]]:
    rewatches = [e for e in entries if e.get("is_rewatch") is True]
    canonical = [e for e in entries if e.get("is_rewatch") is not True]
    return canonical, rewatches


def summarize(canonical: list[dict], rewatches: list[dict]) -> list[dict[str, Any]]:
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
            "anilist": ids.get("anilist") or ids.get("anilist_id"),
            "status": e.get("status") or e.get("rewatch_status"),
            "last_watched_at": e.get("last_watched_at") or e.get("last_updated_at"),
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
                "anilist": ids.get("anilist") or ids.get("anilist_id"),
                "status": None,
                "last_watched_at": None,
                "rewatch_sessions": [],
            }
        by_id[sid]["rewatch_sessions"].append({
            "rewatch_id": e.get("rewatch_id"),
            "rewatch_status": e.get("status") or e.get("rewatch_status"),
            "last_watched_at": e.get("last_watched_at") or e.get("last_updated_at"),
            "watched_episodes_count": e.get("watched_episodes_count") or e.get("watched_episodes"),
        })
    return [row for row in by_id.values() if row["rewatch_sessions"]]


def attempt_remove_session(
    client_id: str,
    token: str,
    media_type: str,
    simkl: str,
    rewatch_id: int | str,
    extra_ids: dict[str, Any] | None = None,
) -> tuple[int, str, str]:
    """Remove one rewatch session. Returns (status, body_snip, kind)."""
    ids: dict[str, Any] = {"simkl": int(simkl) if str(simkl).isdigit() else simkl}
    if extra_ids:
        for k in ("mal", "anilist", "anidb", "kitsu"):
            if extra_ids.get(k):
                ids[k] = extra_ids[k]
    body_item: dict[str, Any] = {
        "ids": ids,
        "is_rewatch": True,
        "rewatch_id": int(rewatch_id) if str(rewatch_id).isdigit() else rewatch_id,
    }
    key = "movies" if media_type == "movies" else "shows"
    payload = {key: [body_item]}
    url = f"{API}/sync/history/remove?{qs(client_id, allow_rewatch='yes')}"
    try:
        r = api_request(
            "POST",
            url,
            token=token,
            client_id=client_id,
            json_body=payload,
            timeout=30.0,
            max_retries=3,
        )
        return r.status_code, r.text[:500], classify_status(r.status_code)
    except SimklAPIError as e:
        return e.status or 0, str(e)[:500], e.kind


def write_reports(rows: list[dict[str, Any]], json_out: Path | None, csv_out: Path | None) -> None:
    if json_out:
        try:
            json_out.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
            print(f"Wrote {json_out}")
        except OSError as e:
            print(f"ERROR writing {json_out}: {e}")
    if csv_out:
        flat = []
        for row in rows:
            for sess in row["rewatch_sessions"]:
                flat.append({
                    "media_type": row.get("media_type") or "",
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
                })
        fields = [
            "media_type", "simkl", "title", "mal", "anilist", "canonical_status",
            "canonical_last_watched_at", "rewatch_id", "rewatch_status",
            "session_last_watched_at", "session_progress",
        ]
        try:
            with csv_out.open("w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                w.writerows(flat)
            print(f"Wrote {csv_out} ({len(flat)} session rows)")
        except OSError as e:
            print(f"ERROR writing {csv_out}: {e}")


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

    try:
        client_id, token = env_creds()
    except SystemExit as e:
        print(e)
        return 1

    types = ["anime", "shows", "movies"] if args.type == "all" else [args.type]
    all_rows: list[dict[str, Any]] = []
    fetch_errors = 0

    for media_type in types:
        print(f"-> Fetching SIMKL {media_type} with allow_rewatch=yes …")
        try:
            entries = fetch_all_items(client_id, token, media_type=media_type, status=args.status)
        except SimklAPIError as e:
            fetch_errors += 1
            print(f"   ERROR [{e.kind}] {e}")
            if e.body:
                print(f"   body: {e.body[:200]}")
            if e.kind == "auth":
                print("   Aborting remaining types — fix credentials and re-run.")
                return 1
            continue
        except Exception as e:
            fetch_errors += 1
            print(f"   ERROR unexpected on {media_type}: {type(e).__name__}: {e}")
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
    if fetch_errors:
        print(f"   ({fetch_errors} media type fetch(es) failed)")
    write_reports(all_rows, args.json_out, args.csv_out)

    if not all_rows:
        print("Nothing to clean up.")
        return 1 if fetch_errors and fetch_errors == len(types) else 0

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
    fail_kinds: dict[str, int] = {}
    for row in all_rows:
        media_type = row.get("media_type") or (types[0] if types else "anime")
        for sess in row["rewatch_sessions"]:
            rid = sess.get("rewatch_id")
            if rid is None:
                print(f"   skip {row['title']}: no rewatch_id")
                continue
            if args.limit and count >= args.limit:
                print(f"Reached --limit {args.limit}")
                break
            code, body, kind = attempt_remove_session(
                client_id,
                token,
                media_type=media_type,
                simkl=row["simkl"],
                rewatch_id=rid,
                extra_ids={"mal": row.get("mal"), "anilist": row.get("anilist")},
            )
            count += 1
            if 200 <= code < 300:
                removed += 1
                print(f"   removed [{media_type}] rewatch_id={rid} {row['title']!r} [{code}]")
            else:
                failed += 1
                fail_kinds[kind] = fail_kinds.get(kind, 0) + 1
                print(
                    f"   FAIL [{media_type}/{kind}] rewatch_id={rid} "
                    f"{row['title']!r} [{code}] {body[:120]}"
                )
                if kind == "auth":
                    print("   Stopping removals — auth error.")
                    print(f"Done. HTTP-success removals={removed} failed={failed} kinds={fail_kinds}")
                    return 1
                if kind == "rate_limit":
                    print("   Backing off 15s after rate limit …")
                    time.sleep(15)
            time.sleep(args.sleep)
        else:
            continue
        break  # limit reached

    print(f"Done. HTTP-success removals={removed} failed={failed}" + (f" kinds={fail_kinds}" if fail_kinds else ""))
    if failed:
        print(
            "Some removals failed — SIMKL may require website Clear All for those sessions. "
            "Canonical rows were never targeted without rewatch_id."
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
