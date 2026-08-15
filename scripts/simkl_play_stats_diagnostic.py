#!/usr/bin/env python3
"""Read-only SIMKL play-stat diagnostic.

This utility only issues GET requests to ``/sync/all-items``.  It never calls
SIMKL write endpoints and cannot alter watch history, rewatch sessions, list
status, ratings, or dates.

The report separates canonical library rows from rows where ``is_rewatch`` is
true.  Use ``--title-contains`` for a title visible in SIMKL's profile play
stats: if no matching rewatch row is returned, the apparent excess plays may
instead be stored in canonical episode history and require review in SIMKL's
website Watch History UI.

Examples:
  SIMKL_CLIENT_ID=... SIMKL_ACCESS_TOKEN=... \
    python scripts/simkl_play_stats_diagnostic.py --type anime

  ... python scripts/simkl_play_stats_diagnostic.py \
    --title-contains "Cowboy Bebop" --date-from 2000-01-01
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

API = "https://api.simkl.com"
APP_NAME = "anime-sync-play-stat-diagnostic"
APP_VERSION = "1.0"
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


class SimklDiagnosticError(RuntimeError):
    """Raised when the read-only SIMKL diagnostic cannot complete."""


def credentials() -> tuple[str, str]:
    """Return required credentials without ever echoing their values."""
    client_id = os.getenv("SIMKL_CLIENT_ID", "").strip()
    token = os.getenv("SIMKL_ACCESS_TOKEN", "").strip()
    if not client_id or not token:
        raise SimklDiagnosticError(
            "SIMKL_CLIENT_ID and SIMKL_ACCESS_TOKEN must both be available in the environment."
        )
    return client_id, token


def make_headers(token: str, client_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "simkl-api-key": client_id,
        "Accept": "application/json",
        "User-Agent": f"{APP_NAME}/{APP_VERSION}",
    }


def _retry_delay(response: requests.Response | None, attempt: int) -> float:
    if response is not None:
        try:
            retry_after = float(response.headers.get("Retry-After", ""))
            if retry_after > 0:
                return min(retry_after, 60.0)
        except (TypeError, ValueError):
            pass
    return min(30.0, (2**attempt) + 0.5)


def fetch_all_items(
    session: requests.Session,
    *,
    client_id: str,
    token: str,
    media_type: str,
    date_from: str,
    max_retries: int = 4,
) -> list[dict[str, Any]]:
    """Fetch canonical and rewatch rows using a GET-only SIMKL request.

    ``date_from`` is intentionally always supplied.  SIMKL documents it as a
    required companion to ``allow_rewatch=yes`` outside initial synchronization.
    The request uses ``extended=full`` and ``episode_watched_at=yes`` so any
    rewatch session rows carry useful session-level evidence, while the report
    only writes a compact summary to disk.
    """
    if media_type not in {"anime", "shows", "movies"}:
        raise ValueError(f"Unsupported SIMKL media type: {media_type}")
    if not date_from.strip():
        raise ValueError("--date-from must not be blank")

    url = f"{API}/sync/all-items/{media_type}"
    params = {
        "client_id": client_id,
        "app-name": APP_NAME,
        "app-version": APP_VERSION,
        "date_from": date_from,
        "allow_rewatch": "yes",
        "extended": "full",
        "episode_watched_at": "yes",
    }
    last_error = ""
    for attempt in range(max_retries):
        response: requests.Response | None = None
        try:
            # Deliberately use GET rather than a generic request method: this
            # diagnostic must remain incapable of issuing a write request.
            response = session.get(
                url,
                params=params,
                headers=make_headers(token, client_id),
                timeout=60,
            )
        except requests.RequestException as exc:
            last_error = f"network error: {type(exc).__name__}: {exc}"
        else:
            if response.ok:
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise SimklDiagnosticError(
                        f"SIMKL returned invalid JSON for {media_type}: {exc}"
                    ) from exc
                if isinstance(payload, list):
                    return [row for row in payload if isinstance(row, dict)]
                if isinstance(payload, dict):
                    for key in (media_type, "anime", "shows", "movies"):
                        rows = payload.get(key)
                        if isinstance(rows, list):
                            return [row for row in rows if isinstance(row, dict)]
                    if not payload:
                        return []
                raise SimklDiagnosticError(
                    f"Unexpected SIMKL response shape for {media_type}: "
                    f"{type(payload).__name__}"
                )
            last_error = f"HTTP {response.status_code}: {response.text[:300]}"
            if response.status_code not in RETRYABLE_STATUS_CODES:
                raise SimklDiagnosticError(f"SIMKL diagnostic request failed ({last_error})")

        if attempt < max_retries - 1:
            delay = _retry_delay(response, attempt)
            print(f"   SIMKL read retry in {delay:.1f}s ({last_error[:120]})")
            time.sleep(delay)

    raise SimklDiagnosticError(
        f"SIMKL diagnostic request failed after {max_retries} GET attempts ({last_error})"
    )


def _show_object(entry: dict[str, Any]) -> dict[str, Any]:
    show = entry.get("show") or entry.get("movie") or entry
    return show if isinstance(show, dict) else entry


def entry_ids(entry: dict[str, Any]) -> dict[str, Any]:
    show = _show_object(entry)
    ids = entry.get("ids") or show.get("ids") or {}
    return ids if isinstance(ids, dict) else {}


def entry_simkl_id(entry: dict[str, Any]) -> str:
    ids = entry_ids(entry)
    value = ids.get("simkl") or ids.get("simkl_id")
    if value is not None:
        return str(value)
    # Keep unidentifiable rows separate and deterministic rather than merging
    # unrelated titles into one bucket.
    return f"unknown:{entry_title(entry).casefold()}"


def entry_title(entry: dict[str, Any]) -> str:
    show = _show_object(entry)
    return str(show.get("title") or entry.get("title") or "Untitled")


def entry_progress(entry: dict[str, Any]) -> int | str:
    value = entry.get("watched_episodes_count")
    if value is None:
        value = entry.get("watched_episodes")
    return value if value is not None else ""


def entry_last_watched(entry: dict[str, Any]) -> str:
    return str(entry.get("last_watched_at") or entry.get("last_updated_at") or "")


def title_is_selected(title: str, terms: list[str]) -> bool:
    return not terms or any(term.casefold() in title.casefold() for term in terms)


def session_summary(entry: dict[str, Any]) -> dict[str, str | int]:
    return {
        "rewatch_id": str(entry.get("rewatch_id") or ""),
        "status": str(entry.get("rewatch_status") or entry.get("status") or ""),
        "progress": entry_progress(entry),
        "last_watched_at": entry_last_watched(entry),
    }


def build_rows(
    entries: list[dict[str, Any]], *, media_type: str, title_terms: list[str]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Group canonical and rewatch rows into title-level diagnostic findings."""
    grouped: dict[str, dict[str, Any]] = {}
    totals = {"canonical": 0, "rewatch": 0, "selected": 0}

    for entry in entries:
        title = entry_title(entry)
        if not title_is_selected(title, title_terms):
            continue
        totals["selected"] += 1
        key = entry_simkl_id(entry)
        ids = entry_ids(entry)
        row = grouped.setdefault(
            key,
            {
                "media_type": media_type,
                "simkl": "" if key.startswith("unknown:") else key,
                "mal": ids.get("mal") or "",
                "anilist": ids.get("anilist") or ids.get("anilist_id") or "",
                "title": title,
                "canonical_status": "",
                "canonical_progress": "",
                "canonical_last_watched_at": "",
                "rewatch_sessions": [],
            },
        )
        if entry.get("is_rewatch") is True:
            totals["rewatch"] += 1
            row["rewatch_sessions"].append(session_summary(entry))
        else:
            totals["canonical"] += 1
            # SIMKL normally returns one canonical row.  Prefer the most recent
            # value if an unexpected duplicate is present.
            if (
                not row["canonical_last_watched_at"]
                or entry_last_watched(entry) >= row["canonical_last_watched_at"]
            ):
                row["canonical_status"] = str(entry.get("status") or "")
                row["canonical_progress"] = entry_progress(entry)
                row["canonical_last_watched_at"] = entry_last_watched(entry)

    rows: list[dict[str, Any]] = []
    for row in grouped.values():
        sessions = sorted(
            row.pop("rewatch_sessions"),
            key=lambda value: (str(value["last_watched_at"]), str(value["rewatch_id"])),
            reverse=True,
        )
        row["rewatch_session_count"] = len(sessions)
        row["rewatch_sessions"] = sessions
        if sessions:
            row["diagnosis"] = (
                "SIMKL returned explicit rewatch-session row(s). These sessions are separate "
                "from the canonical watch history and can contribute to repeat-play indicators."
            )
        elif title_terms:
            row["diagnosis"] = (
                "No explicit SIMKL rewatch-session row was returned for this title. Apparent "
                "extra plays may instead be stored in canonical episode history; review the "
                "title's Watch History in SIMKL before making any deletion."
            )
        else:
            # An unfiltered report is intentionally rewatch-focused, so omit
            # ordinary canonical-only titles to keep its summary readable.
            continue
        rows.append(row)

    rows.sort(key=lambda row: (-int(row["rewatch_session_count"]), str(row["title"]).casefold()))
    return rows, totals


def _markdown_cell(value: Any) -> str:
    return str(value or "—").replace("|", "\\|").replace("\n", " ")


def write_reports(
    rows: list[dict[str, Any]],
    *,
    media_type: str,
    date_from: str,
    title_terms: list[str],
    totals: dict[str, int],
    markdown_out: Path,
    csv_out: Path,
    json_out: Path,
) -> None:
    """Write compact, human-readable output without saving raw watch events."""
    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    markdown_lines = [
        "# SIMKL Play-Stat Diagnostic — Read Only",
        "",
        "> This report was generated using GET `/sync/all-items` only. It made no changes to SIMKL list status, canonical history, or rewatch sessions.",
        "",
        f"- Generated: `{generated_at}`",
        f"- Media: `{media_type}`",
        f"- Rewatch query start (`date_from`): `{date_from}`",
        f"- Title filter: `{', '.join(title_terms) if title_terms else 'none — rewatch sessions only'}`",
        f"- Returned selected rows: `{totals['selected']}` (canonical `{totals['canonical']}`, rewatch `{totals['rewatch']}`)",
        "",
        "## Findings",
        "",
    ]
    if rows:
        markdown_lines += [
            "| Title | Canonical status / progress | Rewatch sessions | Assessment |",
            "| --- | --- | ---: | --- |",
        ]
        for row in rows:
            canonical = f"{row['canonical_status'] or '—'} / {row['canonical_progress'] or '—'}"
            markdown_lines.append(
                "| "
                + " | ".join(
                    [
                        _markdown_cell(row["title"]),
                        _markdown_cell(canonical),
                        _markdown_cell(row["rewatch_session_count"]),
                        _markdown_cell(row["diagnosis"]),
                    ]
                )
                + " |"
            )
        markdown_lines += ["", "## Session evidence", ""]
        for row in rows:
            if not row["rewatch_sessions"]:
                continue
            markdown_lines += [
                f"### {row['title']}",
                "",
                "| Session ID | Status | Progress | Last watched |",
                "| --- | --- | ---: | --- |",
            ]
            for session in row["rewatch_sessions"]:
                markdown_lines.append(
                    "| "
                    + " | ".join(
                        _markdown_cell(session[key])
                        for key in ("rewatch_id", "status", "progress", "last_watched_at")
                    )
                    + " |"
                )
            markdown_lines.append("")
    else:
        markdown_lines += [
            "No matching rewatch-session findings were returned.",
            "",
            "If you supplied a title filter, the absence of a rewatch row does not prove that a profile play count is wrong. It indicates that the SIMKL API did not return a separate `is_rewatch=true` session for the matching title in this query window.",
            "",
        ]

    markdown_lines += [
        "## Interpretation",
        "",
        "A separate `is_rewatch=true` row is evidence of an explicit SIMKL rewatch session. A matching canonical-only row is not a safe basis for deletion: inflated profile plays can also arise from ordinary episode-history events. Use SIMKL's title-level Watch History tools for any manual inspection or cleanup.",
        "",
    ]
    markdown_out.write_text("\n".join(markdown_lines), encoding="utf-8")

    csv_fields = [
        "media_type",
        "simkl",
        "mal",
        "anilist",
        "title",
        "canonical_status",
        "canonical_progress",
        "canonical_last_watched_at",
        "rewatch_session_count",
        "rewatch_id",
        "rewatch_status",
        "rewatch_progress",
        "rewatch_last_watched_at",
        "diagnosis",
    ]
    with csv_out.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=csv_fields)
        writer.writeheader()
        for row in rows:
            sessions = row["rewatch_sessions"] or [{}]
            for session in sessions:
                writer.writerow(
                    {
                        "media_type": row["media_type"],
                        "simkl": row["simkl"],
                        "mal": row["mal"],
                        "anilist": row["anilist"],
                        "title": row["title"],
                        "canonical_status": row["canonical_status"],
                        "canonical_progress": row["canonical_progress"],
                        "canonical_last_watched_at": row["canonical_last_watched_at"],
                        "rewatch_session_count": row["rewatch_session_count"],
                        "rewatch_id": session.get("rewatch_id", ""),
                        "rewatch_status": session.get("status", ""),
                        "rewatch_progress": session.get("progress", ""),
                        "rewatch_last_watched_at": session.get("last_watched_at", ""),
                        "diagnosis": row["diagnosis"],
                    }
                )
    json_out.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only SIMKL rewatch/play-stat diagnostic")
    parser.add_argument("--type", choices=["anime", "shows", "movies"], default="anime")
    parser.add_argument(
        "--date-from",
        default="2000-01-01",
        help="Inclusive SIMKL history query start; always sent with allow_rewatch=yes (default: 2000-01-01).",
    )
    parser.add_argument(
        "--title-contains",
        action="append",
        default=[],
        help="Case-insensitive title substring to diagnose; repeat for multiple titles.",
    )
    parser.add_argument("--md-out", type=Path, default=Path("simkl_play_stats.md"))
    parser.add_argument("--csv-out", type=Path, default=Path("simkl_play_stats.csv"))
    parser.add_argument("--json-out", type=Path, default=Path("simkl_play_stats.json"))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    terms = [term.strip() for term in args.title_contains if term.strip()]
    try:
        client_id, token = credentials()
        with requests.Session() as session:
            print(f"-> Reading SIMKL {args.type} library with rewatch sessions (GET only) …")
            entries = fetch_all_items(
                session,
                client_id=client_id,
                token=token,
                media_type=args.type,
                date_from=args.date_from,
            )
        rows, totals = build_rows(entries, media_type=args.type, title_terms=terms)
        write_reports(
            rows,
            media_type=args.type,
            date_from=args.date_from,
            title_terms=terms,
            totals=totals,
            markdown_out=args.md_out,
            csv_out=args.csv_out,
            json_out=args.json_out,
        )
    except (SimklDiagnosticError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(
        f"   Received {len(entries)} row(s); canonical={totals['canonical']}, "
        f"rewatch={totals['rewatch']}, findings={len(rows)}"
    )
    print(f"   Wrote {args.md_out}, {args.csv_out}, {args.json_out}")
    print("   Read-only diagnostic complete: no SIMKL write endpoint was called.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
