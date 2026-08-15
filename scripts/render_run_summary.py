#!/usr/bin/env python3
"""Build a mobile-friendly Markdown summary for GitHub Actions step summaries.

Reads whatever artifacts exist from a sync/enrich/cleanup/prune run and prints
a short "what happened" report to stdout (and optionally --out path).
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def _rows(path: Path) -> list[dict]:
    if not path.is_file():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    except Exception:
        return []


def coverage_section() -> list[str]:
    lines = ["## Coverage", ""]
    pairs = _rows(Path("anime_pairings.csv"))
    unmatched = _rows(Path("unmatched.csv"))
    if not pairs and not unmatched:
        lines.append("_No pairings/unmatched CSVs in this run._")
        lines.append("")
        return lines
    total = len(pairs)
    def has(col: str) -> int:
        return sum(1 for r in pairs if (r.get(col) or "").strip())
    lines.append("| Metric | Count |")
    lines.append("|--------|------:|")
    lines.append(f"| Total shows | **{total}** |")
    for label, col in [
        ("MAL", "mal_id"),
        ("AniList", "anilist_id"),
        ("Kitsu", "kitsu_id"),
        ("SIMKL", "simkl_id"),
        ("AniDB", "anidb_id"),
        ("IMDb", "imdb_id"),
        ("TVDB", "tvdb_id"),
    ]:
        if pairs and col in (pairs[0] or {}):
            lines.append(f"| {label} | {has(col)} |")
    # source column
    if pairs and "source" in pairs[0]:
        overrides = sum(1 for r in pairs if r.get("source") == "manual_override")
        lines.append(f"| Manual overrides | {overrides} |")
    lines.append(f"| Unmatched (need fix) | **{len(unmatched)}** |")
    lines.append("")
    if unmatched:
        lines.append("### Unmatched sample")
        lines.append("")
        lines.append("| Title | Key |")
        lines.append("|-------|-----|")
        for r in unmatched[:15]:
            title = (r.get("title") or "?").replace("|", "/")[:40]
            key = (r.get("canonical_key") or "").replace("|", "/")[:30]
            lines.append(f"| {title} | `{key}` |")
        if len(unmatched) > 15:
            lines.append("")
            lines.append(f"_…and {len(unmatched) - 15} more in unmatched.csv._")
        lines.append("")
    return lines


def push_section() -> list[str]:
    path = Path("show_report.md")
    if path.is_file() and path.stat().st_size > 0:
        text = path.read_text(encoding="utf-8").strip()
        if text:
            return [text, ""]
    rows = _rows(Path("push_report.csv"))
    if not rows:
        return ["## Changes this run", "", "_No push/change rows recorded._", ""]
    actions = Counter((r.get("action") or "?") for r in rows)
    lines = [
        "## Changes this run",
        "",
        f"**{len(rows)}** row(s) · "
        + " · ".join(f"`{a}` ×{n}" for a, n in actions.most_common()),
        "",
        "| Show | Platform | Action | Detail |",
        "|------|----------|--------|--------|",
    ]
    for r in rows[:60]:
        title = (r.get("title") or r.get("mal") or r.get("anilist") or "?").replace("|", "/")[:36]
        plat = r.get("platform") or ""
        act = r.get("action") or ""
        detail = (r.get("detail") or r.get("error") or "")[:50].replace("|", "/")
        lines.append(f"| {title} | `{plat}` | **{act}** | {detail} |")
    if len(rows) > 60:
        lines.append("")
        lines.append(f"_…and {len(rows) - 60} more in push_report.csv._")
    lines.append("")
    return lines


def conflict_section() -> list[str]:
    rows = _rows(Path("conflict_report.csv"))
    if not rows:
        return []
    accepted = sum(1 for r in rows if str(r.get("accepted")).lower() in ("true", "1", "yes"))
    lines = [
        "## Conflicts",
        "",
        f"**{len(rows)}** disagreement(s) · accepted incoming: **{accepted}** · kept stored: **{len(rows) - accepted}**",
        "",
        "| Title | From | Accepted? | Reason |",
        "|-------|------|-----------|--------|",
    ]
    for r in rows[:25]:
        title = (r.get("title") or r.get("key") or "?")[:36].replace("|", "/")
        plat = r.get("incoming_platform") or ""
        acc = r.get("accepted") or ""
        reason = (r.get("reason") or "")[:40].replace("|", "/")
        lines.append(f"| {title} | `{plat}` | {acc} | {reason} |")
    if len(rows) > 25:
        lines.append("")
        lines.append(f"_…and {len(rows) - 25} more in conflict_report.csv._")
    lines.append("")
    return lines


def simkl_section() -> list[str]:
    rows = _rows(Path("simkl_rewatches.csv"))
    if not rows:
        # still mention if json exists empty
        if Path("simkl_rewatches.json").is_file():
            return ["## SIMKL rewatches", "", "_No extra rewatch sessions found._", ""]
        return []
    by_media = Counter((r.get("media_type") or "?") for r in rows)
    lines = [
        "## SIMKL rewatch sessions",
        "",
        f"**{len(rows)}** extra session(s) · "
        + " · ".join(f"`{k}` ×{v}" for k, v in by_media.most_common()),
        "",
        "| Media | SIMKL | Title | Rewatch ID | Last watched |",
        "|-------|------:|-------|------------|--------------|",
    ]
    for r in rows[:40]:
        media = r.get("media_type") or ""
        simkl = r.get("simkl") or ""
        title = (r.get("title") or "?")[:32].replace("|", "/")
        rid = r.get("rewatch_id") or ""
        lw = (r.get("session_last_watched_at") or "")[:16]
        lines.append(f"| {media} | {simkl} | {title} | `{rid}` | {lw} |")
    if len(rows) > 40:
        lines.append("")
        lines.append(f"_…and {len(rows) - 40} more in simkl_rewatches.csv._")
    lines.append("")
    return lines


def prune_section() -> list[str]:
    # prune writes only to log; optional prune_report.json
    p = Path("prune_report.json")
    if not p.is_file():
        return []
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    lines = ["## Prune result", ""]
    for k in ("deleted_runs", "deleted_artifacts", "deleted_caches", "dry_run", "days"):
        if k in data:
            lines.append(f"- **{k}**: {data[k]}")
    lines.append("")
    return lines


def job_summary_section() -> list[str]:
    p = Path("job_summary.md")
    if p.is_file() and p.stat().st_size > 0:
        text = p.read_text(encoding="utf-8").strip()
        if text and not text.startswith("## Show report"):
            return [text, ""]
    return []


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--title", default="Run summary")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()

    lines = [f"# {args.title}", ""]
    lines += coverage_section()
    lines += push_section()
    lines += conflict_section()
    lines += simkl_section()
    lines += prune_section()
    # Avoid duplicating show report if already included via push_section
    js = job_summary_section()
    if js:
        lines.append("## Job details")
        lines.append("")
        lines += js

    text = "\n".join(lines).rstrip() + "\n"
    print(text)
    if args.out:
        args.out.write_text(text, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
