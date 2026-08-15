"""Watch-history and library statistics from the local sync DB."""
from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _parse_year(entry: dict[str, Any]) -> int | None:
    y = entry.get("year")
    if y is not None and str(y).strip() != "":
        try:
            return int(y)
        except (TypeError, ValueError):
            pass
    for key in ("last_updated",):
        raw = entry.get(key)
        if not raw:
            continue
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).year
        except Exception:
            continue
    return None


def compute_watch_stats(entries: dict[str, Any] | None) -> dict[str, Any]:
    """Aggregate library / watch-history style stats from sync DB entries."""
    entries = entries or {}
    status_counts: Counter[str] = Counter()
    score_bucket: Counter[str] = Counter()  # 0, 1-4, 5-6, 7-8, 9-10
    format_counts: Counter[str] = Counter()
    season_counts: Counter[str] = Counter()
    year_counts: Counter[str] = Counter()
    id_coverage: Counter[str] = Counter()
    platform_synced: Counter[str] = Counter()
    progress_watching = []
    scored = 0
    total_eps_logged = 0
    media_types: Counter[str] = Counter()

    for key, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        state = entry.get("state") or {}
        status = str(state.get("status") or "unknown")
        status_counts[status] += 1
        progress = _safe_int(state.get("progress"), 0)
        score = _safe_int(state.get("score"), 0)
        if score > 0:
            scored += 1
            if score >= 9:
                score_bucket["9-10"] += 1
            elif score >= 7:
                score_bucket["7-8"] += 1
            elif score >= 5:
                score_bucket["5-6"] += 1
            else:
                score_bucket["1-4"] += 1
        else:
            score_bucket["unrated"] += 1

        if status == "watching":
            progress_watching.append(
                {
                    "key": key,
                    "title": entry.get("title")
                    or (entry.get("ids") or {}).get("title")
                    or key,
                    "progress": progress,
                    "episodes": entry.get("episodes"),
                }
            )

        total_eps_logged += progress

        fmt = entry.get("format") or (entry.get("ids") or {}).get("format") or "unknown"
        format_counts[str(fmt)] += 1
        season = entry.get("season") or "unknown"
        season_counts[str(season)] += 1
        mt = entry.get("media_type") or "anime"
        media_types[str(mt)] += 1

        y = _parse_year(entry)
        if y:
            year_counts[str(y)] += 1

        ids = entry.get("ids") or {}
        for field in ("mal", "anilist", "kitsu", "simkl", "imdb", "tvdb", "tmdb", "anidb"):
            if ids.get(field):
                id_coverage[field] += 1

        for plat in (entry.get("last_synced") or {}):
            platform_synced[plat] += 1

    completed = status_counts.get("completed", 0)
    watching = status_counts.get("watching", 0)
    ptw = status_counts.get("plantowatch", 0)
    dropped = status_counts.get("dropped", 0)
    on_hold = status_counts.get("on_hold", 0)

    progress_watching.sort(key=lambda x: (-x["progress"], x["title"]))

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "entries": len(entries),
            "completed": completed,
            "watching": watching,
            "plantowatch": ptw,
            "dropped": dropped,
            "on_hold": on_hold,
            "scored": scored,
            "episodes_progress_sum": total_eps_logged,
            "completion_rate": round(completed / len(entries), 4) if entries else 0.0,
        },
        "by_status": dict(status_counts.most_common()),
        "by_score_bucket": dict(score_bucket),
        "by_format": dict(format_counts.most_common()),
        "by_season": dict(season_counts.most_common(12)),
        "by_year": dict(sorted(year_counts.items(), key=lambda x: x[0])),
        "by_media_type": dict(media_types.most_common()),
        "id_coverage": dict(id_coverage),
        "platform_last_synced": dict(platform_synced),
        "currently_watching": progress_watching[:40],
    }


def stats_to_markdown(stats: dict[str, Any]) -> str:
    t = stats.get("totals") or {}
    lines = [
        "# Watch history stats",
        "",
        f"_Generated {stats.get('generated_at', '')}_",
        "",
        "## Totals",
        "",
        f"| Metric | Value |",
        f"|--------|------:|",
        f"| Entries | {t.get('entries', 0)} |",
        f"| Completed | {t.get('completed', 0)} |",
        f"| Watching | {t.get('watching', 0)} |",
        f"| Plan to watch | {t.get('plantowatch', 0)} |",
        f"| Dropped | {t.get('dropped', 0)} |",
        f"| On hold | {t.get('on_hold', 0)} |",
        f"| Scored (score>0) | {t.get('scored', 0)} |",
        f"| Σ episode progress | {t.get('episodes_progress_sum', 0)} |",
        f"| Completion rate | {float(t.get('completion_rate') or 0)*100:.1f}% |",
        "",
        "## By status",
        "",
    ]
    for k, v in (stats.get("by_status") or {}).items():
        lines.append(f"- **{k}**: {v}")
    lines += ["", "## Score buckets", ""]
    for k, v in (stats.get("by_score_bucket") or {}).items():
        lines.append(f"- **{k}**: {v}")
    lines += ["", "## ID coverage", ""]
    for k, v in (stats.get("id_coverage") or {}).items():
        lines.append(f"- **{k}**: {v}")
    lines += ["", "## Platforms with last_synced", ""]
    for k, v in (stats.get("platform_last_synced") or {}).items():
        lines.append(f"- **{k}**: {v}")
    watching = stats.get("currently_watching") or []
    if watching:
        lines += ["", "## Currently watching (top by progress)", ""]
        for w in watching[:20]:
            eps = w.get("episodes")
            prog = w.get("progress")
            suffix = f" / {eps}" if eps else ""
            lines.append(f"- {w.get('title')}: **{prog}{suffix}**")
    years = stats.get("by_year") or {}
    if years:
        lines += ["", "## By year (metadata)", ""]
        # show densest years
        top = sorted(years.items(), key=lambda x: -int(x[1]))[:15]
        for y, n in top:
            lines.append(f"- **{y}**: {n}")
    lines.append("")
    return "\n".join(lines)


def write_watch_stats(
    entries: dict[str, Any] | None,
    json_path: str | Path = "watch_history_stats.json",
    md_path: str | Path = "watch_history_stats.md",
) -> dict[str, Any]:
    stats = compute_watch_stats(entries)
    Path(json_path).write_text(json.dumps(stats, indent=2, ensure_ascii=False), encoding="utf-8")
    Path(md_path).write_text(stats_to_markdown(stats), encoding="utf-8")
    print(f"   Watch stats: {stats['totals'].get('entries')} entries → {json_path}, {md_path}")
    return stats
