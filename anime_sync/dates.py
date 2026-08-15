"""Watch-date helpers: parse, compare, and merge oldest started/completed dates."""
from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any


def parse_date(value: Any) -> date | None:
    """Parse FuzzyDate dict, ISO string, or YYYY-MM-DD into a date."""
    if value is None or value == "" or value is False:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, dict):
        y, m, d = value.get("year"), value.get("month"), value.get("day")
        if not y:
            return None
        try:
            return date(int(y), int(m or 1), int(d or 1))
        except (TypeError, ValueError):
            return None
    s = str(value).strip()
    if not s or s.startswith("0000"):
        return None
    try:
        if "T" in s or s.endswith("Z"):
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        # YYYY-MM-DD or YYYY-M-D
        parts = s[:10].split("-")
        if len(parts) >= 3:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
        if len(parts) == 1 and len(parts[0]) == 4:
            return date(int(parts[0]), 1, 1)
    except (TypeError, ValueError):
        return None
    return None


def to_iso_date(d: date | None) -> str | None:
    if not d:
        return None
    return d.isoformat()


def to_fuzzy(d: date | None) -> dict[str, int] | None:
    if not d:
        return None
    return {"year": d.year, "month": d.month, "day": d.day}


def to_mal_date(d: date | None) -> str | None:
    return to_iso_date(d)


def to_kitsu_dt(d: date | None) -> str | None:
    """Kitsu accepts ISO-8601 datetime; use noon UTC for day-only dates."""
    if not d:
        return None
    return datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def older(a: date | None, b: date | None) -> date | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if a <= b else b


def merge_platform_dates(existing: dict[str, Any] | None, platform: str, dates: dict[str, Any] | None) -> dict[str, Any]:
    """Merge one platform snapshot into entry dates blob; recompute oldest."""
    out = dict(existing or {})
    sources = dict(out.get("sources") or {})
    snap = {}
    dates = dates or {}
    for key in ("started_at", "completed_at", "last_watched_at"):
        parsed = parse_date(dates.get(key))
        if parsed:
            snap[key] = to_iso_date(parsed)
    if snap:
        sources[platform] = snap
    out["sources"] = sources

    started = None
    completed = None
    last_watched = None
    for src in sources.values():
        started = older(started, parse_date(src.get("started_at")))
        completed = older(completed, parse_date(src.get("completed_at")))
        last_watched = older(last_watched, parse_date(src.get("last_watched_at")))
    # last_watched can inform completed if no explicit completed
    if completed is None and last_watched is not None:
        # only use as completed hint when status is completed elsewhere — leave to caller
        pass
    out["started_at"] = to_iso_date(started)
    out["completed_at"] = to_iso_date(completed)
    out["last_watched_at"] = to_iso_date(last_watched)
    return out


def dates_need_push(platform_snap: dict[str, Any] | None, canonical: dict[str, Any] | None) -> bool:
    """True if platform is missing a date that canonical has, or platform date is newer than canonical oldest."""
    canonical = canonical or {}
    platform_snap = platform_snap or {}
    for key in ("started_at", "completed_at"):
        c = parse_date(canonical.get(key))
        p = parse_date(platform_snap.get(key))
        if c and (p is None or p > c):
            return True
    return False
