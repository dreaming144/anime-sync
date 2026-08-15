"""Watch-date helpers: parse, validate, compare, and merge oldest started/completed dates."""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

log = logging.getLogger("anime_sync.dates")

# Reasonable bounds for user watch dates (not series air dates from antiquity)
_MIN_YEAR = 1950
_MAX_YEAR = 2100


class InvalidDateError(ValueError):
    """Raised when a date value cannot be used for watch-history sync."""


def _in_range(d: date) -> bool:
    return _MIN_YEAR <= d.year <= _MAX_YEAR


def parse_date(value: Any, *, strict: bool = False, context: str | None = None) -> date | None:
    """Parse FuzzyDate dict, ISO string, or YYYY-MM-DD into a date.

    Returns None for missing/empty/unparseable values unless strict=True,
    in which case InvalidDateError is raised for non-empty invalid input.
    Out-of-range years and impossible calendar days are treated as invalid.
    """
    if value is None or value == "" or value is False:
        return None

    label = context or "date"
    parsed: date | None = None
    raw_repr = value

    try:
        if isinstance(value, datetime):
            parsed = value.date()
        elif isinstance(value, date):
            parsed = value
        elif isinstance(value, dict):
            y, m, d = value.get("year"), value.get("month"), value.get("day")
            if not y:
                return None
            y_i = int(y)
            m_i = int(m) if m not in (None, "", 0) else 1
            d_i = int(d) if d not in (None, "", 0) else 1
            if m_i < 1 or m_i > 12 or d_i < 1 or d_i > 31:
                raise ValueError(f"component out of range y={y_i} m={m_i} d={d_i}")
            parsed = date(y_i, m_i, d_i)
        else:
            s = str(value).strip()
            if not s or s.startswith("0000") or s.lower() in ("null", "none", "undefined"):
                return None
            # Reject obvious garbage
            if s in ("0", "1", "true", "false"):
                raise ValueError(f"not a calendar date: {s!r}")
            if "T" in s or s.endswith("Z") or "+" in s[10:]:
                parsed = datetime.fromisoformat(s.replace("Z", "+00:00")).date()
            else:
                # YYYY-MM-DD or YYYY-M-D (MAL sometimes omits zero padding)
                head = s[:10]
                parts = head.split("-")
                if len(parts) >= 3:
                    y_i, m_i, d_i = int(parts[0]), int(parts[1]), int(parts[2])
                    parsed = date(y_i, m_i, d_i)
                elif len(parts) == 1 and len(parts[0]) == 4 and parts[0].isdigit():
                    parsed = date(int(parts[0]), 1, 1)
                else:
                    raise ValueError(f"unrecognized format: {s!r}")
    except (TypeError, ValueError, OverflowError) as e:
        msg = f"invalid {label}: {raw_repr!r} ({e})"
        log.warning(msg)
        if strict:
            raise InvalidDateError(msg) from e
        return None

    if parsed is not None and not _in_range(parsed):
        msg = f"invalid {label}: {parsed.isoformat()} out of range {_MIN_YEAR}-{_MAX_YEAR}"
        log.warning(msg)
        if strict:
            raise InvalidDateError(msg)
        return None

    return parsed


def safe_parse_date(value: Any, context: str | None = None) -> date | None:
    """Alias for parse_date(..., strict=False) — never raises."""
    try:
        return parse_date(value, strict=False, context=context)
    except Exception as e:  # noqa: BLE001 — absolute last resort
        log.warning("safe_parse_date unexpected error for %r: %s", value, e)
        return None


def validate_date_pair(
    started: date | None,
    completed: date | None,
    *,
    context: str | None = None,
) -> tuple[date | None, date | None]:
    """Ensure started <= completed when both present; drop the inconsistent one.

    Prefer keeping the older (started) and clearing completed if inverted.
    """
    if started and completed and started > completed:
        label = context or "date pair"
        log.warning(
            "invalid %s: started_at %s is after completed_at %s — clearing completed_at",
            label,
            started.isoformat(),
            completed.isoformat(),
        )
        return started, None
    return started, completed


def to_iso_date(d: date | None) -> str | None:
    if not d or not isinstance(d, date):
        return None
    if not _in_range(d):
        log.warning("to_iso_date rejected out-of-range %s", d)
        return None
    return d.isoformat()


def to_fuzzy(d: date | None) -> dict[str, int] | None:
    if not d or not isinstance(d, date) or not _in_range(d):
        return None
    return {"year": d.year, "month": d.month, "day": d.day}


def to_mal_date(d: date | None) -> str | None:
    """MAL v2 expects year-month-day; prefer zero-padded YYYY-MM-DD."""
    if not d or not isinstance(d, date) or not _in_range(d):
        return None
    return f"{d.year:04d}-{d.month:02d}-{d.day:02d}"


def to_kitsu_dt(d: date | None) -> str | None:
    """Kitsu accepts ISO-8601 datetime; use noon UTC for day-only dates."""
    if not d or not isinstance(d, date) or not _in_range(d):
        return None
    return (
        datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def older(a: date | None, b: date | None) -> date | None:
    if a is None:
        return b
    if b is None:
        return a
    return a if a <= b else b


def merge_platform_dates(
    existing: dict[str, Any] | None,
    platform: str,
    dates: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge one platform snapshot into entry dates blob; recompute oldest.

    Invalid dates from a platform are skipped (logged) and do not overwrite
    previously known good values.
    """
    out = dict(existing or {})
    sources = dict(out.get("sources") or {})
    snap: dict[str, str] = {}
    dates = dates or {}
    for key in ("started_at", "completed_at", "last_watched_at"):
        parsed = safe_parse_date(dates.get(key), context=f"{platform}.{key}")
        if parsed:
            snap[key] = to_iso_date(parsed)  # type: ignore[assignment]
    if snap:
        # Validate pair within this platform snapshot
        s, c = validate_date_pair(
            safe_parse_date(snap.get("started_at")),
            safe_parse_date(snap.get("completed_at")),
            context=f"{platform}",
        )
        cleaned: dict[str, str] = {}
        if s:
            cleaned["started_at"] = to_iso_date(s)  # type: ignore[assignment]
        if c:
            cleaned["completed_at"] = to_iso_date(c)  # type: ignore[assignment]
        if snap.get("last_watched_at"):
            cleaned["last_watched_at"] = snap["last_watched_at"]
        if cleaned:
            sources[platform] = cleaned
    out["sources"] = sources

    started = None
    completed = None
    last_watched = None
    for src_name, src in sources.items():
        started = older(started, safe_parse_date(src.get("started_at"), context=f"{src_name}.started_at"))
        completed = older(
            completed, safe_parse_date(src.get("completed_at"), context=f"{src_name}.completed_at")
        )
        last_watched = older(
            last_watched,
            safe_parse_date(src.get("last_watched_at"), context=f"{src_name}.last_watched_at"),
        )
    started, completed = validate_date_pair(started, completed, context="canonical")

    out["started_at"] = to_iso_date(started)
    out["completed_at"] = to_iso_date(completed)
    out["last_watched_at"] = to_iso_date(last_watched)
    return out


def dates_need_push(platform_snap: dict[str, Any] | None, canonical: dict[str, Any] | None) -> bool:
    """True if platform is missing a date that canonical has, or platform date is newer."""
    canonical = canonical or {}
    platform_snap = platform_snap or {}
    for key in ("started_at", "completed_at"):
        c = safe_parse_date(canonical.get(key), context=f"canonical.{key}")
        p = safe_parse_date(platform_snap.get(key), context=f"platform.{key}")
        if c and (p is None or p > c):
            return True
    return False


def sanitize_dates_for_push(dates: dict[str, Any] | None) -> dict[str, str]:
    """Return only valid ISO dates suitable for platform pushers.

    Drops invalid values and inverted pairs so pushers never send garbage.
    """
    dates = dates or {}
    started = safe_parse_date(dates.get("started_at"), context="push.started_at")
    completed = safe_parse_date(dates.get("completed_at"), context="push.completed_at")
    started, completed = validate_date_pair(started, completed, context="push")
    out: dict[str, str] = {}
    if started:
        out["started_at"] = to_iso_date(started)  # type: ignore[assignment]
    if completed:
        out["completed_at"] = to_iso_date(completed)  # type: ignore[assignment]
    lw = safe_parse_date(dates.get("last_watched_at"), context="push.last_watched_at")
    if lw:
        out["last_watched_at"] = to_iso_date(lw)  # type: ignore[assignment]
    return out
