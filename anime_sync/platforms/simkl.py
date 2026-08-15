"""SIMKL REST loader and pusher."""
import os
from datetime import datetime, timezone

from anime_sync.http import request_with_retries, CircuitOpenError
from anime_sync.platforms.common import _push_skip_logged
from anime_sync.platforms.status import REVERSE_STATUS, STATUS_MAP

import requests


def load_simkl():
    client_id = os.getenv("SIMKL_CLIENT_ID")
    token = os.getenv("SIMKL_ACCESS_TOKEN")
    if not client_id or not token:
        print("-> SIMKL skipped (no secrets)")
        return []
    print("-> Fetching SIMKL...")
    headers = {"Authorization": f"Bearer {token}", "simkl-api-key": client_id}
    try:
        r = request_with_retries(
            "GET",
            f"https://api.simkl.com/sync/all-items/anime?client_id={client_id}",
            headers=headers,
            timeout=30,
        )
    except CircuitOpenError as e:
        print(f"-> SIMKL skipped: {e}")
        return []
    if not r.ok:
        print(f"   SIMKL error {r.status_code}")
        return []
    data = r.json()
    raw_list = data if isinstance(data, list) else data.get("anime", []) or data.get("shows", []) or []
    items = []
    for entry in raw_list:
        show_obj = entry.get("show", entry) if isinstance(entry, dict) else {}
        ids_obj = entry.get("ids") or show_obj.get("ids") or {}
        if not ids_obj:
            continue
        last = entry.get("last_watched_at") or entry.get("last_updated_at") or show_obj.get("last_watched_at")
        status_raw = entry.get("status") or show_obj.get("status") or "plantowatch"
        title = show_obj.get("title") or ""
        items.append({
            "platform": "simkl",
            "ids": {
                "simkl": ids_obj.get("simkl") or ids_obj.get("simkl_id"),
                "mal": ids_obj.get("mal"),
                "anilist": ids_obj.get("anilist") or ids_obj.get("anilist_id"),
                "anidb": ids_obj.get("anidb"),
                "title": title,
            },
            "state": {
                "status": STATUS_MAP["simkl"].get(status_raw, "plantowatch"),
                "progress": entry.get("watched_episodes_count")
                or entry.get("watched_episodes")
                or show_obj.get("watched_episodes_count", 0),
                "score": entry.get("user_rating") or show_obj.get("user_rating") or 0,
            },
            "dates": {
                "last_watched_at": last,
                # SIMKL list rows don't expose started_at; use last_watched as completed hint when completed
                "completed_at": last if STATUS_MAP["simkl"].get(status_raw) == "completed" else None,
            },
            "updated": datetime.fromisoformat(last.replace("Z", "+00:00")) if last else datetime.now(timezone.utc),
            "title": title,
        })
    print(f"   SIMKL: {len(items)} entries")
    return items


def push_simkl(entry, state, dates=None):
    """Push list status to SIMKL only. Never create rewatch sessions by default.

    SIMKL Pro/VIP treats POST /sync/history on already-watched titles as a *new
    rewatch session*. That is the source of duplicate rows. Therefore:

    - Status / list membership → POST /sync/add-to-list only (safe).
    - Episode history → **disabled by default**. Set SIMKL_ALLOW_HISTORY=1 to
      enable, and only for watching/on_hold when the local state is not completed.
    - Payload uses both `anime` and `shows` keys when the API accepts either;
      primary key is `anime` for this anime-only sync.
    """
    client_id = os.getenv("SIMKL_CLIENT_ID")
    token = os.getenv("SIMKL_ACCESS_TOKEN")
    if not client_id or not token:
        return
    headers = {
        "Authorization": f"Bearer {token}",
        "simkl-api-key": client_id,
        "Content-Type": "application/json",
        "User-Agent": "anime-sync/3.13",
    }
    ids = {
        k: v
        for k, v in {
            "simkl": entry["ids"].get("simkl"),
            "mal": entry["ids"].get("mal"),
            "anilist": entry["ids"].get("anilist"),
            "kitsu": entry["ids"].get("kitsu"),
        }.items()
        if v
    }
    if not ids:
        return

    status = state.get("status") or "plantowatch"
    to = REVERSE_STATUS["simkl"].get(status)
    if not to:
        return

    # Build item — include title when present to improve matching, never invent IDs
    item = {"ids": ids, "to": to}
    title = entry.get("title") or (entry.get("ids") or {}).get("title")
    if title:
        item["title"] = title

    # Prefer anime[] for anime library; also send shows[] for API variants that
    # only accept shows (harmless if ignored). One logical membership update.
    payload = {"anime": [item], "shows": [item]}
    r = request_with_retries(
        "POST",
        f"https://api.simkl.com/sync/add-to-list?client_id={client_id}&app-name=anime-sync&app-version=3.13",
        json=payload,
        headers=headers,
        timeout=15,
    )

    allow_hist = os.getenv("SIMKL_ALLOW_HISTORY", "0").strip().lower() in ("1", "true", "yes")
    progress = int(state.get("progress") or 0)
    hist_code = "skipped"
    if (
        allow_hist
        and status in ("watching", "on_hold")
        and progress > 0
        and status != "completed"
    ):
        # Still never pass allow_rewatch=yes — that explicitly creates sessions.
        hist_item = {"ids": ids, "watched_episodes": progress}
        if title:
            hist_item["title"] = title
        hist = {"anime": [hist_item], "shows": [hist_item]}
        hr = request_with_retries(
            "POST",
            f"https://api.simkl.com/sync/history?client_id={client_id}&app-name=anime-sync&app-version=3.13",
            json=hist,
            headers=headers,
            timeout=15,
        )
        hist_code = str(hr.status_code)
    print(f"   -> PUSH SIMKL {ids} => {state} list={r.status_code} hist={hist_code}")
