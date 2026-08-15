"""AniList GraphQL loader and pusher."""
import os
from datetime import datetime, timezone

from anime_sync.http import request_with_retries, CircuitOpenError
from anime_sync.platforms.common import _push_skip_logged
from anime_sync.platforms.status import REVERSE_STATUS, STATUS_MAP
from anime_sync.ids import normalize_ids

import requests

def load_anilist():
    username = os.getenv("ANILIST_USERNAME", "")
    if not username:
        print("-> AniList skipped (no ANILIST_USERNAME)"); return []
    print(f"-> Fetching AniList {username}...")
    # Note: MediaList only exposes `score` (user format). scoreRaw is mutation-only.
    query = """query ($userName: String) { MediaListCollection(userName: $userName, type: ANIME) { lists { entries { status progress score updatedAt startedAt { year month day } completedAt { year month day } media { id idMal title { romaji english native } } } } } }"""
    headers = {}
    token = os.getenv("ANILIST_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = request_with_retries(
        "POST",
        "https://graphql.anilist.co",
        json={"query": query, "variables": {"userName": username}},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    items = []
    for lst in r.json()["data"]["MediaListCollection"]["lists"]:
        for e in lst["entries"]:
            title = e["media"]["title"]["romaji"] or e["media"]["title"]["english"] or e["media"]["title"]["native"]
            # score is in the user's preferred format (often 0-10 or 0-100).
            # We store as-is; push_anilist converts 0-10 internal -> scoreRaw when writing.
            score = e.get("score") or 0
            if score > 10:  # likely 100-point scale
                score = round(score / 10, 1)
            items.append({
                "platform": "anilist",
                "ids": normalize_ids({"anilist": e["media"]["id"], "mal": e["media"]["idMal"], "title": title}),
                "state": {
                    "status": STATUS_MAP["anilist"].get(e["status"], "plantowatch"),
                    "progress": e["progress"],
                    "score": score,
                },
                "dates": {
                    "started_at": e.get("startedAt"),
                    "completed_at": e.get("completedAt"),
                },
                "updated": datetime.fromtimestamp(e["updatedAt"], tz=timezone.utc) if e["updatedAt"] else datetime.now(timezone.utc),
                "title": title,
            })
    print(f"   AniList: {len(items)} entries")
    return items


def push_anilist(entry, state, dates=None):
    """Create or update an AniList media list entry via SaveMediaListEntry mutation.

    Optional dates: {"started_at": "YYYY-MM-DD", "completed_at": "YYYY-MM-DD"}
    propagated as FuzzyDateInput without creating rewatch counters.
    """
    token = os.getenv("ANILIST_TOKEN")
    if not token:
        if "anilist" not in _push_skip_logged:
            print("   -> PUSH AniList skipped (no ANILIST_TOKEN) [silencing further]")
            _push_skip_logged.add("anilist")
        return
    media_id = entry["ids"].get("anilist")
    if not media_id:
        return
    try:
        media_id = int(media_id)
    except (ValueError, TypeError):
        print(f"   -> PUSH AniList skipped (invalid mediaId: {media_id})")
        return

    status = REVERSE_STATUS["anilist"].get(state["status"])
    if not status:
        print(f"   -> PUSH AniList skipped (unknown status: {state.get('status')})")
        return

    from anime_sync.dates import parse_date, to_fuzzy

    mutation = """
    mutation (
      $mediaId: Int
      $status: MediaListStatus
      $progress: Int
      $scoreRaw: Int
      $startedAt: FuzzyDateInput
      $completedAt: FuzzyDateInput
    ) {
      SaveMediaListEntry(
        mediaId: $mediaId
        status: $status
        progress: $progress
        scoreRaw: $scoreRaw
        startedAt: $startedAt
        completedAt: $completedAt
      ) {
        id
        status
        progress
        score
        startedAt { year month day }
        completedAt { year month day }
      }
    }
    """
    score_10 = float(state.get("score") or 0)
    score_raw = int(round(score_10 * 10)) if score_10 else 0
    variables = {
        "mediaId": media_id,
        "status": status,
        "progress": int(state.get("progress") or 0),
        "scoreRaw": score_raw if score_raw > 0 else None,
    }
    dates = dates or entry.get("dates") or {}
    started = to_fuzzy(parse_date(dates.get("started_at")))
    completed = to_fuzzy(parse_date(dates.get("completed_at")))
    if started:
        variables["startedAt"] = started
    if completed:
        variables["completedAt"] = completed
    # Drop nulls so we don't clear existing score unintentionally with scoreRaw null
    variables = {k: v for k, v in variables.items() if v is not None}

    r = request_with_retries(
        "POST",
        "https://graphql.anilist.co",
        json={"query": mutation, "variables": variables},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=20,
    )
    print(f"   -> PUSH AniList media={media_id} => {state} dates={ {k: variables.get(k) for k in ('startedAt','completedAt') if k in variables} } [{r.status_code}]")


