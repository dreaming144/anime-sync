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
    query = """query ($userName: String) { MediaListCollection(userName: $userName, type: ANIME) { lists { entries { status progress score updatedAt media { id idMal title { romaji english native } } } } } }"""
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
                "updated": datetime.fromtimestamp(e["updatedAt"], tz=timezone.utc) if e["updatedAt"] else datetime.now(timezone.utc),
                "title": title,
            })
    print(f"   AniList: {len(items)} entries")
    return items


def push_anilist(entry, state):
    """Create or update an AniList media list entry via SaveMediaListEntry mutation."""
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

    mutation = """
    mutation ($mediaId: Int, $status: MediaListStatus, $progress: Int, $scoreRaw: Int) {
      SaveMediaListEntry(mediaId: $mediaId, status: $status, progress: $progress, scoreRaw: $scoreRaw) {
        id
        status
        progress
        score
      }
    }
    """
    # Internal score is 0-10; AniList scoreRaw is 0-100
    score_10 = float(state.get("score") or 0)
    score_raw = int(round(score_10 * 10)) if score_10 > 0 else 0
    variables = {
        "mediaId": media_id,
        "status": status,
        "progress": int(state.get("progress") or 0),
        "scoreRaw": score_raw,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        r = request_with_retries(
            "POST",
            "https://graphql.anilist.co",
            json={"query": mutation, "variables": variables},
            headers=headers,
            timeout=15,
        )
        if r.ok and r.json().get("data", {}).get("SaveMediaListEntry"):
            print(f"   -> PUSH AniList {media_id} => {state} [{r.status_code}]")
        else:
            err = r.text[:300] if r.text else r.status_code
            print(f"   -> PUSH AniList failed {media_id}: {err}")
    except requests.RequestException as e:
        print(f"   -> PUSH AniList network error: {e}")



