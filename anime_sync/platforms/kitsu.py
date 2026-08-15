"""Kitsu JSON:API loader, pusher, and password-grant OAuth."""
import os
from datetime import datetime, timezone

from anime_sync.http import request_with_retries, CircuitOpenError
from anime_sync.platforms.common import _push_skip_logged
from anime_sync.platforms.status import REVERSE_STATUS, STATUS_MAP
from anime_sync.ids import normalize_ids

import requests

_kitsu_access_token = None
_kitsu_user_id = None

def load_kitsu():
    username = os.getenv("KITSU_USERNAME", "")
    if not username:
        print("-> Kitsu skipped (no KITSU_USERNAME)"); return []
    print(f"-> Fetching Kitsu {username}...")
    try:
        u_resp = request_with_retries("GET", f"https://kitsu.io/api/edge/users?filter[name]={username}", timeout=15)
        u_resp.raise_for_status()
        u = u_resp.json()
        if not u.get("data"):
            print(f"   Kitsu user {username} not found"); return []
        user_id = u["data"][0]["id"]
        print(f"   Kitsu user ID: {user_id}")

        items=[]
        url = f"https://kitsu.io/api/edge/users/{user_id}/library-entries?filter[kind]=anime&page[limit]=500&include=anime"
        page=1
        while url:
            print(f"   Kitsu page {page} fetching...")
            try:
                r = request_with_retries("GET", url, timeout=30)
            except CircuitOpenError as e:
                print(f"   Kitsu skipped: {e}"); break
            if not r.ok:
                print(f"   Kitsu page {page} error {r.status_code}: {r.text[:200]}")
                break
            lib = r.json()
            id_map = {a["id"]: a["attributes"] for a in lib.get("included", []) if a["type"]=="anime"}
            for e in lib.get("data", []):
                a = e["attributes"]
                rel = e.get("relationships", {}).get("anime", {}).get("data")
                if not rel: continue
                anime_id = rel.get("id")
                anime = id_map.get(anime_id, {})
                mal_id = anime.get("idMal") or anime.get("id_mal") if anime else None
                title = anime.get("canonicalTitle") if anime else ""
                items.append({
                    "platform": "kitsu",
                    "ids": normalize_ids({"kitsu": anime_id, "mal": mal_id, "title": title}),
                    "state": {
                        "status": STATUS_MAP["kitsu"].get(a.get("status"), "plantowatch"),
                        "progress": a.get("progress", 0),
                        "score": a.get("ratingTwenty", 0) // 2 if a.get("ratingTwenty") else 0,
                    },
                    "updated": datetime.fromisoformat(a["updatedAt"].replace("Z", "+00:00")) if a.get("updatedAt") else datetime.now(timezone.utc),
                    "title": title,
                })
            url = lib.get("links", {}).get("next")
            page+=1
            if page>30:
                break
        print(f"   Kitsu: {len(items)} entries (ALL pages)")
        return items
    except requests.RequestException as ex:
        print(f"   Kitsu network error: {ex}")
        return []
    except Exception as ex:
        import traceback
        print(f"   Kitsu unexpected error: {ex}")
        traceback.print_exc()
        return []


def ensure_kitsu_token():
    """Return a Kitsu OAuth access token.

    Prefer KITSU_TOKEN / KITSU_ACCESS_TOKEN if set.
    Otherwise obtain one via password grant using KITSU_EMAIL + KITSU_PASSWORD
    (or KITSU_USERNAME as the login id).
    """
    global _kitsu_access_token
    existing = os.getenv("KITSU_TOKEN") or os.getenv("KITSU_ACCESS_TOKEN")
    if existing:
        return existing
    if "_kitsu_access_token" in globals() and _kitsu_access_token:
        return _kitsu_access_token

    email = os.getenv("KITSU_EMAIL") or os.getenv("KITSU_USERNAME")
    password = os.getenv("KITSU_PASSWORD")
    if not email or not password:
        return None

    try:
        r = request_with_retries(
            "POST",
            "https://kitsu.io/api/oauth/token",
            data={
                "grant_type": "password",
                "username": email,
                "password": password,
            },
            headers={"Accept": "application/json"},
            timeout=20,
        )
        if not r.ok:
            print(f"   -> Kitsu OAuth failed: {r.status_code} {r.text[:200]}")
            return None
        token = (r.json() or {}).get("access_token")
        if token:
            _kitsu_access_token = token
            # Make available to any code reading env later in this process
            os.environ["KITSU_TOKEN"] = token
            print("   -> Kitsu OAuth token obtained via password grant")
        return token
    except requests.RequestException as e:
        print(f"   -> Kitsu OAuth network error: {e}")
        return None



def push_kitsu(entry, state):
    """Best-effort Kitsu library-entry update/create.
    Full reliability needs the library-entry ID stored in the DB.
    Requires KITSU_TOKEN, or KITSU_EMAIL/USERNAME + KITSU_PASSWORD for password grant.
    """
    token = ensure_kitsu_token()
    if not token:
        if "kitsu" not in _push_skip_logged:
            print("   -> PUSH Kitsu skipped (no KITSU_TOKEN / email+password) [silencing further]")
            _push_skip_logged.add("kitsu")
        return
    kitsu_id = entry["ids"].get("kitsu")
    if not kitsu_id:
        return

    status = REVERSE_STATUS["kitsu"].get(state["status"])
    if not status:
        return

    # Kitsu expects ratingTwenty (0-20 scale). Our score is 0-10.
    rating = None
    score = state.get("score")
    if score is not None and score > 0:
        rating = int(float(score) * 2)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.api+json",
        "Accept": "application/vnd.api+json",
    }

    global _kitsu_user_id
    try:
        # Resolve current user id once per process
        if not _kitsu_user_id:
            me = request_with_retries(
                "GET",
                "https://kitsu.io/api/edge/users?filter[self]=true",
                headers=headers,
                timeout=15,
            )
            if not me.ok:
                print(f"   -> PUSH Kitsu auth failed: {me.status_code}")
                return
            users = me.json().get("data") or []
            if not users:
                print("   -> PUSH Kitsu: could not resolve current user")
                return
            _kitsu_user_id = users[0]["id"]
        user_id = _kitsu_user_id

        # Look up library entry
        lookup = request_with_retries(
            "GET",
            f"https://kitsu.io/api/edge/library-entries"
            f"?filter[userId]={user_id}&filter[animeId]={kitsu_id}&filter[kind]=anime",
            headers=headers,
            timeout=15,
        )
        existing = (lookup.json().get("data") or []) if lookup.ok else []

        attrs = {
            "status": status,
            "progress": int(state.get("progress") or 0),
        }
        if rating is not None:
            attrs["ratingTwenty"] = rating

        if existing:
            entry_id = existing[0]["id"]
            payload = {
                "data": {
                    "type": "libraryEntries",
                    "id": entry_id,
                    "attributes": attrs,
                }
            }
            r = request_with_retries(
                "PATCH",
                f"https://kitsu.io/api/edge/library-entries/{entry_id}",
                json=payload,
                headers=headers,
                timeout=15,
            )
            action = "update"
        else:
            payload = {
                "data": {
                    "type": "libraryEntries",
                    "attributes": attrs,
                    "relationships": {
                        "anime": {"data": {"type": "anime", "id": str(kitsu_id)}},
                        "user": {"data": {"type": "users", "id": str(user_id)}},
                    },
                }
            }
            r = request_with_retries(
                "POST",
                "https://kitsu.io/api/edge/library-entries",
                json=payload,
                headers=headers,
                timeout=15,
            )
            action = "create"

        if r.ok:
            print(f"   -> PUSH Kitsu {action} {kitsu_id} => {state} [{r.status_code}]")
        else:
            print(f"   -> PUSH Kitsu {action} failed {kitsu_id}: {r.status_code} {r.text[:200]}")
    except requests.RequestException as e:
        print(f"   -> PUSH Kitsu network error: {e}")


