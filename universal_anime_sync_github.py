"""
Universal Anime Sync V3.13 - Automated offline DB sync
- Fribb anime-list-mini: auto-download + refresh when stale (IMDb/TVDB/TMDB)
- Manami anime-offline-database: auto-download + refresh for title backfill
- apply_offline_ids_to_db + apply_offline_titles_to_db every sync
- ARM + AniZip + Kitsu mappings for remaining gaps
"""

import requests, json, os, hashlib, argparse, time, csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from datetime import datetime, timezone
from pathlib import Path
import sys
import re
import sqlite3

CSV_PATH_DEFAULT = Path("anime_pairings.csv")
PUSH_REPORT_PATH = Path("push_report.csv")
DRY_RUN = False
_push_report_rows = []

UNMATCHED_PATH = Path("unmatched.csv")



# ---------------------------------------------------------------------------
# HTTP resilience layer — extracted to anime_sync.http (Phase 1 modularization)
# ---------------------------------------------------------------------------
from anime_sync.http import (  # noqa: E402
    BULKHEAD_LIMITS,
    Bulkhead,
    BulkheadPool,
    CircuitBreaker,
    CircuitOpenError,
    POOL_ENRICH,
    POOL_LOAD,
    POOL_PUSH,
    RATE_LIMITS,
    RateLimiter,
    _service_key,
    bulkhead_status,
    circuit_status,
    get_bulkhead,
    get_circuit,
    get_rate_limiter,
    rate_limiter_status,
    request_with_retries,
    write_circuit_metrics,
)

# ---------------------------------------------------------------------------
# Storage + IDs — extracted Phase 2
# ---------------------------------------------------------------------------
from anime_sync.storage import (  # noqa: E402
    CACHE_PATH,
    DB_PATH,
    OVERRIDES_PATH,
    SQLITE_PATH,
    _loaded,
    db,
    ensure_loaded,
    id_cache,
    load_db,
    manual_overrides,
    save_db,
)
from anime_sync.ids import (  # noqa: E402
    _normalize_imdb,
    _normalize_tmdb,
    dedupe_entries,
    get_canonical_key,
    get_override_for_ids,
    hash_state,
    normalize_ids,
)

# ---------------------------------------------------------------------------
# Enrichment — extracted Phase 3
# ---------------------------------------------------------------------------
from anime_sync.enrich import (  # noqa: E402
    FRIBB_PATH,
    FRIBB_URL,
    MANAMI_PATH,
    MANAMI_URL,
    OFFLINE_MAX_AGE_SEC,
    _arm_is_sparse,
    _arm_normalize_entry,
    _arm_pick_source,
    _arm_source_candidates,
    apply_offline_ids_to_db,
    apply_offline_titles_to_db,
    enrich_ids,
    enrich_ids_batch,
    ensure_offline_file,
    fetch_animeapi,
    fetch_anizip,
    fetch_arm,
    fetch_arm_batch,
    fetch_fribb,
    fetch_ids_moe,
    fetch_kitsu_mappings,
    fill_missing_simkl_ids,
    is_fully_resolved,
    load_fribb_index,
    load_manami_title_index,
)




CONFIG = {
    "anilist_username": os.getenv("ANILIST_USERNAME", ""),
    "mal_username": os.getenv("MAL_USERNAME", ""),
    "kitsu_username": os.getenv("KITSU_USERNAME", ""),
    # Conflict resolution policy when two platforms disagree:
    #   "last_write_wins"  - accept the entry with the newer updated timestamp (default)
    #   "source_priority"  - accept the entry from the higher-ranked platform
    "conflict_policy": os.getenv("CONFLICT_POLICY", "last_write_wins"),
    # Used only when conflict_policy == "source_priority" (first = highest priority)
    "source_priority": ["anilist", "mal", "kitsu", "simkl"],
}

STATUS_MAP = {
    "anilist": {"CURRENT": "watching", "COMPLETED": "completed", "PLANNING": "plantowatch", "DROPPED": "dropped", "PAUSED": "on_hold", "REPEATING": "watching"},
    "mal": {"watching": "watching", "completed": "completed", "plan_to_watch": "plantowatch", "dropped": "dropped", "on_hold": "on_hold"},
    "kitsu": {"current": "watching", "completed": "completed", "planned": "plantowatch", "dropped": "dropped", "on_hold": "on_hold"},
    "simkl": {"watching": "watching", "completed": "completed", "plantowatch": "plantowatch", "dropped": "dropped", "hold": "on_hold"}
}
REVERSE_STATUS = {
    "anilist": {"watching": "CURRENT", "completed": "COMPLETED", "plantowatch": "PLANNING", "dropped": "DROPPED", "on_hold": "PAUSED"},
    "mal": {"watching": "watching", "completed": "completed", "plantowatch": "plan_to_watch", "dropped": "dropped", "on_hold": "on_hold"},
    "kitsu": {"watching": "current", "completed": "completed", "plantowatch": "planned", "dropped": "dropped", "on_hold": "on_hold"},
    "simkl": {"watching": "watching", "completed": "completed", "plantowatch": "plantowatch", "dropped": "dropped", "on_hold": "hold"}
}

def load_anilist():
    username = CONFIG.get("anilist_username") or os.getenv("ANILIST_USERNAME")
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

def load_simkl():
    client_id = os.getenv("SIMKL_CLIENT_ID"); token = os.getenv("SIMKL_ACCESS_TOKEN")
    if not client_id or not token: 
        print("-> SIMKL skipped (no secrets)"); return []
    print("-> Fetching SIMKL...")
    headers = {"Authorization": f"Bearer {token}", "simkl-api-key": client_id}
    try:
        r = request_with_retries("GET", f"https://api.simkl.com/sync/all-items/anime?client_id={client_id}", headers=headers, timeout=30)
    except CircuitOpenError as e:
        print(f"-> SIMKL skipped: {e}"); return []
    if not r.ok: 
        print(f"   SIMKL error {r.status_code}"); return []
    data = r.json()
    raw_list = data if isinstance(data, list) else data.get("anime", []) or data.get("shows", []) or []
    items=[]
    for entry in raw_list:
        show_obj = entry.get("show", entry) if isinstance(entry, dict) else {}
        ids_obj = entry.get("ids") or show_obj.get("ids") or {}
        if not ids_obj:
            continue
        last = entry.get("last_watched_at") or entry.get("last_updated_at") or show_obj.get("last_watched_at")
        status_raw = entry.get("status") or show_obj.get("status") or "plantowatch"
        title = show_obj.get("title") or ""
        items.append({
            "platform":"simkl",
            "ids":{"simkl": ids_obj.get("simkl") or ids_obj.get("simkl_id"), "mal": ids_obj.get("mal"), "anilist": ids_obj.get("anilist") or ids_obj.get("anilist_id"), "anidb": ids_obj.get("anidb"), "title": title},
            "state":{"status": STATUS_MAP["simkl"].get(status_raw, "plantowatch"), "progress": entry.get("watched_episodes_count") or entry.get("watched_episodes") or show_obj.get("watched_episodes_count",0), "score": entry.get("user_rating") or show_obj.get("user_rating") or 0},
            "updated": datetime.fromisoformat(last.replace("Z","+00:00")) if last else datetime.now(timezone.utc),
            "title": title
        })
    print(f"   SIMKL: {len(items)} entries")
    return items


def _persist_mal_secrets(access_token, refresh_token=None):
    """Best-effort write updated MAL tokens back to GitHub Actions secrets via gh CLI."""
    repo = os.getenv("GITHUB_REPOSITORY", "dreaming144/anime-sync")
    token = os.getenv("SECRETS_WRITE_TOKEN") or os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not token:
        print("   -> MAL token refreshed in-process only (no GH token to persist secrets)")
        return False
    env = {**os.environ, "GH_TOKEN": token, "GITHUB_TOKEN": token}
    ok = True
    for name, value in (
        ("MAL_ACCESS_TOKEN", access_token),
        ("MAL_REFRESH_TOKEN", refresh_token),
    ):
        if not value:
            continue
        try:
            import subprocess
            r = subprocess.run(
                ["gh", "secret", "set", name, "-R", repo],
                input=value.encode("utf-8"),
                capture_output=True,
                timeout=60,
                env=env,
            )
            if r.returncode == 0:
                print(f"   -> Persisted GitHub secret {name}")
            else:
                err = (r.stderr or r.stdout or b"").decode("utf-8", "replace")[:200]
                print(f"   -> Could not persist {name}: {err}")
                ok = False
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
            print(f"   -> Could not persist {name}: {e}")
            ok = False
    return ok


def refresh_mal_token(force=False):
    """Refresh MAL access token using MAL_REFRESH_TOKEN + client credentials.

    Updates os.environ['MAL_ACCESS_TOKEN'] (and refresh token if rotated).
    Safe to call multiple times — only hits the network once per process unless force=True.
    """
    global _mal_token_refreshed
    if _mal_token_refreshed and not force:
        return os.getenv("MAL_ACCESS_TOKEN")

    refresh = os.getenv("MAL_REFRESH_TOKEN")
    client_id = os.getenv("MAL_CLIENT_ID")
    client_secret = os.getenv("MAL_CLIENT_SECRET")
    if not refresh or not client_id:
        return os.getenv("MAL_ACCESS_TOKEN")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": client_id,
    }
    if client_secret:
        data["client_secret"] = client_secret

    try:
        r = request_with_retries(
            "POST",
            "https://myanimelist.net/v1/oauth2/token",
            data=data,
            timeout=20,
        )
        if not r.ok:
            print(f"   -> MAL token refresh failed: {r.status_code} {r.text[:200]}")
            # Keep existing access token if any
            return os.getenv("MAL_ACCESS_TOKEN")
        payload = r.json() or {}
        access = payload.get("access_token")
        new_refresh = payload.get("refresh_token")
        if not access:
            print("   -> MAL token refresh returned no access_token")
            return os.getenv("MAL_ACCESS_TOKEN")
        os.environ["MAL_ACCESS_TOKEN"] = access
        if new_refresh:
            os.environ["MAL_REFRESH_TOKEN"] = new_refresh
        _mal_token_refreshed = True
        expires = payload.get("expires_in")
        print(f"   -> MAL access token refreshed (expires_in={expires})")
        _persist_mal_secrets(access, new_refresh or refresh)
        return access
    except requests.RequestException as e:
        print(f"   -> MAL token refresh network error: {e}")
        return os.getenv("MAL_ACCESS_TOKEN")


def _mal_access_expiring_soon(token, skew_seconds=86400):
    """True if JWT access token is missing/expired or within skew of expiry."""
    if not token or token.count(".") < 2:
        return True
    try:
        import base64
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        exp = int(data.get("exp") or 0)
        return exp < (time.time() + skew_seconds)
    except Exception:
        return True


def ensure_mal_token():
    """Return a usable MAL access token, refreshing when near expiry or missing."""
    token = os.getenv("MAL_ACCESS_TOKEN")
    can_refresh = bool(os.getenv("MAL_REFRESH_TOKEN") and os.getenv("MAL_CLIENT_ID"))
    if can_refresh and _mal_access_expiring_soon(token):
        token = refresh_mal_token() or token
    return token


def load_mal():
    token = ensure_mal_token()
    if not token:
        print("-> MAL skipped (no token)"); return []
    print("-> Fetching MAL...")
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.myanimelist.net/v2/users/@me/animelist?fields=list_status,num_episodes&limit=1000&nsfw=true"
    items = []
    page = 1
    while url:
        try:
            r = request_with_retries("GET", url, headers=headers, timeout=30)
        except CircuitOpenError as e:
            print(f"   MAL skipped: {e}"); break
        if not r.ok:
            print(f"   MAL error page {page}: {r.text[:300]}")
            break
        data = r.json()
        for n in data.get("data", []):
            ls = n["list_status"]
            title = n["node"].get("title", "")
            items.append({
                "platform": "mal",
                "ids": normalize_ids({"mal": n["node"]["id"], "title": title}),
                "state": {
                    "status": STATUS_MAP["mal"].get(ls["status"], "plantowatch"),
                    "progress": ls["num_episodes_watched"],
                    "score": ls["score"],
                },
                "updated": datetime.fromisoformat(ls["updated_at"].replace("Z", "+00:00")),
                "title": title,
            })
        url = (data.get("paging") or {}).get("next")
        page += 1
        if url:
            print(f"   MAL page {page}...")
            time.sleep(0.3)
    print(f"   MAL: {len(items)} entries")
    return items

def load_kitsu():
    username = CONFIG.get("kitsu_username") or os.getenv("KITSU_USERNAME")
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

def push_simkl(entry, state):
    client_id = os.getenv("SIMKL_CLIENT_ID"); token = os.getenv("SIMKL_ACCESS_TOKEN")
    if not client_id or not token: return
    headers = {"Authorization": f"Bearer {token}", "simkl-api-key": client_id, "Content-Type": "application/json"}
    ids = {k:v for k,v in {"mal": entry["ids"].get("mal"), "anilist": entry["ids"].get("anilist"), "kitsu": entry["ids"].get("kitsu")}.items() if v}
    if not ids: return
    payload = {"shows": [{"ids": ids, "to": REVERSE_STATUS["simkl"][state["status"]]}]}
    r = request_with_retries("POST", f"https://api.simkl.com/sync/add-to-list?client_id={client_id}", json=payload, headers=headers, timeout=15)
    if state["progress"]>0:
        hist = {"shows": [{"ids": ids, "watched_episodes": state["progress"]}]}
        request_with_retries("POST", f"https://api.simkl.com/sync/history?client_id={client_id}", json=hist, headers=headers, timeout=15)
    print(f"   -> PUSH SIMKL {ids} => {state} [{r.status_code}]")

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


def push_mal(entry, state):
    """Create or update a MAL list entry via PUT /anime/{id}/my_list_status.

    Requires MAL_ACCESS_TOKEN with write:users scope.
    Body is application/x-www-form-urlencoded (not JSON).
    """
    token = ensure_mal_token()
    if not token:
        if "mal" not in _push_skip_logged:
            print("   -> PUSH MAL skipped (no MAL_ACCESS_TOKEN) [silencing further]")
            _push_skip_logged.add("mal")
        return
    mal_id = entry["ids"].get("mal")
    if not mal_id:
        return
    try:
        mal_id = int(mal_id)
    except (ValueError, TypeError):
        print(f"   -> PUSH MAL skipped (invalid mal id: {mal_id})")
        return

    status = REVERSE_STATUS["mal"].get(state["status"])
    if not status:
        print(f"   -> PUSH MAL skipped (unknown status: {state.get('status')})")
        return

    # MAL score is integer 0-10 (0 = unset)
    score = int(round(float(state.get("score") or 0)))
    if score < 0:
        score = 0
    if score > 10:
        score = 10

    payload = {
        "status": status,
        "num_watched_episodes": int(state.get("progress") or 0),
        "score": score,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        r = request_with_retries(
            "PUT",
            f"https://api.myanimelist.net/v2/anime/{mal_id}/my_list_status",
            data=payload,
            headers=headers,
            timeout=15,
        )
        if r.ok:
            print(f"   -> PUSH MAL {mal_id} => {state} [{r.status_code}]")
        else:
            print(f"   -> PUSH MAL failed {mal_id}: {r.status_code} {r.text[:200]}")
    except requests.RequestException as e:
        print(f"   -> PUSH MAL network error: {e}")



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

PUSHERS = {"anilist": push_anilist, "mal": push_mal, "kitsu": push_kitsu, "simkl": push_simkl}


def resolve_title(ids, existing_title=None):
    """Best-effort title from local data, then AniList / Jikan / Kitsu APIs."""
    if existing_title:
        return existing_title
    if ids.get("title"):
        return ids["title"]

    # AniList GraphQL
    if ids.get("anilist"):
        try:
            q = "query ($id: Int) { Media(id: $id, type: ANIME) { title { romaji english native } } }"
            r = request_with_retries(
                "POST",
                "https://graphql.anilist.co",
                json={"query": q, "variables": {"id": int(ids["anilist"])}},
                timeout=12,
            )
            if r.ok:
                t = (((r.json().get("data") or {}).get("Media") or {}).get("title") or {})
                title = t.get("english") or t.get("romaji") or t.get("native")
                if title:
                    return title
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError):
            pass

    # Jikan (MAL)
    if ids.get("mal"):
        try:
            r = request_with_retries("GET", f"https://api.jikan.moe/v4/anime/{int(ids['mal'])}", timeout=12, base_sleep=2.0)
            if r.ok:
                data = (r.json().get("data") or {})
                title = data.get("title_english") or data.get("title") or data.get("title_japanese")
                if title:
                    return title
            time.sleep(0.35)  # Jikan soft rate limit
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError):
            pass

    # Kitsu
    if ids.get("kitsu"):
        try:
            r = request_with_retries("GET", f"https://kitsu.io/api/edge/anime/{ids['kitsu']}", timeout=12)
            if r.ok:
                attrs = ((r.json().get("data") or {}).get("attributes") or {})
                titles = attrs.get("titles") or {}
                title = attrs.get("canonicalTitle") or titles.get("en") or titles.get("en_jp")
                if title:
                    return title
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError):
            pass

    return ""


def export_csv(file_path=CSV_PATH_DEFAULT, fill_titles=True, max_title_fetches=400):
    """Export pairings CSV. Optionally backfill blank titles via AniList/Jikan/Kitsu."""
    entries = db.get("entries", {})
    if not entries:
        print("No entries to export")
        return

    fieldnames = [
        "title", "title_english", "title_romaji", "title_native",
        "year", "season", "format", "episodes",
        "canonical_key", "mal_id", "anilist_id", "kitsu_id", "anidb_id",
        "imdb_id", "tvdb_id", "tmdb_id", "simkl_id",
        "status", "progress", "score", "last_updated", "source", "media_type",
    ]

    filled = 0
    fetched = 0
    rows = []
    for key, data in entries.items():
        ids = dict(data.get("ids") or {})
        state = data.get("state") or {}
        title = ids.get("title") or data.get("title") or ""
        if fill_titles and not title and fetched < max_title_fetches:
            title = resolve_title(ids, None) or ""
            fetched += 1
            if title:
                filled += 1
                ids["title"] = title
                data["title"] = title
                data["ids"] = ids
                entries[key] = data
        rows.append({
            "title": title,
            "title_english": ids.get("title_english") or data.get("title_english") or "",
            "title_romaji": ids.get("title_romaji") or data.get("title_romaji") or title,
            "title_native": ids.get("title_native") or data.get("title_native") or "",
            "year": ids.get("year") or data.get("year") or "",
            "season": ids.get("season") or data.get("season") or "",
            "format": ids.get("format") or data.get("format") or "",
            "episodes": ids.get("episodes") or data.get("episodes") or "",
            "canonical_key": key,
            "mal_id": ids.get("mal") or "",
            "anilist_id": ids.get("anilist") or "",
            "kitsu_id": ids.get("kitsu") or "",
            "anidb_id": ids.get("anidb") or "",
            "imdb_id": ids.get("imdb") or "",
            "tvdb_id": ids.get("tvdb") or "",
            "tmdb_id": ids.get("tmdb") or "",
            "simkl_id": ids.get("simkl") or "",
            "status": state.get("status") or "",
            "progress": state.get("progress") or 0,
            "score": state.get("score") or 0,
            "last_updated": data.get("last_updated") or "",
            "source": ids.get("_source") or "",
            "media_type": ids.get("media_type") or data.get("media_type") or "",
        })

    rows.sort(key=lambda r: (r.get("title") or "").lower())

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    if filled:
        try:
            safe_save_db()
        except Exception:
            pass
        print(f"   Titles backfilled: {filled} (fetched up to {fetched})")
    print(f"CSV exported to {file_path} - {len(entries)} rows")
    return file_path

def export_unmatched(file_path=UNMATCHED_PATH):
    """Export shows that couldn't be fully paired"""
    entries = db.get("entries", {})
    unmatched = []
    
    for key, data in entries.items():
        ids = data.get("ids", {})
        # Consider unmatched if missing both mal AND anilist (core IDs) or missing anidb when we expect it
        missing = []
        if not ids.get("mal"):
            missing.append("mal")
        if not ids.get("anilist"):
            missing.append("anilist")
        if not ids.get("anidb"):
            missing.append("anidb")
        # Western / non-anime (Avatar, Korra, etc.) are not expected to have MAL/AniList
        if data.get("non_anime") or ids.get("non_anime") or data.get("media_type") == "western" or ids.get("media_type") == "western":
            continue
        # Only flag as unmatched if missing core anime pairing (MAL + AniList)
        if not ids.get("mal") and not ids.get("anilist"):
            has = [k for k in ["kitsu", "simkl", "anidb"] if ids.get(k)]
            reason = "isolated - only has " + (",".join(has) if has else "nothing") + " - needs manual pairing"
            title = ids.get("title") or data.get("title") or ""
            if not title:
                title = resolve_title(ids) or ""
            unmatched.append({
                "title": title,
                "canonical_key": key,
                "mal_id": ids.get("mal") or "",
                "anilist_id": ids.get("anilist") or "",
                "kitsu_id": ids.get("kitsu") or "",
                "anidb_id": ids.get("anidb") or "",
                "imdb_id": ids.get("imdb") or "",
                "existing_ids": json.dumps({k:v for k,v in ids.items() if v and not k.startswith("_")}),
                "missing": ",".join(missing),
                "reason": reason,
                "suggested_override_key": key,
                "suggested_override_value": f'{{"mal": {ids.get("mal") or "???"}, "anilist": {ids.get("anilist") or "???"}, "anidb": {ids.get("anidb") or "???"}}}'
            })
    
    fieldnames = ["title", "canonical_key", "mal_id", "anilist_id", "kitsu_id", "anidb_id", "imdb_id", "missing", "reason", "existing_ids", "suggested_override_key", "suggested_override_value"]
    
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted(unmatched, key=lambda x: x["title"].lower()):
            writer.writerow(row)
    
    print(f"Unmatched report: {file_path} - {len(unmatched)} entries need attention")
    
    # Also create a template overrides file for these
    if unmatched:
        template_path = Path("manual_overrides_template.json")
        template = {}
        for u in unmatched[:20]:  # first 20 as example
            template[u["suggested_override_key"]] = {
                "mal": None,
                "anilist": None,
                "anidb": None,
                "title": u["title"],
                "comment": f"Fill in IDs for {u['title']} - {u['reason']}"
            }
        template_path.write_text(json.dumps(template, indent=2), encoding='utf-8')
        print(f"Template for manual fixes: {template_path}")
    
    return file_path, len(unmatched)


def should_accept_update(existing, item, policy=None):
    """Decide whether the incoming item should overwrite the stored state.

    Returns (accept: bool, reason: str)
    """
    policy = policy or CONFIG.get("conflict_policy", "last_write_wins")

    if policy == "source_priority":
        priority = CONFIG.get("source_priority", ["anilist", "mal", "kitsu", "simkl"])
        # Find the platform that last wrote the stored state (best effort)
        last_platform = None
        for p in priority:
            if existing.get("last_synced", {}).get(p):
                last_platform = p
                break
        incoming_rank = priority.index(item["platform"]) if item["platform"] in priority else 999
        stored_rank = priority.index(last_platform) if last_platform in priority else 999

        if incoming_rank < stored_rank:
            return True, f"source_priority ({item['platform']} > {last_platform})"
        if incoming_rank > stored_rank:
            return False, f"source_priority ({last_platform} > {item['platform']})"
        # same rank → fall through to timestamp

    # Default / fallback: last_write_wins
    try:
        last_updated = datetime.fromisoformat(existing["last_updated"])
        if item["updated"] > last_updated:
            return True, "newer timestamp"
        return False, "older or equal timestamp"
    except (ValueError, TypeError, KeyError):
        return True, "missing timestamp - accept"



def record_push(platform, ids, state, action="planned", detail=""):
    """Append a row for push_report.csv (always recorded; dry-run skips HTTP)."""
    _push_report_rows.append({
        "platform": platform,
        "action": action,
        "mal": (ids or {}).get("mal") or "",
        "anilist": (ids or {}).get("anilist") or "",
        "kitsu": (ids or {}).get("kitsu") or "",
        "simkl": (ids or {}).get("simkl") or "",
        "status": (state or {}).get("status") or "",
        "progress": (state or {}).get("progress") or "",
        "score": (state or {}).get("score") or "",
        "detail": detail,
        "dry_run": str(bool(DRY_RUN)).lower(),
    })


def write_push_report(path=PUSH_REPORT_PATH):
    if not _push_report_rows:
        print("   Push report: no pushes planned")
        return None
    fields = ["platform", "action", "mal", "anilist", "kitsu", "simkl", "status", "progress", "score", "detail", "dry_run"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(_push_report_rows)
    print(f"   Push report: {len(_push_report_rows)} rows → {path}")
    return path


def write_job_summary(path="job_summary.md"):
    """Markdown summary for GitHub Actions step summary / artifact."""
    entries = (db.get("entries") or {}) if isinstance(db, dict) else {}
    lines = [
        "# Anime Sync Job Summary",
        "",
        f"- Entries: **{len(entries)}**",
        f"- Dry run: **{DRY_RUN}**",
    ]
    for field in ("mal", "anilist", "kitsu", "simkl", "imdb", "tvdb"):
        n = sum(1 for e in entries.values() if (e.get("ids") or {}).get(field))
        lines.append(f"- With {field}: **{n}**")
    try:
        cs = circuit_status()
        if cs:
            lines.append("")
            lines.append("## Circuits")
            for k, v in cs.items():
                lines.append(f"- `{k}`: {v.get('state')} ok={v.get('successes')} fail={v.get('failures')} skip={v.get('short_circuits')}")
    except Exception:
        pass
    try:
        rl = rate_limiter_status()
        if rl:
            lines.append("")
            lines.append("## Rate limits")
            for k, v in rl.items():
                lines.append(
                    f"- `{k}`: {v.get('total')} req, {v.get('waits')} waits, "
                    f"interval={v.get('min_interval')}s (base {v.get('base_interval')}), "
                    f"throttle={v.get('throttle_events')} recover={v.get('recover_events')}"
                )
    except Exception:
        pass
    lines.append("")
    lines.append(f"## Pushes recorded: {len(_push_report_rows)}")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"   Wrote {path}")
    return path


def run_once(enrich_new=True, export_csv_flag=False, csv_file=CSV_PATH_DEFAULT, export_unmatched_flag=True, write_json_backup=True):
    ensure_loaded()
    # Always apply offline IMDb/TVDB/TMDB + titles (refresh dumps if stale)
    apply_offline_ids_to_db()
    apply_offline_titles_to_db()
    fill_missing_simkl_ids()
    all_items=[]
    # Bulkhead: each platform loader runs in the isolated load pool
    load_pool = POOL_LOAD.executor()
    loader_futs = {
        load_pool.submit(loader): loader.__name__
        for loader in (load_anilist, load_simkl, load_mal, load_kitsu)
    }
    for fut in as_completed(loader_futs):
        name = loader_futs[fut]
        try:
            items = fut.result()
            all_items.extend(items or [])
            print(f"   loader {name}: {len(items or [])} items")
        except Exception as e:
            print(f"Loader {name} failed: {e}")

    changes=0
    enriched_count=0

    # --- Smarter enrichment pass (skip fully-resolved, concurrent for the rest) ---
    if enrich_new:
        to_enrich = []
        for item in all_items:
            key_preview = get_canonical_key(item["ids"])
            already_known = key_preview and db["entries"].get(key_preview)
            known_ids = already_known.get("ids", {}) if already_known else {}
            # Copy known IDs forward so we keep coverage
            if already_known:
                for k, v in known_ids.items():
                    if v and not item["ids"].get(k):
                        item["ids"][k] = v
            merged = {**known_ids, **item["ids"]}
            # Core IDs enough to skip *network* enrich; offline Fribb already ran on DB
            if is_fully_resolved(merged):
                item["ids"] = merged
                continue
            to_enrich.append(item)

        if to_enrich:
            print(f"-> Enriching {len(to_enrich)} items concurrently (skipping {len(all_items) - len(to_enrich)} already resolved)...")
            enriched_list = enrich_ids_batch(to_enrich, max_workers=4)
            for item, enriched_ids in zip(to_enrich, enriched_list):
                if not enriched_ids:
                    continue
                for k, v in enriched_ids.items():
                    if v and not k.startswith("_"):
                        if not item["ids"].get(k):
                            item["ids"][k] = v
                    if k == "_source":
                        item["ids"]["_source"] = v
                enriched_count += 1
            print(f"   Enriched {enriched_count} items")

    for item in all_items:
        key = get_canonical_key(item["ids"])
        if not key: 
            if item["ids"].get("mal"):
                key = f"mal_{item['ids']['mal']}"
            elif item["ids"].get("anilist"):
                key = f"anilist_{item['ids']['anilist']}"
            elif item["ids"].get("kitsu"):
                key = f"kitsu_{item['ids']['kitsu']}"
            else:
                continue

        existing = db["entries"].get(key)
        incoming_hash = hash_state(item["state"])
        
        if not existing:
            title = item.get("title") or (item.get("ids") or {}).get("title") or ""
            ids = dict(item["ids"])
            if title and not ids.get("title"):
                ids["title"] = title
            db["entries"][key] = {
                "ids": ids,
                "state": item["state"],
                "last_updated": item["updated"].isoformat(),
                "last_synced": {item["platform"]: incoming_hash},
                "title": title,
            }
            continue
        
        # Propagate title if missing in DB (both entry-level and ids.title for CSV export)
        incoming_title = item.get("title") or (item.get("ids") or {}).get("title")
        if incoming_title:
            if not existing.get("title"):
                existing["title"] = incoming_title
            if not (existing.get("ids") or {}).get("title"):
                existing.setdefault("ids", {})["title"] = incoming_title

        if existing["last_synced"].get(item["platform"]) == incoming_hash:
            for k, v in item["ids"].items():
                if v and not existing["ids"].get(k):
                    existing["ids"][k] = v
            continue
        
        accept, reason = should_accept_update(existing, item)
        if accept:
            print(f"[CHANGE] {key} on {item['platform']} ({reason}) - {item['state']}")
            existing["state"] = item["state"]
            existing["last_updated"] = item["updated"].isoformat()
            for k, v in item["ids"].items():
                if v:
                    existing["ids"][k] = v
            for platform, pusher in PUSHERS.items():
                if platform == item["platform"]:
                    existing["last_synced"][platform] = incoming_hash
                    continue
                try:
                    record_push(platform, existing.get("ids"), item["state"], action="change")
                    if DRY_RUN:
                        print(f"   [DRY-RUN] skip push {platform} {key}")
                        existing["last_synced"][platform] = incoming_hash
                        changes += 1
                        continue
                    pusher(existing, item["state"])
                    existing["last_synced"][platform] = incoming_hash
                    changes += 1
                except Exception as e:
                    record_push(platform, existing.get("ids"), item["state"], action="error", detail=str(e)[:120])
                    print(f"Push to {platform} failed: {e}")
        else:
            # Incoming is older / lower priority → backfill the stored state to this platform if needed
            if existing["last_synced"].get(item["platform"]) != hash_state(existing["state"]):
                print(f"[BACKFILL] {key} -> {item['platform']} (kept stored state: {reason})")
                try:
                    record_push(item["platform"], existing.get("ids"), existing["state"], action="backfill")
                    if DRY_RUN:
                        print(f"   [DRY-RUN] skip backfill {item['platform']} {key}")
                        existing["last_synced"][item["platform"]] = hash_state(existing["state"])
                        changes += 1
                        continue
                    PUSHERS[item["platform"]](existing, existing["state"])
                    existing["last_synced"][item["platform"]] = hash_state(existing["state"])
                    changes += 1
                except Exception as e:
                    record_push(item["platform"], existing.get("ids"), existing["state"], action="error", detail=str(e)[:120])
                    print(e)

    db["id_cache"] = id_cache
    dedupe_entries()
    save_db(db, id_cache, write_json_backup=write_json_backup)
    
    anidb_count = sum(1 for e in db["entries"].values() if e["ids"].get("anidb"))
    imdb_count = sum(1 for e in db["entries"].values() if e["ids"].get("imdb"))
    mal_count = sum(1 for e in db["entries"].values() if e["ids"].get("mal"))
    anilist_count = sum(1 for e in db["entries"].values() if e["ids"].get("anilist"))
    kitsu_count = sum(1 for e in db["entries"].values() if e["ids"].get("kitsu"))
    manual_count = sum(1 for e in db["entries"].values() if e["ids"].get("_source") == "manual_override")
    
    print(f"Done. {len(all_items)} total fetched, {len(db['entries'])} unique shows, {changes} pushes.")
    write_push_report()
    write_job_summary()
    _cs = circuit_status()
    if _cs:
        print(
            "   Circuits: "
            + ", ".join(
                f"{k}={v['state']}(ok={v['successes']}/fail={v['failures']}/skip={v['short_circuits']})"
                for k, v in _cs.items()
            )
        )
    _bh = bulkhead_status()
    if _bh:
        print(
            "   Bulkheads: "
            + ", ".join(f"{k}={v['total']}calls/{v['rejected']}rej" for k, v in _bh.items())
        )
    _rl = rate_limiter_status()
    if _rl:
        print(
            "   RateLimits: "
            + ", ".join(
                f"{k}={v['total']}req/{v['waits']}w/i={v['min_interval']}s"
                for k, v in _rl.items()
            )
        )
    try:
        write_circuit_metrics("circuit_metrics.json")
        print("   Wrote circuit_metrics.json")
    except Exception as e:
        print(f"   circuit metrics write skipped: {e}")

    if export_csv_flag:
        export_csv(csv_file)
    
    if export_unmatched_flag:
        export_unmatched(UNMATCHED_PATH)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal Anime Sync")
    parser.add_argument("--no-enrich", action="store_true", help="Skip network ID enrichment")
    parser.add_argument("--enrich-all", action="store_true", help="Clear ID cache and re-enrich everything")
    parser.add_argument("--export-csv", action="store_true", help="Export anime_pairings.csv after sync")
    parser.add_argument("--export-csv-file", default="anime_pairings.csv")
    parser.add_argument("--export-only", action="store_true", help="Only export CSVs, no fetch/sync")
    parser.add_argument("--no-unmatched", action="store_true", help="Skip unmatched report")
    parser.add_argument("--no-json-backup", action="store_true", help="Skip writing legacy JSON backups")
    parser.add_argument("--dry-run", action="store_true", help="Plan pushes but do not write to remote lists")
    parser.add_argument("--no-push", action="store_true", help="Fetch/enrich only; skip all remote pushes")
    args = parser.parse_args()

    if args.dry_run or args.no_push:
        DRY_RUN = True  # module-level flag
        print("-> DRY-RUN / no-push: remote list writes disabled")

    ensure_loaded()

    if args.export_only:
        apply_offline_ids_to_db()
        apply_offline_titles_to_db()
        fill_missing_simkl_ids()
        dedupe_entries()
        save_db(db, id_cache, write_json_backup=not args.no_json_backup)
        export_csv(args.export_csv_file)
        if not args.no_unmatched:
            export_unmatched(UNMATCHED_PATH)
        sys.exit(0)

    if args.enrich_all:
        id_cache.clear()

    run_once(
        enrich_new=not args.no_enrich,
        export_csv_flag=args.export_csv,
        csv_file=Path(args.export_csv_file),
        export_unmatched_flag=not args.no_unmatched,
        write_json_backup=not args.no_json_backup,
    )
    
