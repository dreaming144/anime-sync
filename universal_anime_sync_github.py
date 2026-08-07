"""
Universal Anime Sync V3.3 - Bidirectional push support
- Manual overrides + unmatched report
- Exports anime_pairings.csv with all IDs
- Real pushers: AniList (SaveMediaListEntry), Kitsu (library-entries), SIMKL
- MAL push still a placeholder
"""

import requests, json, os, hashlib, argparse, time, csv
from datetime import datetime, timezone
from pathlib import Path
import sys

DB_PATH = Path("sync_db.json")
CACHE_PATH = Path("id_cache.json")
CSV_PATH_DEFAULT = Path("anime_pairings.csv")
UNMATCHED_PATH = Path("unmatched.csv")
OVERRIDES_PATH = Path("manual_overrides.json")

if DB_PATH.exists():
    try:
        db = json.loads(DB_PATH.read_text(encoding='utf-8'))
        if "entries" not in db:
            db = {"entries": db} if isinstance(db, dict) else {"entries": {}}
    except (json.JSONDecodeError, FileNotFoundError, KeyError):
        db = {"entries": {}, "id_cache": {}}
else:
    db = {"entries": {}, "id_cache": {}}

if CACHE_PATH.exists():
    try:
        id_cache = json.loads(CACHE_PATH.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, FileNotFoundError):
        id_cache = {}
else:
    id_cache = db.get("id_cache", {})

# Load manual overrides
if OVERRIDES_PATH.exists():
    try:
        manual_overrides = json.loads(OVERRIDES_PATH.read_text(encoding='utf-8'))
        print(f"Loaded {len(manual_overrides)} manual overrides from {OVERRIDES_PATH}")
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Failed to load overrides: {e}")
        manual_overrides = {}
else:
    manual_overrides = {}
    # Create empty file for user
    OVERRIDES_PATH.write_text(json.dumps({}, indent=2), encoding='utf-8')
    print(f"Created empty {OVERRIDES_PATH} - add manual pairings there")

CONFIG = {
    "anilist_username": os.getenv("ANILIST_USERNAME", "Dreamstorm"),
    "mal_username": os.getenv("MAL_USERNAME", ""),
    "kitsu_username": os.getenv("KITSU_USERNAME", "Dreamst0rm"),
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

def hash_state(state):
    # Using SHA256 for state change detection (non-cryptographic use)
    payload = f"{state['status']}|{state['progress']}|{state['score']}"
    return hashlib.sha256(payload.encode()).hexdigest()[:32]

# ============== MANUAL OVERRIDES ==============
def get_override_for_ids(ids_dict):
    """Check if any ID in ids_dict has a manual override"""
    possible_keys = []
    for k in ["mal", "anilist", "kitsu", "anidb", "simkl"]:
        if ids_dict.get(k):
            possible_keys.append(f"{k}_{ids_dict[k]}")
            # also try just the numeric id as key
            possible_keys.append(str(ids_dict[k]))
    
    for key in possible_keys:
        if key in manual_overrides:
            return manual_overrides[key]
        # case-insensitive
        if key.lower() in manual_overrides:
            return manual_overrides[key.lower()]
    
    return None

# ============== ID PAIRING ==============
def fetch_anizip(anilist_id=None, mal_id=None, use_cache=True):
    cache_key = f"anilist_{anilist_id}" if anilist_id else f"mal_{mal_id}" if mal_id else None
    if use_cache and cache_key and cache_key in id_cache:
        cached = id_cache[cache_key]
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached.get("_cached_at", "2000-01-01T00:00:00+00:00"))).days
            if age < 30:
                return cached
        except (ValueError, TypeError):
            pass

    url = None
    if anilist_id:
        url = f"https://api.ani.zip/mappings?anilist_id={anilist_id}"
    elif mal_id:
        url = f"https://api.ani.zip/mappings?mal_id={mal_id}"
    else:
        return None

    try:
        r = requests.get(url, timeout=15)
        if not r.ok:
            return None
        data = r.json()
        mappings = data.get("mappings", {})
        titles = data.get("titles", {})
        title = titles.get("en") or titles.get("x-jat") or (list(titles.values())[0] if titles else None)
        
        result = {
            "anilist": data.get("id") or mappings.get("anilist_id") or anilist_id,
            "mal": mappings.get("mal_id") or mal_id,
            "kitsu": mappings.get("kitsu_id"),
            "anidb": mappings.get("anidb_id"),
            "imdb": mappings.get("imdb_id"),
            "tvdb": mappings.get("thetvdb_id"),
            "tmdb": mappings.get("themoviedb_id"),
            "title": title,
            "_cached_at": datetime.now(timezone.utc).isoformat(),
            "_source": "anizip"
        }
        result = {k: v for k, v in result.items() if v}
        if cache_key:
            id_cache[cache_key] = result
        return result
    except requests.RequestException:
        return None

def fetch_kitsu_mappings(kitsu_anime_id, use_cache=True):
    cache_key = f"kitsu_{kitsu_anime_id}"
    if use_cache and cache_key in id_cache:
        cached = id_cache[cache_key]
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached.get("_cached_at", "2000-01-01T00:00:00+00:00"))).days
            if age < 30 and cached.get("_source") == "kitsu_mappings":
                return cached
        except (ValueError, TypeError):
            pass

    try:
        url = f"https://kitsu.io/api/edge/anime/{kitsu_anime_id}/mappings"
        r = requests.get(url, timeout=15)
        if not r.ok:
            return {}
        result = {"kitsu": kitsu_anime_id, "_cached_at": datetime.now(timezone.utc).isoformat(), "_source": "kitsu_mappings"}
        for m in r.json().get("data", []):
            site = m["attributes"]["externalSite"]
            ext_id = m["attributes"]["externalId"]
            if site == "myanimelist/anime" and ext_id.isdigit():
                result["mal"] = int(ext_id)
            elif site == "anidb":
                try:
                    result["anidb"] = int(ext_id)
                except (ValueError, TypeError):
                    result["anidb"] = ext_id
            elif site.startswith("imdb"):
                result["imdb"] = ext_id if str(ext_id).startswith("tt") else f"tt{ext_id}"
            elif site == "thetvdb":
                result["tvdb"] = ext_id
            elif site == "themoviedb":
                result["tmdb"] = ext_id
            elif site == "anilist/anime" and ext_id.isdigit():
                result["anilist"] = int(ext_id)
        if cache_key:
            id_cache[cache_key] = result
        return result
    except requests.RequestException:
        return {}

def enrich_ids(ids_dict, do_network=True):
    # 0. Manual overrides first - highest priority
    override = get_override_for_ids(ids_dict)
    if override:
        enriched = {**ids_dict, **override}
        enriched["_source"] = "manual_override"
        return enriched

    enriched = {}
    for k in ["mal", "anilist", "kitsu", "anidb", "imdb", "tvdb", "tmdb", "simkl", "title"]:
        if ids_dict.get(k):
            enriched[k] = ids_dict[k]

    if not do_network:
        return enriched

    anizip_result = None
    if enriched.get("anilist"):
        anizip_result = fetch_anizip(anilist_id=enriched["anilist"])
        time.sleep(0.25)
    if not anizip_result and enriched.get("mal"):
        anizip_result = fetch_anizip(mal_id=enriched["mal"])
        time.sleep(0.25)
    
    if anizip_result:
        for k in ["mal", "anilist", "kitsu", "anidb", "imdb", "tvdb", "tmdb", "title"]:
            if anizip_result.get(k) and not enriched.get(k):
                enriched[k] = anizip_result[k]

    if enriched.get("kitsu") and (not enriched.get("anidb") or not enriched.get("mal") or not enriched.get("imdb")):
        km = fetch_kitsu_mappings(str(enriched["kitsu"]))
        time.sleep(0.25)
        for k in ["mal", "anidb", "imdb", "tvdb", "tmdb", "anilist"]:
            if km.get(k) and not enriched.get(k):
                enriched[k] = km[k]

    return enriched

def get_canonical_key(ids):
    enriched = enrich_ids(ids, do_network=False)
    if not enriched.get("mal") and not enriched.get("anilist"):
        enriched = enrich_ids(ids, do_network=True)

    if enriched.get("mal"):
        return f"mal_{enriched['mal']}"
    if enriched.get("anilist"):
        return f"anilist_{enriched['anilist']}"
    if enriched.get("anidb"):
        return f"anidb_{enriched['anidb']}"
    if enriched.get("kitsu"):
        return f"kitsu_{enriched['kitsu']}"
    if enriched.get("simkl"):
        return f"simkl_{enriched['simkl']}"
    return None

# ============== LOADERS ==============
def load_anilist():
    print("-> Fetching AniList Dreamstorm...")
    query = """query ($userName: String) { MediaListCollection(userName: $userName, type: ANIME) { lists { entries { status progress score updatedAt media { id idMal title { romaji english native } } } } } }"""
    headers = {}
    token = os.getenv("ANILIST_TOKEN")
    if token: headers["Authorization"] = f"Bearer {token}"
    r = requests.post("https://graphql.anilist.co", json={"query": query, "variables": {"userName": CONFIG["anilist_username"]}}, headers=headers, timeout=30)
    r.raise_for_status()
    items=[]
    for lst in r.json()["data"]["MediaListCollection"]["lists"]:
        for e in lst["entries"]:
            title = e["media"]["title"]["romaji"] or e["media"]["title"]["english"] or e["media"]["title"]["native"]
            items.append({"platform":"anilist","ids":{"anilist": e["media"]["id"], "mal": e["media"]["idMal"], "title": title},"state":{"status": STATUS_MAP["anilist"].get(e["status"], "plantowatch"), "progress": e["progress"], "score": e["score"] or 0},"updated": datetime.fromtimestamp(e["updatedAt"], tz=timezone.utc) if e["updatedAt"] else datetime.now(timezone.utc), "title": title})
    print(f"   AniList: {len(items)} entries")
    return items

def load_simkl():
    client_id = os.getenv("SIMKL_CLIENT_ID"); token = os.getenv("SIMKL_ACCESS_TOKEN")
    if not client_id or not token: 
        print("-> SIMKL skipped (no secrets)"); return []
    print("-> Fetching SIMKL...")
    headers = {"Authorization": f"Bearer {token}", "simkl-api-key": client_id}
    r = requests.get(f"https://api.simkl.com/sync/all-items/anime?client_id={client_id}", headers=headers, timeout=30)
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

def load_mal():
    token = os.getenv("MAL_ACCESS_TOKEN")
    if not token: 
        print("-> MAL skipped (no token)"); return []
    print("-> Fetching MAL...")
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.myanimelist.net/v2/users/@me/animelist?fields=list_status,num_episodes&limit=1000&nsfw=true"
    r = requests.get(url, headers=headers, timeout=30)
    if not r.ok:
        print(f"   MAL error: {r.text[:300]}"); return []
    items=[]
    for n in r.json().get("data", []):
        ls = n["list_status"]
        title = n["node"].get("title","")
        items.append({"platform":"mal","ids":{"mal": n["node"]["id"], "title": title},"state":{"status": STATUS_MAP["mal"].get(ls["status"], "plantowatch"), "progress": ls["num_episodes_watched"], "score": ls["score"]},"updated": datetime.fromisoformat(ls["updated_at"].replace("Z","+00:00")), "title": title})
    print(f"   MAL: {len(items)} entries")
    return items

def load_kitsu():
    username = os.getenv("KITSU_USERNAME")
    if not username: 
        print("-> Kitsu skipped"); return []
    print(f"-> Fetching Kitsu {username}...")
    try:
        u_resp = requests.get(f"https://kitsu.io/api/edge/users?filter[name]={username}", timeout=15)
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
            r = requests.get(url, timeout=30)
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
                    "platform":"kitsu",
                    "ids":{"kitsu": anime_id, "mal": mal_id, "title": title},
                    "state":{"status": STATUS_MAP["kitsu"].get(a.get("status"), "plantowatch"), "progress": a.get("progress", 0), "score": a.get("ratingTwenty", 0) // 2 if a.get("ratingTwenty") else 0},
                    "updated": datetime.fromisoformat(a["updatedAt"].replace("Z","+00:00")) if a.get("updatedAt") else datetime.now(timezone.utc),
                    "title": title
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
    r = requests.post(f"https://api.simkl.com/sync/add-to-list?client_id={client_id}", json=payload, headers=headers, timeout=15)
    if state["progress"]>0:
        hist = {"shows": [{"ids": ids, "watched_episodes": state["progress"]}]}
        requests.post(f"https://api.simkl.com/sync/history?client_id={client_id}", json=hist, headers=headers, timeout=15)
    print(f"   -> PUSH SIMKL {ids} => {state} [{r.status_code}]")

def push_anilist(entry, state):
    """Create or update an AniList media list entry via SaveMediaListEntry mutation."""
    token = os.getenv("ANILIST_TOKEN")
    if not token:
        print("   -> PUSH AniList skipped (no ANILIST_TOKEN)")
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
    mutation ($mediaId: Int, $status: MediaListStatus, $progress: Int, $score: Float) {
      SaveMediaListEntry(mediaId: $mediaId, status: $status, progress: $progress, score: $score) {
        id
        status
        progress
        score
      }
    }
    """
    variables = {
        "mediaId": media_id,
        "status": status,
        "progress": int(state.get("progress") or 0),
        "score": float(state.get("score") or 0),
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        r = requests.post(
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
    """Placeholder — MAL write support not yet implemented.
    Requires MAL_ACCESS_TOKEN (and usually a client id).
    Endpoint: PUT https://api.myanimelist.net/v2/anime/{id}/my_list_status
    """
    print(f"   -> PUSH MAL placeholder (not implemented): {entry['ids'].get('mal')} => {state}")


def push_kitsu(entry, state):
    """Best-effort Kitsu library-entry update/create.
    Full reliability needs the library-entry ID stored in the DB.
    Requires KITSU_TOKEN (OAuth access token).
    """
    token = os.getenv("KITSU_TOKEN") or os.getenv("KITSU_ACCESS_TOKEN")
    if not token:
        print("   -> PUSH Kitsu skipped (no KITSU_TOKEN)")
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

    # Try to find existing library entry for this anime + current user
    try:
        # First resolve current user id
        me = requests.get(
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
        user_id = users[0]["id"]

        # Look up library entry
        lookup = requests.get(
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
            r = requests.patch(
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
            r = requests.post(
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

def export_csv(file_path=CSV_PATH_DEFAULT):
    entries = db.get("entries", {})
    if not entries:
        print("No entries to export")
        return
    
    fieldnames = ["title", "canonical_key", "mal_id", "anilist_id", "kitsu_id", "anidb_id", "imdb_id", "tvdb_id", "tmdb_id", "simkl_id", "status", "progress", "score", "last_updated", "source"]
    
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for key, data in sorted(entries.items(), key=lambda x: (x[1].get("ids",{}).get("title","") or "").lower()):
            ids = data.get("ids", {})
            state = data.get("state", {})
            writer.writerow({
                "title": ids.get("title") or data.get("title") or "",
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
                "source": ids.get("_source") or ""
            })
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
        # IMDB missing is normal, don't count as unmatched unless it's a movie
        # Only flag as unmatched if missing core pairing
        if (not ids.get("mal") and not ids.get("anilist")) or (not ids.get("anidb") and (ids.get("mal") or ids.get("anilist"))):
            reason = ""
            if not ids.get("mal") and not ids.get("anilist"):
                reason = "isolated - only has " + ",".join([k for k in ["kitsu","simkl"] if ids.get(k)]) + " - needs manual pairing"
            elif not ids.get("anidb"):
                reason = "no anidb mapping yet (too new or not in anizip)"
            else:
                reason = "partial pairing"
            
            unmatched.append({
                "title": ids.get("title") or data.get("title") or "",
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
    
    fieldnames = ["title", "canonical_key", "mal_id", "anilist_id", "kitsu_id", "anidb_id", "imdb_id", "missing", "reason", "existing_ids", "suggested_override_key"]
    
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

def run_once(enrich_new=True, export_csv_flag=False, csv_file=CSV_PATH_DEFAULT, export_unmatched_flag=True):
    all_items=[]
    for loader in [load_anilist, load_simkl, load_mal, load_kitsu]:
        try: all_items.extend(loader())
        except Exception as e: print(f"Loader {loader.__name__} failed: {e}")

    changes=0
    enriched_count=0

    for item in all_items:
        if enrich_new:
            needs_enrich = not item["ids"].get("anidb") or not item["ids"].get("mal") or not item["ids"].get("anilist")
            key_preview = get_canonical_key(item["ids"])
            if not db["entries"].get(key_preview):
                needs_enrich = True
            
            if needs_enrich:
                enriched_ids = enrich_ids(item["ids"], do_network=True)
                for k, v in enriched_ids.items():
                    if v and not k.startswith("_"):
                        if not item["ids"].get(k):
                            item["ids"][k] = v
                    # Keep source tracking
                    if k == "_source":
                        item["ids"]["_source"] = v
                enriched_count += 1
                if enriched_count % 10 == 0:
                    print(f"   Enriched {enriched_count}/{len(all_items)}...")

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
            db["entries"][key] = {
                "ids": item["ids"], 
                "state": item["state"], 
                "last_updated": item["updated"].isoformat(), 
                "last_synced": {item["platform"]: incoming_hash},
                "title": item.get("title","")
            }
            continue
        
        if existing["last_synced"].get(item["platform"]) == incoming_hash:
            for k, v in item["ids"].items():
                if v and not existing["ids"].get(k):
                    existing["ids"][k] = v
            continue
        
        last_updated = datetime.fromisoformat(existing["last_updated"])
        if item["updated"] > last_updated:
            print(f"[CHANGE] {key} newer on {item['platform']} - {item['state']}")
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
                    pusher(existing, item["state"])
                    existing["last_synced"][platform] = incoming_hash
                    changes+=1
                except Exception as e: print(f"Push to {platform} failed: {e}")
        else:
            if existing["last_synced"].get(item["platform"]) != hash_state(existing["state"]):
                print(f"[BACKFILL] {key} -> {item['platform']}")
                try:
                    PUSHERS[item["platform"]](existing, existing["state"])
                    existing["last_synced"][item["platform"]] = hash_state(existing["state"])
                    changes+=1
                except Exception as e: print(e)

    db["id_cache"] = id_cache
    DB_PATH.write_text(json.dumps(db, indent=2), encoding='utf-8')
    try:
        CACHE_PATH.write_text(json.dumps(id_cache, indent=2), encoding='utf-8')
    except OSError:
        pass
    
    anidb_count = sum(1 for e in db["entries"].values() if e["ids"].get("anidb"))
    imdb_count = sum(1 for e in db["entries"].values() if e["ids"].get("imdb"))
    mal_count = sum(1 for e in db["entries"].values() if e["ids"].get("mal"))
    anilist_count = sum(1 for e in db["entries"].values() if e["ids"].get("anilist"))
    kitsu_count = sum(1 for e in db["entries"].values() if e["ids"].get("kitsu"))
    manual_count = sum(1 for e in db["entries"].values() if e["ids"].get("_source") == "manual_override")
    
    print(f"Done. {len(all_items)} total fetched, {len(db['entries'])} unique shows, {changes} pushes.")
    print(f"ID Coverage: MAL:{mal_count} AniList:{anilist_count} Kitsu:{kitsu_count} AniDB:{anidb_count} IMDB:{imdb_count} (manual overrides: {manual_count})")

    if export_csv_flag:
        export_csv(csv_file)
    
    if export_unmatched_flag:
        export_unmatched(UNMATCHED_PATH)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-enrich", action="store_true")
    parser.add_argument("--enrich-all", action="store_true")
    parser.add_argument("--export-csv", action="store_true")
    parser.add_argument("--export-csv-file", default="anime_pairings.csv")
    parser.add_argument("--export-only", action="store_true")
    parser.add_argument("--no-unmatched", action="store_true", help="Skip unmatched report")
    args = parser.parse_args()
    
    if args.export_only:
        export_csv(args.export_csv_file)
        if not args.no_unmatched:
            export_unmatched(UNMATCHED_PATH)
        sys.exit(0)
    
    if args.enrich_all:
        id_cache.clear()
    
    run_once(enrich_new=not args.no_enrich, export_csv_flag=args.export_csv, csv_file=Path(args.export_csv_file), export_unmatched_flag=not args.no_unmatched)
    