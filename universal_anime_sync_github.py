"""
Universal Anime Sync - GitHub Actions version for Dreamstorm
V3 - With full ID pairing (MAL + AniList + Kitsu + AniDB + IMDB + TVDB + TMDB)
Runs once per invocation (for cron), loop-proof with timestamp + hash.

Enable in GitHub: Settings -> Secrets and variables -> Actions

NEW: 
- load_kitsu() now paginates all pages (no 500 cap)
- enrich_ids() uses api.ani.zip + Kitsu mappings + MALSync to pair IDs
- get_key() merges entries that map to same show (kitsu_11 + mal_21 -> same key)
- sync_db.json now stores: {mal, anilist, kitsu, anidb, imdb, tvdb, tmdb, simkl}
"""
import requests, json, os, hashlib, argparse, time
from datetime import datetime, timezone
from pathlib import Path
import sys

DB_PATH = Path("sync_db.json")
CACHE_PATH = Path("id_cache.json")

# Load DB
if DB_PATH.exists():
    try:
        db = json.loads(DB_PATH.read_text())
        if "entries" not in db:
            db = {"entries": db} if isinstance(db, dict) else {"entries": {}}
    except:
        db = {"entries": {}, "id_cache": {}}
else:
    db = {"entries": {}, "id_cache": {}}

# Load ID cache for AniZip results to avoid re-querying every run
if CACHE_PATH.exists():
    try:
        id_cache = json.loads(CACHE_PATH.read_text())
    except:
        id_cache = {}
else:
    id_cache = db.get("id_cache", {})

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
    return hashlib.md5(f"{state['status']}|{state['progress']}|{state['score']}".encode()).hexdigest()

# ============== ID PAIRING LOGIC ==============
def fetch_anizip(anilist_id=None, mal_id=None, use_cache=True):
    """Fetch from api.ani.zip - best hub"""
    cache_key = f"anilist_{anilist_id}" if anilist_id else f"mal_{mal_id}" if mal_id else None
    if use_cache and cache_key and cache_key in id_cache:
        # cache for 30 days
        cached = id_cache[cache_key]
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached.get("_cached_at", "2000-01-01T00:00:00+00:00"))).days
            if age < 30:
                return cached
        except:
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
        result = {
            "anilist": data.get("id") or mappings.get("anilist_id") or anilist_id,
            "mal": mappings.get("mal_id") or mal_id,
            "kitsu": mappings.get("kitsu_id"),
            "anidb": mappings.get("anidb_id"),
            "anidb_id": mappings.get("anidb_id"),
            "imdb": mappings.get("imdb_id"),
            "imdb_id": mappings.get("imdb_id"),
            "tvdb": mappings.get("thetvdb_id"),
            "thetvdb": mappings.get("thetvdb_id"),
            "tmdb": mappings.get("themoviedb_id"),
            "_cached_at": datetime.now(timezone.utc).isoformat(),
            "_source": "anizip"
        }
        # Clean None
        result = {k: v for k, v in result.items() if v}
        if cache_key:
            id_cache[cache_key] = result
        return result
    except Exception as e:
        # print(f" AniZip fail {anilist_id or mal_id}: {e}")
        return None

def fetch_kitsu_mappings(kitsu_anime_id, use_cache=True):
    cache_key = f"kitsu_{kitsu_anime_id}"
    if use_cache and cache_key in id_cache:
        cached = id_cache[cache_key]
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(cached.get("_cached_at", "2000-01-01T00:00:00+00:00"))).days
            if age < 30 and cached.get("_source") == "kitsu_mappings":
                return cached
        except:
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
                except:
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
    except Exception as e:
        return {}

def enrich_ids(ids_dict, do_network=True):
    """
    Take any ids dict like {"anilist":21} or {"kitsu":"11"} 
    Returns enriched dict with mal, anilist, kitsu, anidb, imdb, tvdb, tmdb
    Uses cache first, then network if do_network=True
    """
    # Start with what we have, normalized
    enriched = {}
    for k in ["mal", "anilist", "kitsu", "anidb", "imdb", "tvdb", "tmdb", "simkl", "thetvdb", "imdb_id", "anidb_id"]:
        if ids_dict.get(k):
            enriched[k] = ids_dict[k]
    # Normalize aliases
    if enriched.get("thetvdb") and not enriched.get("tvdb"):
        enriched["tvdb"] = enriched["thetvdb"]
    if enriched.get("anidb_id") and not enriched.get("anidb"):
        enriched["anidb"] = enriched["anidb_id"]
    if enriched.get("imdb_id") and not enriched.get("imdb"):
        enriched["imdb"] = enriched["imdb_id"]

    if not do_network:
        return enriched

    # Try AniZip
    anizip_result = None
    if enriched.get("anilist"):
        anizip_result = fetch_anizip(anilist_id=enriched["anilist"])
        time.sleep(0.25)
    if not anizip_result and enriched.get("mal"):
        anizip_result = fetch_anizip(mal_id=enriched["mal"])
        time.sleep(0.25)
    
    if anizip_result:
        for k in ["mal", "anilist", "kitsu", "anidb", "imdb", "tvdb", "tmdb"]:
            if anizip_result.get(k) and not enriched.get(k):
                enriched[k] = anizip_result[k]

    # If we have kitsu but still missing anidb/imdb/mal/anilist, try Kitsu mappings
    if enriched.get("kitsu") and (not enriched.get("anidb") or not enriched.get("mal") or not enriched.get("imdb")):
        km = fetch_kitsu_mappings(str(enriched["kitsu"]))
        time.sleep(0.25)
        for k in ["mal", "anidb", "imdb", "tvdb", "tmdb", "anilist"]:
            if km.get(k) and not enriched.get(k):
                enriched[k] = km[k]

    return enriched

def get_canonical_key(ids):
    """
    Canonical key for DB, prefers mal > anilist > anidb > kitsu > simkl
    This ensures mal_21 and kitsu_11 (same show) map to same key after enrichment
    """
    enriched = enrich_ids(ids, do_network=False)  # use cached only for key calc initially
    # If not enriched enough, try network enrichment for key stability (only if missing mal/anilist)
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
    query = """query ($userName: String) { MediaListCollection(userName: $userName, type: ANIME) { lists { entries { status progress score updatedAt media { id idMal title { romaji } } } } } }"""
    headers = {}
    token = os.getenv("ANILIST_TOKEN")
    if token: headers["Authorization"] = f"Bearer {token}"
    r = requests.post("https://graphql.anilist.co", json={"query": query, "variables": {"userName": CONFIG["anilist_username"]}}, headers=headers, timeout=30)
    r.raise_for_status()
    items=[]
    for lst in r.json()["data"]["MediaListCollection"]["lists"]:
        for e in lst["entries"]:
            items.append({"platform":"anilist","ids":{"anilist": e["media"]["id"], "mal": e["media"]["idMal"]},"state":{"status": STATUS_MAP["anilist"].get(e["status"], "plantowatch"), "progress": e["progress"], "score": e["score"] or 0},"updated": datetime.fromtimestamp(e["updatedAt"], tz=timezone.utc) if e["updatedAt"] else datetime.now(timezone.utc)})
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
        items.append({
            "platform":"simkl",
            "ids":{"simkl": ids_obj.get("simkl") or ids_obj.get("simkl_id"), "mal": ids_obj.get("mal"), "anilist": ids_obj.get("anilist") or ids_obj.get("anilist_id"), "anidb": ids_obj.get("anidb")},
            "state":{
                "status": STATUS_MAP["simkl"].get(status_raw, "plantowatch"),
                "progress": entry.get("watched_episodes_count") or entry.get("watched_episodes") or show_obj.get("watched_episodes_count",0),
                "score": entry.get("user_rating") or show_obj.get("user_rating") or 0
            },
            "updated": datetime.fromisoformat(last.replace("Z","+00:00")) if last else datetime.now(timezone.utc)
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
        items.append({"platform":"mal","ids":{"mal": n["node"]["id"]},"state":{"status": STATUS_MAP["mal"].get(ls["status"], "plantowatch"), "progress": ls["num_episodes_watched"], "score": ls["score"]},"updated": datetime.fromisoformat(ls["updated_at"].replace("Z","+00:00"))})
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
                mal_id = None
                if anime:
                    mal_id = anime.get("idMal") or anime.get("id_mal")
                items.append({
                    "platform":"kitsu",
                    "ids":{"kitsu": anime_id, "mal": mal_id},
                    "state":{
                        "status": STATUS_MAP["kitsu"].get(a.get("status"), "plantowatch"),
                        "progress": a.get("progress", 0),
                        "score": a.get("ratingTwenty", 0) // 2 if a.get("ratingTwenty") else 0
                    },
                    "updated": datetime.fromisoformat(a["updatedAt"].replace("Z","+00:00")) if a.get("updatedAt") else datetime.now(timezone.utc)
                })
            url = lib.get("links", {}).get("next")
            page+=1
            if page>30:
                break
        print(f"   Kitsu: {len(items)} entries (ALL pages)")
        return items
    except Exception as ex:
        import traceback
        print(f"   Kitsu failed {ex}")
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

def push_anilist(entry, state): print(f"   -> PUSH AniList would: {entry['ids']} => {state} (implement GraphQL mutation)")
def push_mal(entry, state): print(f"   -> PUSH MAL would: {entry['ids']} => {state} (implement PUT /animelist)")
def push_kitsu(entry, state): print(f"   -> PUSH Kitsu would: {entry['ids']} => {state}")

PUSHERS = {"anilist": push_anilist, "mal": push_mal, "kitsu": push_kitsu, "simkl": push_simkl}

def run_once(enrich_new=True):
    all_items=[]
    for loader in [load_anilist, load_simkl, load_mal, load_kitsu]:
        try: all_items.extend(loader())
        except Exception as e: print(f"Loader {loader.__name__} failed: {e}")

    changes=0
    enriched_count=0

    for item in all_items:
        # Enrich IDs via AniZip/Kitsu for full pairing
        if enrich_new:
            # Only enrich if we haven't cached this already, or if missing anidb/imdb
            needs_enrich = not item["ids"].get("anidb") or not item["ids"].get("imdb")
            # For new entries, always enrich
            key_preview = get_canonical_key(item["ids"])
            if not db["entries"].get(key_preview):
                needs_enrich = True
            
            if needs_enrich:
                enriched_ids = enrich_ids(item["ids"], do_network=True)
                # Merge enriched into item
                for k, v in enriched_ids.items():
                    if v and not k.startswith("_"):
                        if not item["ids"].get(k):
                            item["ids"][k] = v
                enriched_count += 1
                if enriched_count % 10 == 0:
                    print(f"   Enriched {enriched_count}/{len(all_items)} for ID pairing...")

        # Now compute canonical key AFTER enrichment (so kitsu_11 and mal_21 become same key)
        key = get_canonical_key(item["ids"])
        if not key: 
            # fallback
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
                "last_synced": {item["platform"]: incoming_hash}
            }
            continue
        
        # Loop protection
        if existing["last_synced"].get(item["platform"]) == incoming_hash:
            # Merge any new IDs discovered
            for k, v in item["ids"].items():
                if v and not existing["ids"].get(k):
                    existing["ids"][k] = v
            continue
        
        last_updated = datetime.fromisoformat(existing["last_updated"])
        if item["updated"] > last_updated:
            print(f"[CHANGE] {key} newer on {item['platform']} - {item['state']} | IDs: {item['ids']}")
            existing["state"] = item["state"]
            existing["last_updated"] = item["updated"].isoformat()
            # Merge all IDs
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

    # Save caches
    db["id_cache"] = id_cache
    DB_PATH.write_text(json.dumps(db, indent=2))
    try:
        CACHE_PATH.write_text(json.dumps(id_cache, indent=2))
    except:
        pass
    
    # Print summary with ID coverage
    anidb_count = sum(1 for e in db["entries"].values() if e["ids"].get("anidb"))
    imdb_count = sum(1 for e in db["entries"].values() if e["ids"].get("imdb"))
    mal_count = sum(1 for e in db["entries"].values() if e["ids"].get("mal"))
    anilist_count = sum(1 for e in db["entries"].values() if e["ids"].get("anilist"))
    kitsu_count = sum(1 for e in db["entries"].values() if e["ids"].get("kitsu"))
    
    print(f"Done. {len(all_items)} total fetched, {len(db['entries'])} unique shows, {changes} pushes.")
    print(f"ID Coverage: MAL:{mal_count} AniList:{anilist_count} Kitsu:{kitsu_count} AniDB:{anidb_count} IMDB:{imdb_count}")
    print(f"DB saved to {DB_PATH}, cache saved to {CACHE_PATH}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-enrich", action="store_true", help="Skip AniZip enrichment (faster)")
    parser.add_argument("--enrich-all", action="store_true", help="Force enrich ALL entries (slow, hits API a lot)")
    args = parser.parse_args()
    
    if args.enrich_all:
        # Clear cache to force re-fetch
        id_cache.clear()
    
    run_once(enrich_new=not args.no_enrich)
    