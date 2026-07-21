"""
Universal Anime Sync - GitHub Actions version for Dreamstorm
Runs once per invocation (for cron), loop-proof with timestamp + hash.

Enable in GitHub: Settings -> Secrets and variables -> Actions
"""
import requests, json, os, hashlib, argparse
from datetime import datetime, timezone
from pathlib import Path
import sys

DB_PATH = Path("sync_db.json")
if DB_PATH.exists():
    try:
        db = json.loads(DB_PATH.read_text())
    except:
        db = {"entries": {}}
else:
    db = {"entries": {}}

CONFIG = {
    "anilist_username": os.getenv("ANILIST_USERNAME", "Dreamstorm"),
    "mal_username": os.getenv("MAL_USERNAME", ""),
    "kitsu_username": os.getenv("KITSU_USERNAME", ""),
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
    # API can return list directly or dict with 'anime' key
    raw_list = data if isinstance(data, list) else data.get("anime", []) or data.get("shows", []) or []
    items=[]
    for entry in raw_list:
        # entry can be {"show": {"ids":...}, "status":...} or direct
        show_obj = entry.get("show", entry) if isinstance(entry, dict) else {}
        ids_obj = entry.get("ids") or show_obj.get("ids") or {}
        if not ids_obj:
            continue
        last = entry.get("last_watched_at") or entry.get("last_updated_at") or show_obj.get("last_watched_at")
        status_raw = entry.get("status") or show_obj.get("status") or "plantowatch"
        items.append({
            "platform":"simkl",
            "ids":{"simkl": ids_obj.get("simkl") or ids_obj.get("simkl_id"), "mal": ids_obj.get("mal"), "anilist": ids_obj.get("anilist") or ids_obj.get("anidb")},
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
        u = requests.get(f"https://kitsu.io/api/edge/users?filter[name]={username}", timeout=15).json()
        user_id = u["data"][0]["id"]
        lib = requests.get(f"https://kitsu.io/api/edge/users/{user_id}/library-entries?filter[kind]=anime&page[limit]=500&include=anime", timeout=15).json()
        id_map = {a["id"]: a["attributes"] for a in lib.get("included", []) if a["type"]=="anime"}
        items=[]
        for e in lib["data"]:
            a = e["attributes"]
            anime = id_map.get(e["relationships"]["anime"]["data"]["id"], {})
            items.append({"platform":"kitsu","ids":{"kitsu": e["relationships"]["anime"]["data"]["id"], "mal": anime.get("idMal")},"state":{"status": STATUS_MAP["kitsu"].get(a["status"], "plantowatch"), "progress": a["progress"], "score": 0},"updated": datetime.fromisoformat(a["updatedAt"].replace("Z","+00:00"))})
        print(f"   Kitsu: {len(items)} entries")
        return items
    except Exception as ex:
        print(f"   Kitsu failed {ex}"); return []

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

# Stubs for other platforms - fill when ready
def push_anilist(entry, state): print(f"   -> PUSH AniList would: {entry['ids']} => {state} (implement GraphQL mutation)")
def push_mal(entry, state): print(f"   -> PUSH MAL would: {entry['ids']} => {state} (implement PUT /animelist)")
def push_kitsu(entry, state): print(f"   -> PUSH Kitsu would: {entry['ids']} => {state}")

PUSHERS = {"anilist": push_anilist, "mal": push_mal, "kitsu": push_kitsu, "simkl": push_simkl}

def get_key(ids):
    if ids.get("mal"): return f"mal_{ids['mal']}"
    if ids.get("anilist"): return f"anilist_{ids['anilist']}"
    if ids.get("kitsu"): return f"kitsu_{ids['kitsu']}"
    if ids.get("simkl"): return f"simkl_{ids['simkl']}"
    return None

def run_once():
    all_items=[]
    for loader in [load_anilist, load_simkl, load_mal, load_kitsu]:
        try: all_items.extend(loader())
        except Exception as e: print(f"Loader {loader.__name__} failed: {e}")

    changes=0
    for item in all_items:
        key = get_key(item["ids"])
        if not key: continue
        existing = db["entries"].get(key)
        incoming_hash = hash_state(item["state"])
        if not existing:
            db["entries"][key] = {"ids": item["ids"], "state": item["state"], "last_updated": item["updated"].isoformat(), "last_synced": {item["platform"]: incoming_hash}}
            continue
        # Loop protection
        if existing["last_synced"].get(item["platform"]) == incoming_hash:
            existing["ids"].update({k:v for k,v in item["ids"].items() if v})
            continue
        last_updated = datetime.fromisoformat(existing["last_updated"])
        if item["updated"] > last_updated:
            print(f"[CHANGE] {key} newer on {item['platform']} - {item['state']}")
            existing["state"] = item["state"]
            existing["last_updated"] = item["updated"].isoformat()
            existing["ids"].update({k:v for k,v in item["ids"].items() if v})
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
            # DB newer than this platform - sync back
            if existing["last_synced"].get(item["platform"]) != hash_state(existing["state"]):
                print(f"[BACKFILL] {key} -> {item['platform']}")
                try:
                    PUSHERS[item["platform"]](existing, existing["state"])
                    existing["last_synced"][item["platform"]] = hash_state(existing["state"])
                    changes+=1
                except Exception as e: print(e)

    DB_PATH.write_text(json.dumps(db, indent=2))
    print(f"Done. {len(all_items)} total, {changes} pushes. DB saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    run_once()
