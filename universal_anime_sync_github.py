"""
Universal Anime Sync V3.13 - Automated offline DB sync
- Fribb anime-list-mini: auto-download + refresh when stale (IMDb/TVDB/TMDB)
- Manami anime-offline-database: auto-download + refresh for title backfill
- apply_offline_ids_to_db + apply_offline_titles_to_db every sync
- ARM + AniZip + Kitsu mappings for remaining gaps
"""

import requests, json, os, hashlib, argparse, time, csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import sys
import re
import sqlite3

DB_PATH = Path("sync_db.json")          # legacy JSON (still written as backup)
CACHE_PATH = Path("id_cache.json")      # legacy JSON (still written as backup)
SQLITE_PATH = Path("sync.db")           # primary store
FRIBB_PATH = Path("anime-list-mini.json")  # offline MAL/AniList→IMDb/TVDB/TMDB
FRIBB_URL = "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-mini.json"
MANAMI_PATH = Path("anime-offline-database-minified.json")  # titles + cross IDs
MANAMI_URL = (
    "https://github.com/manami-project/anime-offline-database/releases/download/"
    "latest/anime-offline-database-minified.json"
)
# Refresh offline dumps if older than this (seconds). 7 days.
OFFLINE_MAX_AGE_SEC = 7 * 24 * 3600
CSV_PATH_DEFAULT = Path("anime_pairings.csv")
UNMATCHED_PATH = Path("unmatched.csv")
OVERRIDES_PATH = Path("manual_overrides.json")


def _init_sqlite(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS entries (
            canonical_key TEXT PRIMARY KEY,
            data TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS id_cache (
            cache_key TEXT PRIMARY KEY,
            data TEXT NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()


def load_db():
    """Load entries + id_cache from SQLite (migrating from JSON on first run)."""
    conn = sqlite3.connect(SQLITE_PATH)
    _init_sqlite(conn)

    # Check if SQLite already has data
    count = conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]

    if count == 0 and DB_PATH.exists():
        # One-time migration from legacy JSON
        print(f"-> Migrating {DB_PATH} → {SQLITE_PATH} ...")
        try:
            raw = json.loads(DB_PATH.read_text(encoding="utf-8"))
            if "entries" not in raw:
                raw = {"entries": raw if isinstance(raw, dict) else {}, "id_cache": {}}
            entries = raw.get("entries", {})
            cache = raw.get("id_cache", {})
            # Also pull standalone id_cache.json if present
            if CACHE_PATH.exists():
                try:
                    cache.update(json.loads(CACHE_PATH.read_text(encoding="utf-8")))
                except (json.JSONDecodeError, OSError):
                    pass
            with conn:
                for k, v in entries.items():
                    conn.execute(
                        "INSERT OR REPLACE INTO entries (canonical_key, data) VALUES (?, ?)",
                        (k, json.dumps(v, ensure_ascii=False)),
                    )
                for k, v in cache.items():
                    conn.execute(
                        "INSERT OR REPLACE INTO id_cache (cache_key, data) VALUES (?, ?)",
                        (k, json.dumps(v, ensure_ascii=False)),
                    )
            print(f"   Migrated {len(entries)} entries, {len(cache)} cache keys")
        except Exception as e:
            print(f"   Migration warning: {e}")

    # Load into memory (same interface as before)
    entries = {}
    for row in conn.execute("SELECT canonical_key, data FROM entries"):
        try:
            entries[row[0]] = json.loads(row[1])
        except json.JSONDecodeError:
            continue
    cache = {}
    for row in conn.execute("SELECT cache_key, data FROM id_cache"):
        try:
            cache[row[0]] = json.loads(row[1])
        except json.JSONDecodeError:
            continue
    conn.close()
    return {"entries": entries, "id_cache": cache}, cache


def save_db(db_obj, cache_obj, write_json_backup=True):
    """Persist in-memory db + id_cache to SQLite via atomic replace.

    Writes to a temporary DB file, then os.replace() so a crash mid-write
    never leaves an empty/corrupt primary store.
    """
    tmp_path = SQLITE_PATH.with_suffix(".db.tmp")
    if tmp_path.exists():
        tmp_path.unlink()
    conn = sqlite3.connect(tmp_path)
    _init_sqlite(conn)
    with conn:
        for k, v in db_obj.get("entries", {}).items():
            # Normalize IDs before persisting
            if isinstance(v, dict) and "ids" in v:
                v = dict(v)
                v["ids"] = normalize_ids(v.get("ids") or {})
            conn.execute(
                "INSERT INTO entries (canonical_key, data) VALUES (?, ?)",
                (k, json.dumps(v, ensure_ascii=False)),
            )
        for k, v in cache_obj.items():
            conn.execute(
                "INSERT INTO id_cache (cache_key, data) VALUES (?, ?)",
                (k, json.dumps(v, ensure_ascii=False)),
            )
    conn.close()
    os.replace(tmp_path, SQLITE_PATH)

    if write_json_backup:
        try:
            DB_PATH.write_text(json.dumps(db_obj, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError:
            pass
        try:
            CACHE_PATH.write_text(json.dumps(cache_obj, indent=2, ensure_ascii=False), encoding="utf-8")
        except OSError:
            pass


# Lazy globals — populated by ensure_loaded() so importing this module is side-effect free
db = {"entries": {}, "id_cache": {}}
id_cache = {}
manual_overrides = {}
_loaded = False
_kitsu_user_id = None  # cached for push_kitsu
_kitsu_access_token = None  # filled by ensure_kitsu_token()
_mal_token_refreshed = False  # ensure we only refresh once per process
_push_skip_logged = set()  # log missing-token skips once per platform


def ensure_loaded():
    """Load DB, cache, and overrides once per process."""
    global db, id_cache, manual_overrides, _loaded
    if _loaded:
        return
    db, id_cache = load_db()
    if OVERRIDES_PATH.exists():
        try:
            manual_overrides = json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
            real = [k for k in manual_overrides if not k.startswith("_")]
            print(f"Loaded {len(real)} manual overrides from {OVERRIDES_PATH}")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Failed to load overrides: {e}")
            manual_overrides = {}
    else:
        manual_overrides = {
            "_comment": "Manual overrides for shows that APIs can't auto-pair.",
            "_how_to": "Key = any existing ID like kitsu_12345 or mal_12345. Value = full IDs to force.",
        }
        OVERRIDES_PATH.write_text(json.dumps(manual_overrides, indent=2), encoding="utf-8")
        print(f"Created empty {OVERRIDES_PATH}")
    # Normalize IDs already in the DB
    for entry in db.get("entries", {}).values():
        if "ids" in entry:
            entry["ids"] = normalize_ids(entry["ids"])
    _loaded = True


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

def hash_state(state):
    # Using SHA256 for state change detection (non-cryptographic use)
    payload = f"{state['status']}|{state['progress']}|{state['score']}"
    return hashlib.sha256(payload.encode()).hexdigest()[:32]

def normalize_ids(ids_dict):
    """Force all known ID fields to str (or None). Prevents str/int key mismatches."""
    if not ids_dict:
        return {}
    out = dict(ids_dict)
    for k in ("mal", "anilist", "kitsu", "anidb", "simkl", "tvdb", "tmdb"):
        if k in out and out[k] is not None and out[k] != "":
            out[k] = str(out[k])
    return out



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

_fribb_index = None  # mal/anilist/kitsu/anidb -> external ids
_manami_title_index = None  # {mal|anilist|kitsu} -> title


def _normalize_imdb(val):
    """Normalize IMDb id to ttXXXXXXX string."""
    if val is None or val == "":
        return None
    if isinstance(val, list):
        val = val[0] if val else None
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.isdigit():
        return f"tt{s}"
    if not s.startswith("tt"):
        return f"tt{s}"
    return s


def _normalize_tmdb(val):
    """Fribb stores themoviedb_id as int or {tv: id}/{movie: id}."""
    if val is None or val == "":
        return None
    if isinstance(val, list):
        val = val[0] if val else None
    if isinstance(val, dict):
        val = val.get("tv") or val.get("movie") or next(iter(val.values()), None)
    if val is None:
        return None
    s = str(val).strip()
    return s if s and s != "None" else None



def ensure_offline_file(path, url, label, max_age_sec=OFFLINE_MAX_AGE_SEC, force=False):
    """Download offline JSON if missing or older than max_age_sec.

    Returns True if a file is present and usable afterward.
    """
    path = Path(path)
    need = force or not path.exists()
    if not need and path.exists():
        age = time.time() - path.stat().st_mtime
        if age > max_age_sec:
            need = True
            print(f"-> {label} is {age/86400:.1f}d old (>{max_age_sec/86400:.0f}d) — refreshing")
    if not need:
        return True
    print(f"-> Downloading {label} ({url}) ...")
    try:
        r = requests.get(url, timeout=180, allow_redirects=True)
        r.raise_for_status()
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(r.content)
        tmp.replace(path)
        print(f"   Saved {path.name} ({len(r.content)//1024} KB)")
        return True
    except requests.RequestException as e:
        if path.exists():
            print(f"   {label} download failed ({e}); using existing file")
            return True
        print(f"   {label} download failed: {e}")
        return False


def load_fribb_index(force=False):
    """Load Fribb anime-list-mini into memory indexes (download once if missing)."""
    global _fribb_index
    if _fribb_index is not None and not force:
        return _fribb_index

    if not ensure_offline_file(FRIBB_PATH, FRIBB_URL, "Fribb anime-list-mini", force=force):
        _fribb_index = {}
        return _fribb_index

    try:
        data = json.loads(FRIBB_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        print(f"   Fribb load failed: {e}")
        _fribb_index = {}
        return _fribb_index

    idx = {"mal": {}, "anilist": {}, "kitsu": {}, "anidb": {}}
    for row in data:
        ext = {
            "imdb": _normalize_imdb(row.get("imdb_id")),
            "tvdb": str(row["tvdb_id"]) if row.get("tvdb_id") not in (None, "") else None,
            "tmdb": _normalize_tmdb(row.get("themoviedb_id")),
            "mal": str(row["mal_id"]) if row.get("mal_id") not in (None, "") else None,
            "anilist": str(row["anilist_id"]) if row.get("anilist_id") not in (None, "") else None,
            "kitsu": str(row["kitsu_id"]) if row.get("kitsu_id") not in (None, "") else None,
            "anidb": str(row["anidb_id"]) if row.get("anidb_id") not in (None, "") else None,
            "_source": "fribb",
        }
        ext = {k: v for k, v in ext.items() if v}
        for key_name, index_name in (("mal_id", "mal"), ("anilist_id", "anilist"), ("kitsu_id", "kitsu"), ("anidb_id", "anidb")):
            vid = row.get(key_name)
            if vid not in (None, ""):
                idx[index_name][str(vid)] = ext
    _fribb_index = idx
    print(f"   Offline index: mal={len(idx['mal'])} anilist={len(idx['anilist'])} kitsu={len(idx['kitsu'])}")
    return _fribb_index


def fetch_fribb(ids_dict):
    """Lookup IMDb/TVDB/TMDB (and fill other IDs) from offline Fribb list."""
    idx = load_fribb_index()
    if not idx:
        return {}
    for source in ("mal", "anilist", "kitsu", "anidb"):
        vid = ids_dict.get(source)
        if vid and str(vid) in idx.get(source, {}):
            return dict(idx[source][str(vid)])
    return {}


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



def _arm_pick_source(ids_dict):
    """Choose best ARM query key from a partial ids dict. Prefer MAL/AniList over Kitsu/AniDB."""
    if ids_dict.get("anilist") is not None and str(ids_dict.get("anilist")).strip() != "":
        return "anilist", ids_dict["anilist"], f"arm_anilist_{ids_dict['anilist']}"
    if ids_dict.get("mal") is not None and str(ids_dict.get("mal")).strip() != "":
        return "myanimelist", ids_dict["mal"], f"arm_mal_{ids_dict['mal']}"
    if ids_dict.get("kitsu") is not None and str(ids_dict.get("kitsu")).strip() != "":
        return "kitsu", ids_dict["kitsu"], f"arm_kitsu_{ids_dict['kitsu']}"
    if ids_dict.get("anidb") is not None and str(ids_dict.get("anidb")).strip() != "":
        return "anidb", ids_dict["anidb"], f"arm_anidb_{ids_dict['anidb']}"
    return None, None, None


def _arm_normalize_entry(data, source_tag="arm"):
    """Map ARM v1/v2 response fields into our id schema."""
    if not data or not isinstance(data, dict):
        return {}
    result = {
        "anilist": data.get("anilist"),
        "mal": data.get("myanimelist") if data.get("myanimelist") is not None else data.get("mal"),
        "kitsu": data.get("kitsu"),
        "anidb": data.get("anidb"),
        "simkl": data.get("simkl") or data.get("animecountdown"),
        "imdb": _normalize_imdb(data.get("imdb")) if data.get("imdb") else None,
        "tvdb": data.get("thetvdb") if data.get("thetvdb") is not None else data.get("tvdb"),
        "tmdb": data.get("themoviedb") if data.get("themoviedb") is not None else data.get("tmdb"),
        "_cached_at": datetime.now(timezone.utc).isoformat(),
        "_source": source_tag,
    }
    return {k: v for k, v in result.items() if v is not None and v != ""}


def fetch_arm(ids_dict, use_cache=True):
    """Lookup cross-IDs via ARM v2 (relations.yuna.moe).

    Prefers GET /api/v2/ids for SIMKL/IMDB/TVDB/TMDB + core IDs.
    Falls back to POST /api/ids (v1) if v2 fails.
    """
    source, raw_id, cache_key = _arm_pick_source(ids_dict)
    if not source:
        return {}

    if use_cache and cache_key and cache_key in id_cache:
        cached = id_cache[cache_key]
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                cached.get("_cached_at", "2000-01-01T00:00:00+00:00")
            )).days
            if age < 30:
                return cached
        except (ValueError, TypeError):
            pass

    id_param = int(raw_id) if str(raw_id).isdigit() else raw_id
    result = {}
    try:
        r = requests.get(
            "https://relations.yuna.moe/api/v2/ids",
            params={"source": source, "id": id_param},
            timeout=15,
        )
        if r.ok:
            result = _arm_normalize_entry(r.json(), source_tag="arm_v2")
    except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError):
        result = {}

    if not result or (not result.get("mal") and not result.get("anilist") and source in ("kitsu", "anidb")):
        # v1 often enough for core four; also used when v2 returns sparse
        try:
            body = {source: id_param}
            r = requests.post("https://relations.yuna.moe/api/ids", json=body, timeout=15)
            if r.ok:
                v1 = _arm_normalize_entry(r.json(), source_tag="arm_v1")
                for k, v in v1.items():
                    if k.startswith("_"):
                        continue
                    if v and not result.get(k):
                        result[k] = v
                if not result.get("_source"):
                    result["_source"] = "arm_v1"
                result["_cached_at"] = datetime.now(timezone.utc).isoformat()
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError):
            pass

    if cache_key and len([k for k in result if not k.startswith("_")]) >= 1:
        id_cache[cache_key] = result
    return result


def fetch_arm_batch(ids_list, use_cache=True):
    """Batch ARM v1 lookups. ids_list is a list of ids dicts; returns list of result dicts (same length).

    Uses POST /api/ids with an array body. Cached entries are reused.
    """
    results = [{} for _ in ids_list]
    to_fetch = []  # (index, body_obj, cache_key)

    for i, ids_dict in enumerate(ids_list):
        source, raw_id, cache_key = _arm_pick_source(ids_dict or {})
        if not source:
            continue
        if use_cache and cache_key and cache_key in id_cache:
            cached = id_cache[cache_key]
            try:
                age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                    cached.get("_cached_at", "2000-01-01T00:00:00+00:00")
                )).days
                if age < 30:
                    results[i] = cached
                    continue
            except (ValueError, TypeError):
                pass
        id_param = int(raw_id) if str(raw_id).isdigit() else raw_id
        to_fetch.append((i, {source: id_param}, cache_key))

    # Chunk to keep payloads reasonable
    chunk_size = 50
    for start in range(0, len(to_fetch), chunk_size):
        chunk = to_fetch[start:start + chunk_size]
        bodies = [b for _, b, _ in chunk]
        try:
            r = requests.post(
                "https://relations.yuna.moe/api/ids",
                json=bodies,
                timeout=30,
            )
            if not r.ok:
                continue
            payload = r.json()
            if not isinstance(payload, list):
                payload = [payload]
            for (idx, _body, cache_key), data in zip(chunk, payload):
                if not data:
                    continue
                norm = _arm_normalize_entry(data, source_tag="arm_batch")
                results[idx] = norm
                if cache_key and norm:
                    id_cache[cache_key] = norm
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError):
            continue
        time.sleep(0.15)

    return results




def load_manami_title_index(force=False):
    """Load manami offline DB into id -> detail dict indexes (title, year, format, …)."""
    global _manami_title_index
    if _manami_title_index is not None and not force:
        return _manami_title_index

    if not ensure_offline_file(MANAMI_PATH, MANAMI_URL, "Manami anime-offline-database", force=force):
        _manami_title_index = {"mal": {}, "anilist": {}, "kitsu": {}}
        return _manami_title_index

    try:
        raw = json.loads(MANAMI_PATH.read_text(encoding="utf-8"))
        data = raw if isinstance(raw, list) else (raw.get("data") or [])
    except (OSError, json.JSONDecodeError) as e:
        print(f"   Manami load failed: {e}")
        _manami_title_index = {"mal": {}, "anilist": {}, "kitsu": {}}
        return _manami_title_index

    def _pick_english(title, synonyms):
        en_hints = {
            "the","a","an","of","and","or","to","in","on","for","with","from",
            "season","part","movie","series","story","attack","titan","cowboy",
            "bebop","hero","academia","piece","demon","slayer","sword","art",
        }
        best, best_score = None, -10
        for s in synonyms or []:
            if not s or not isinstance(s, str):
                continue
            s = s.strip()
            if s == title or len(s) < 5 or not s.isascii():
                continue
            if re.fullmatch(r"[A-Z0-9]{2,6}", s):
                continue
            if re.search(r"[àèéìòùáéíóúäöüßñç]", s, re.I):
                continue
            tokens = re.findall(r"[A-Za-z]+", s.lower())
            if not tokens:
                continue
            score = sum(3 for t in tokens if t in en_hints) + min(len(tokens), 6)
            if " " in s:
                score += 2
            if score > best_score:
                best_score, best = score, s
        return best

    def _pick_native(synonyms):
        cjk = None
        for s in synonyms or []:
            if not s:
                continue
            if re.search(r"[\u3040-\u30ff]", s):
                return s
            if re.search(r"[\u4e00-\u9fff]", s) and not cjk:
                cjk = s
        return cjk

    idx = {"mal": {}, "anilist": {}, "kitsu": {}}
    for e in data:
        title = e.get("title") or ""
        if not title:
            continue
        syn = e.get("synonyms") or []
        details = {
            "title": title,
            "title_romaji": title,
            "title_english": _pick_english(title, syn),
            "title_native": _pick_native(syn),
            "year": (e.get("animeSeason") or {}).get("year"),
            "season": (e.get("animeSeason") or {}).get("season"),
            "format": e.get("type"),
            "episodes": e.get("episodes"),
        }
        for src in e.get("sources") or []:
            s = str(src)
            if "myanimelist.net/anime/" in s:
                idx["mal"][s.rstrip("/").split("/")[-1]] = details
            elif "anilist.co/anime/" in s:
                idx["anilist"][s.rstrip("/").split("/")[-1]] = details
            elif "kitsu.app/anime/" in s or "kitsu.io/anime/" in s:
                idx["kitsu"][s.rstrip("/").split("/")[-1]] = details
    _manami_title_index = idx
    print(
        f"   Manami titles: mal={len(idx['mal'])} anilist={len(idx['anilist'])} kitsu={len(idx['kitsu'])}"
    )
    return _manami_title_index



    if not ensure_offline_file(MANAMI_PATH, MANAMI_URL, "Manami anime-offline-database", force=force):
        _manami_title_index = {"mal": {}, "anilist": {}, "kitsu": {}}
        return _manami_title_index

    try:
        raw = json.loads(MANAMI_PATH.read_text(encoding="utf-8"))
        data = raw if isinstance(raw, list) else (raw.get("data") or [])
    except (OSError, json.JSONDecodeError) as e:
        print(f"   Manami load failed: {e}")
        _manami_title_index = {"mal": {}, "anilist": {}, "kitsu": {}}
        return _manami_title_index

    idx = {"mal": {}, "anilist": {}, "kitsu": {}}
    for e in data:
        title = e.get("title") or ""
        if not title:
            continue
        for src in e.get("sources") or []:
            s = str(src)
            if "myanimelist.net/anime/" in s:
                idx["mal"][s.rstrip("/").split("/")[-1]] = title
            elif "anilist.co/anime/" in s:
                idx["anilist"][s.rstrip("/").split("/")[-1]] = title
            elif "kitsu.app/anime/" in s or "kitsu.io/anime/" in s:
                idx["kitsu"][s.rstrip("/").split("/")[-1]] = title
    _manami_title_index = idx
    print(
        f"   Manami titles: mal={len(idx['mal'])} anilist={len(idx['anilist'])} kitsu={len(idx['kitsu'])}"
    )
    return _manami_title_index


def apply_offline_titles_to_db():
    """Fill blank titles and title details from Manami offline DB (no live API)."""
    ensure_loaded()
    idx = load_manami_title_index()
    filled = 0
    detail_filled = 0
    entries = db.get("entries") or {}
    detail_fields = (
        "title", "title_english", "title_romaji", "title_native",
        "year", "season", "format", "episodes",
    )
    for key, data in list(entries.items()):
        ids = dict(data.get("ids") or {})
        det = None
        if ids.get("mal"):
            det = idx["mal"].get(str(ids["mal"]))
        if not det and ids.get("anilist"):
            det = idx["anilist"].get(str(ids["anilist"]))
        if not det and ids.get("kitsu"):
            det = idx["kitsu"].get(str(ids["kitsu"]))
        if not det:
            continue
        changed = False
        for field in detail_fields:
            val = det.get(field)
            if val is None or val == "":
                continue
            if not ids.get(field):
                ids[field] = val
                changed = True
            if not data.get(field):
                data[field] = val
                changed = True
        if det.get("title"):
            if not data.get("title"):
                data["title"] = det["title"]
                changed = True
            if not ids.get("title"):
                ids["title"] = det["title"]
                changed = True
        if changed:
            data["ids"] = ids
            entries[key] = data
            filled += 1
            if det.get("year") or det.get("format"):
                detail_filled += 1
    if filled:
        db["entries"] = entries
        print(f"   Offline titles/details applied: {filled} (with meta≈{detail_filled})")
    else:
        print("   Offline titles: nothing to fill")
    return filled




def apply_offline_ids_to_db():
    """Fill missing imdb/tvdb/tmdb (and cross IDs) on every stored entry via Fribb.

    Runs every sync — offline after first download, so it is cheap and reliable.
    """
    ensure_loaded()
    load_fribb_index()
    filled = {"imdb": 0, "tvdb": 0, "tmdb": 0, "other": 0}
    for entry in db.get("entries", {}).values():
        ids = entry.get("ids") or {}
        fribb = fetch_fribb(ids)
        if not fribb:
            continue
        for k in ("imdb", "tvdb", "tmdb", "mal", "anilist", "kitsu", "anidb"):
            if fribb.get(k) and not ids.get(k):
                ids[k] = fribb[k]
                if k in filled:
                    filled[k] += 1
                else:
                    filled["other"] += 1
        entry["ids"] = ids
    print(
        f"-> Offline Fribb fill: +imdb={filled['imdb']} +tvdb={filled['tvdb']} "
        f"+tmdb={filled['tmdb']} +other={filled['other']}"
    )
    return filled


def is_fully_resolved(ids):
    """True when the entry has the core IDs we consider complete enough to skip network enrichment."""
    has_core = bool(ids.get("mal") or ids.get("anilist"))
    has_secondary = bool(ids.get("anidb") or ids.get("kitsu"))
    return has_core and has_secondary


def enrich_ids_batch(items_needing_enrich, max_workers=4):
    """Enrich multiple items: ARM batch pre-warm, then concurrent Kitsu/AniZip fill.

    Returns a list of enriched id dicts in the same order as input.
    """
    if not items_needing_enrich:
        return []

    # Pre-warm id_cache with one batched ARM v1 call (chunked inside fetch_arm_batch)
    try:
        seed_ids = [it.get("ids") or {} for it in items_needing_enrich]
        arm_hits = fetch_arm_batch(seed_ids, use_cache=True)
        hit_n = sum(1 for h in arm_hits if h and (h.get("mal") or h.get("anilist")))
        print(f"   ARM batch: {hit_n}/{len(seed_ids)} filled core MAL/AniList from cache/API")
    except Exception as e:
        print(f"   ARM batch pre-warm skipped: {e}")

    results = [None] * len(items_needing_enrich)

    def _work(idx_item):
        idx, item = idx_item
        try:
            enriched = enrich_ids(item["ids"], do_network=True)
            return idx, enriched
        except Exception as e:
            print(f"   Enrich error for {item.get('ids')}: {e}")
            return idx, item["ids"]

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_work, (i, it)) for i, it in enumerate(items_needing_enrich)]
        for fut in as_completed(futures):
            idx, enriched = fut.result()
            results[idx] = enriched
            time.sleep(0.05)

    return results

def enrich_ids(ids_dict, do_network=True):
    # 0. Manual overrides first - highest priority
    override = get_override_for_ids(ids_dict)
    if override:
        enriched = {**ids_dict, **override}
        enriched["_source"] = "manual_override"
        return normalize_ids(enriched)

    enriched = {}
    for k in ["mal", "anilist", "kitsu", "anidb", "imdb", "tvdb", "tmdb", "simkl", "title"]:
        if ids_dict.get(k):
            enriched[k] = ids_dict[k]

    # Offline Fribb pass (IMDb / TVDB / TMDB + cross IDs) — no network if file cached
    fribb = fetch_fribb(enriched)
    if fribb:
        for k in ["mal", "anilist", "kitsu", "anidb", "imdb", "tvdb", "tmdb"]:
            if fribb.get(k) and not enriched.get(k):
                enriched[k] = fribb[k]

    if not do_network:
        return normalize_ids(enriched)

    # 1) ARM v2 (core + SIMKL/IMDB/TVDB/TMDB) when any core ID is missing or externals empty
    needs_arm = (
        not enriched.get("mal")
        or not enriched.get("anilist")
        or not enriched.get("anidb")
        or not enriched.get("kitsu")
        or not enriched.get("imdb")
        or not enriched.get("tvdb")
        or not enriched.get("simkl")
    )
    if needs_arm and (enriched.get("mal") or enriched.get("anilist") or enriched.get("kitsu") or enriched.get("anidb")):
        arm = fetch_arm(enriched, use_cache=True)
        time.sleep(0.05)
        if arm:
            for k in ["mal", "anilist", "kitsu", "anidb", "simkl", "imdb", "tvdb", "tmdb"]:
                if arm.get(k) and not enriched.get(k):
                    enriched[k] = arm[k]
            if arm.get("_source"):
                enriched.setdefault("_source", arm["_source"])

    # 2) Kitsu mappings — strong for seasonal titles ARM has not indexed yet
    if enriched.get("kitsu") and (
        not enriched.get("mal")
        or not enriched.get("anilist")
        or not enriched.get("anidb")
        or not enriched.get("imdb")
    ):
        km = fetch_kitsu_mappings(str(enriched["kitsu"]))
        time.sleep(0.2)
        for k in ["mal", "anilist", "anidb", "imdb", "tvdb", "tmdb"]:
            if km.get(k) and not enriched.get(k):
                enriched[k] = km[k]
        if km.get("_source"):
            enriched.setdefault("_source", km["_source"])

    # 3) AniZip for remaining gaps (title + externals)
    needs_anizip = (
        not enriched.get("anidb")
        or not enriched.get("imdb")
        or not enriched.get("tvdb")
        or not enriched.get("mal")
        or not enriched.get("anilist")
        or not enriched.get("title")
    )
    anizip_result = None
    if needs_anizip and enriched.get("anilist"):
        anizip_result = fetch_anizip(anilist_id=enriched["anilist"])
        time.sleep(0.2)
    if needs_anizip and not anizip_result and enriched.get("mal"):
        anizip_result = fetch_anizip(mal_id=enriched["mal"])
        time.sleep(0.2)

    if anizip_result:
        for k in ["mal", "anilist", "kitsu", "anidb", "imdb", "tvdb", "tmdb", "title"]:
            if anizip_result.get(k) and not enriched.get(k):
                val = anizip_result[k]
                if k == "imdb":
                    val = _normalize_imdb(val)
                enriched[k] = val

    return normalize_ids(enriched)

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
    r = requests.post(
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
        r = requests.post(
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
        r = requests.get(url, headers=headers, timeout=30)
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
    r = requests.post(f"https://api.simkl.com/sync/add-to-list?client_id={client_id}", json=payload, headers=headers, timeout=15)
    if state["progress"]>0:
        hist = {"shows": [{"ids": ids, "watched_episodes": state["progress"]}]}
        requests.post(f"https://api.simkl.com/sync/history?client_id={client_id}", json=hist, headers=headers, timeout=15)
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
        r = requests.put(
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
        r = requests.post(
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
            _kitsu_user_id = users[0]["id"]
        user_id = _kitsu_user_id

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
            r = requests.post(
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
            r = requests.get(f"https://api.jikan.moe/v4/anime/{int(ids['mal'])}", timeout=12)
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
            r = requests.get(f"https://kitsu.io/api/edge/anime/{ids['kitsu']}", timeout=12)
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
        "status", "progress", "score", "last_updated", "source",
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
        # IMDB missing is normal, don't count as unmatched unless it's a movie
        # Only flag as unmatched if missing core pairing
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



def dedupe_entries():
    """Merge entries that share the same MAL or AniList id into one canonical key.

    Prefer mal_{id} as canonical when MAL is known. State fields use the newer
    last_updated timestamp on conflict.
    """
    ensure_loaded()
    entries = db.get("entries") or {}
    if not entries:
        return 0

    def _ts(s):
        if not s:
            return datetime.min.replace(tzinfo=timezone.utc)
        try:
            return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=timezone.utc)

    def _merge_ids(a, b):
        out = dict(a or {})
        for k, v in (b or {}).items():
            if v in (None, "", []):
                continue
            if out.get(k) in (None, "", []):
                out[k] = v
        return out

    def _canon(ids):
        if ids.get("mal"):
            return f"mal_{ids['mal']}"
        if ids.get("anilist"):
            return f"anilist_{ids['anilist']}"
        if ids.get("anidb"):
            return f"anidb_{ids['anidb']}"
        if ids.get("kitsu"):
            return f"kitsu_{ids['kitsu']}"
        if ids.get("simkl"):
            return f"simkl_{ids['simkl']}"
        return None

    parent = {k: k for k in entries}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    by_mal, by_al = {}, {}
    for k, d in entries.items():
        ids = d.get("ids") or {}
        if ids.get("mal"):
            by_mal.setdefault(str(ids["mal"]), []).append(k)
        if ids.get("anilist"):
            by_al.setdefault(str(ids["anilist"]), []).append(k)
    for keys in list(by_mal.values()) + list(by_al.values()):
        if len(keys) < 2:
            continue
        for k in keys[1:]:
            union(keys[0], k)

    groups = {}
    for k in entries:
        groups.setdefault(find(k), []).append(k)

    merged = {}
    removed = 0
    for keys in groups.values():
        keys_sorted = sorted(keys, key=lambda k: _ts(entries[k].get("last_updated")), reverse=True)
        base = dict(entries[keys_sorted[0]])
        base_ids = dict(base.get("ids") or {})
        base_state = dict(base.get("state") or {})
        base_ts = _ts(base.get("last_updated"))
        for k in keys_sorted[1:]:
            d = entries[k]
            base_ids = _merge_ids(base_ids, d.get("ids"))
            ts = _ts(d.get("last_updated"))
            if ts > base_ts:
                st = dict(d.get("state") or {})
                for sk, sv in st.items():
                    if sv not in (None, ""):
                        base_state[sk] = sv
                base_ts = ts
                base["last_updated"] = d.get("last_updated")
            else:
                st = dict(d.get("state") or {})
                for sk, sv in st.items():
                    if sv not in (None, "") and base_state.get(sk) in (None, ""):
                        base_state[sk] = sv
            for fld in ("title", "title_english", "title_romaji", "title_native", "year", "season", "format", "episodes"):
                if d.get(fld) and not base.get(fld):
                    base[fld] = d[fld]
        base["ids"] = base_ids
        base["state"] = base_state
        if base_ids.get("title"):
            base["title"] = base_ids["title"]
        new_key = _canon(base_ids) or keys_sorted[0]
        if new_key in merged:
            existing = merged[new_key]
            existing["ids"] = _merge_ids(existing.get("ids"), base_ids)
            merged[new_key] = existing
            removed += len(keys)
        else:
            merged[new_key] = base
            removed += len(keys) - 1

    if removed:
        db["entries"] = merged
        print(f"   Dedupe: {len(entries)} → {len(merged)} (removed {removed} duplicate rows)")
    return removed


def run_once(enrich_new=True, export_csv_flag=False, csv_file=CSV_PATH_DEFAULT, export_unmatched_flag=True, write_json_backup=True):
    ensure_loaded()
    # Always apply offline IMDb/TVDB/TMDB + titles (refresh dumps if stale)
    apply_offline_ids_to_db()
    apply_offline_titles_to_db()
    all_items=[]
    for loader in [load_anilist, load_simkl, load_mal, load_kitsu]:
        try: all_items.extend(loader())
        except Exception as e: print(f"Loader {loader.__name__} failed: {e}")

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
                    pusher(existing, item["state"])
                    existing["last_synced"][platform] = incoming_hash
                    changes += 1
                except Exception as e:
                    print(f"Push to {platform} failed: {e}")
        else:
            # Incoming is older / lower priority → backfill the stored state to this platform if needed
            if existing["last_synced"].get(item["platform"]) != hash_state(existing["state"]):
                print(f"[BACKFILL] {key} -> {item['platform']} (kept stored state: {reason})")
                try:
                    PUSHERS[item["platform"]](existing, existing["state"])
                    existing["last_synced"][item["platform"]] = hash_state(existing["state"])
                    changes += 1
                except Exception as e:
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
    print(f"ID Coverage: MAL:{mal_count} AniList:{anilist_count} Kitsu:{kitsu_count} AniDB:{anidb_count} IMDB:{imdb_count} (manual overrides: {manual_count})")

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
    args = parser.parse_args()

    ensure_loaded()

    if args.export_only:
        apply_offline_ids_to_db()
        apply_offline_titles_to_db()
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
    
