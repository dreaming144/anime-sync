"""SQLite / JSON persistence and process-global sync state."""
import json
import os
import sqlite3
from pathlib import Path

DB_PATH = Path("sync_db.json")
CACHE_PATH = Path("id_cache.json")
SQLITE_PATH = Path("sync.db")
OVERRIDES_PATH = Path("manual_overrides.json")

# Lazy globals — populated by ensure_loaded()
db = {"entries": {}, "id_cache": {}}
id_cache = {}
manual_overrides = {}
_loaded = False

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
                from anime_sync.ids import normalize_ids as _normalize_ids
                v = dict(v)
                v["ids"] = _normalize_ids(v.get("ids") or {})
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
    """Load DB, cache, and overrides once per process.

    Mutates the existing module-level dicts in place so that
    `from anime_sync.storage import db` keeps a stable object identity.
    """
    global manual_overrides, _loaded
    if _loaded:
        return
    loaded_db, loaded_cache = load_db()
    # in-place update so importers share the same objects
    db.clear()
    db["entries"] = loaded_db.get("entries") or {}
    db["id_cache"] = loaded_db.get("id_cache") or {}
    id_cache.clear()
    id_cache.update(loaded_cache or {})
    if OVERRIDES_PATH.exists():
        try:
            loaded_ov = json.loads(OVERRIDES_PATH.read_text(encoding="utf-8"))
            manual_overrides.clear()
            manual_overrides.update(loaded_ov)
            real = [k for k in manual_overrides if not k.startswith("_")]
            print(f"Loaded {len(real)} manual overrides from {OVERRIDES_PATH}")
        except (json.JSONDecodeError, FileNotFoundError) as e:
            print(f"Failed to load overrides: {e}")
            manual_overrides.clear()
    else:
        manual_overrides.clear()
        manual_overrides.update({
            "_comment": "Manual overrides for shows that APIs can't auto-pair.",
            "_how_to": "Key = any existing ID like kitsu_12345 or mal_12345. Value = full IDs to force.",
        })
        OVERRIDES_PATH.write_text(json.dumps(manual_overrides, indent=2), encoding="utf-8")
        print(f"Created empty {OVERRIDES_PATH}")

    # Normalize IDs for every entry (lazy import avoids circular dependency with ids.py)
    from anime_sync.ids import normalize_ids as _normalize_ids
    for entry in db.get("entries", {}).values():
        if isinstance(entry.get("ids"), dict):
            entry["ids"] = _normalize_ids(entry["ids"])

    _loaded = True




def apply_manual_override(
    *,
    key: str | None = None,
    title: str | None = None,
    mal: str | None = None,
    anilist: str | None = None,
    kitsu: str | None = None,
    simkl: str | None = None,
    imdb: str | None = None,
    tvdb: str | None = None,
) -> dict:
    """Append/update one manual override and persist to manual_overrides.json.

    `key` is the lookup key (usually lowercase title or existing override key).
    If only title is given, key defaults to title.lower().strip().
    """
    global manual_overrides
    ensure_loaded()
    key = (key or title or "").strip()
    if not key:
        raise ValueError("override requires --override-key or --override-title")
    # Prefer stable non-lowercase display key if title given
    store_key = key
    entry = dict(manual_overrides.get(store_key) or manual_overrides.get(store_key.lower()) or {})
    if title:
        entry["title"] = title
    for field, val in (
        ("mal", mal),
        ("anilist", anilist),
        ("kitsu", kitsu),
        ("simkl", simkl),
        ("imdb", imdb),
        ("tvdb", tvdb),
    ):
        if val is not None and str(val).strip() != "":
            entry[field] = str(val).strip()
    if not any(entry.get(f) for f in ("mal", "anilist", "kitsu", "simkl", "imdb", "tvdb")):
        raise ValueError("override needs at least one id field")
    # Remove old case variant
    for k in list(manual_overrides.keys()):
        if k.lower() == store_key.lower() and k != store_key:
            del manual_overrides[k]
    manual_overrides[store_key] = entry
    # Persist
    import json
    serializable = {k: v for k, v in manual_overrides.items()}
    OVERRIDES_PATH.write_text(json.dumps(serializable, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"-> Manual override saved: {store_key!r} → {entry}")
    return entry
