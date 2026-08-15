"""Offline enrichment: Fribb (IMDb/TVDB) + Manami (titles)."""
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from anime_sync.http import request_with_retries, CircuitOpenError
from anime_sync.ids import _normalize_imdb, _normalize_tmdb
from anime_sync.storage import db, ensure_loaded

import requests

FRIBB_PATH = Path("anime-list-mini.json")
FRIBB_URL = "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-mini.json"
MANAMI_PATH = Path("anime-offline-database-minified.json")
MANAMI_URL = (
    "https://github.com/manami-project/anime-offline-database/releases/download/"
    "latest/anime-offline-database-minified.json"
)
OFFLINE_MAX_AGE_SEC = 7 * 24 * 3600

_fribb_index = None
_manami_title_index = None

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
        r = request_with_retries(
            "GET", url, timeout=180, allow_redirects=True,
            use_circuit=False, use_bulkhead=False, use_rate_limit=True,
        )
        r.raise_for_status()
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_bytes(r.content)
        tmp.replace(path)
        print(f"   Saved {path.name} ({len(r.content)//1024} KB)")
        return True
    except (requests.RequestException, CircuitOpenError, TimeoutError) as e:
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



