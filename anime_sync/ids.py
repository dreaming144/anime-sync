"""ID normalization, canonical keys, and entry deduplication."""
import hashlib
from datetime import datetime, timezone

from anime_sync.storage import db, ensure_loaded, id_cache, manual_overrides

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



def get_canonical_key(ids):
    from anime_sync.enrich import enrich_ids as _enrich_ids
    enriched = _enrich_ids(ids, do_network=False)
    # Network enrich deliberately skipped here (hot path); run enrich_ids_batch instead

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



