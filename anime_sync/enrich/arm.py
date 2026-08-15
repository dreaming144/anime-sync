"""ARM (relations.yuna.moe) ID mapping with sparse multi-source retry."""
import json
import time
from datetime import datetime, timezone

from anime_sync.http import request_with_retries, CircuitOpenError
from anime_sync.ids import _normalize_imdb, _normalize_tmdb
from anime_sync.storage import id_cache

import requests

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



def _arm_is_sparse(result, ids_dict=None):
    """True when result lacks useful externals we still need."""
    if not result:
        return True
    ids_dict = ids_dict or {}
    has_core = bool(result.get("mal") or result.get("anilist"))
    # Sparse if missing several high-value externals that input also lacks
    wanted = ("simkl", "imdb", "tvdb", "anidb", "kitsu")
    missing_wanted = sum(
        1 for k in wanted
        if not result.get(k) and not ids_dict.get(k)
    )
    # Only core echo of query id with almost nothing else
    real = [k for k in result if not k.startswith("_") and result.get(k)]
    if len(real) <= 2 and not result.get("simkl") and not result.get("imdb"):
        return True
    if has_core and missing_wanted >= 3:
        return True
    if not has_core:
        return True
    return False



def _arm_source_candidates(ids_dict):
    """Ordered list of (source, id, cache_key) to try for sparse retry."""
    order = [
        ("anilist", "anilist", "arm_anilist_"),
        ("myanimelist", "mal", "arm_mal_"),
        ("kitsu", "kitsu", "arm_kitsu_"),
        ("anidb", "anidb", "arm_anidb_"),
    ]
    out = []
    seen = set()
    # Prefer primary pick first
    primary = _arm_pick_source(ids_dict)
    if primary[0]:
        out.append(primary)
        seen.add(primary[0])
    for source, field, prefix in order:
        if source in seen:
            continue
        val = ids_dict.get(field)
        if val is not None and str(val).strip() != "":
            out.append((source, val, f"{prefix}{val}"))
            seen.add(source)
    return out



def fetch_arm(ids_dict, use_cache=True):
    """Lookup cross-IDs via ARM v2 with sparse external retry.

    1) Primary source via GET /api/v2/ids
    2) If sparse, try alternate sources present on the entry
    3) POST /api/ids (v1) merge for remaining core gaps
    """
    candidates = _arm_source_candidates(ids_dict or {})
    if not candidates:
        return {}

    primary_key = candidates[0][2]

    if use_cache and primary_key and primary_key in id_cache:
        cached = id_cache[primary_key]
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                cached.get("_cached_at", "2000-01-01T00:00:00+00:00")
            )).days
            # Only trust rich cache hits; sparse ones expire faster (3d)
            sparse_cached = _arm_is_sparse(cached, ids_dict)
            max_age = 3 if sparse_cached else 30
            if age < max_age and not sparse_cached:
                return cached
            if age < max_age and sparse_cached:
                # Re-query but keep cached as baseline
                pass
            elif age >= max_age:
                pass
            else:
                return cached
        except (ValueError, TypeError):
            pass

    result = {}
    tried = set()
    for source, raw_id, cache_key in candidates:
        if source in tried:
            continue
        tried.add(source)
        id_param = int(raw_id) if str(raw_id).isdigit() else raw_id
        try:
            r = request_with_retries(
                "GET",
                "https://relations.yuna.moe/api/v2/ids",
                params={"source": source, "id": id_param},
                timeout=15,
            )
            if r.ok:
                piece = _arm_normalize_entry(r.json(), source_tag=f"arm_v2:{source}")
                for k, v in piece.items():
                    if k.startswith("_"):
                        continue
                    if v and not result.get(k):
                        result[k] = v
                if piece.get("_source"):
                    result["_source"] = piece["_source"]
        except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError, CircuitOpenError, TimeoutError):
            pass

        # Stop early if we have core + at least one external
        if result.get("mal") and result.get("anilist") and (
            result.get("simkl") or result.get("imdb") or result.get("tvdb")
        ):
            break
        # Continue while still sparse
        if not _arm_is_sparse(result, ids_dict):
            break
        time.sleep(0.05)

    # v1 fallback for core four when still weak
    if _arm_is_sparse(result, ids_dict) or (
        not result.get("mal") and not result.get("anilist")
    ):
        for source, raw_id, _ck in candidates[:2]:
            id_param = int(raw_id) if str(raw_id).isdigit() else raw_id
            try:
                body = {source: id_param}
                r = request_with_retries(
                    "POST", "https://relations.yuna.moe/api/ids", json=body, timeout=15
                )
                if r.ok:
                    v1 = _arm_normalize_entry(r.json(), source_tag="arm_v1")
                    for k, v in v1.items():
                        if k.startswith("_"):
                            continue
                        if v and not result.get(k):
                            result[k] = v
                    if not result.get("_source"):
                        result["_source"] = "arm_v1"
            except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError, CircuitOpenError, TimeoutError):
                pass
            time.sleep(0.05)

    if result:
        result["_cached_at"] = datetime.now(timezone.utc).isoformat()
        # Cache under every candidate key we have an id for
        if not _arm_is_sparse(result, ids_dict):
            for _s, _id, ck in candidates:
                if ck:
                    id_cache[ck] = result
        elif primary_key:
            # Still cache sparse briefly so we don't hammer
            id_cache[primary_key] = result
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
            r = request_with_retries(
                "POST",
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





