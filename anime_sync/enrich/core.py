"""ID enrichment orchestration."""
import os
import time
from concurrent.futures import as_completed

from anime_sync.http import POOL_ENRICH, request_with_retries, CircuitOpenError
from anime_sync.ids import get_override_for_ids, normalize_ids, _normalize_imdb
from anime_sync.storage import db, ensure_loaded, id_cache

from anime_sync.enrich.arm import fetch_arm, fetch_arm_batch
from anime_sync.enrich.offline import fetch_fribb, apply_offline_ids_to_db, apply_offline_titles_to_db
from anime_sync.enrich.providers import (
    fetch_anizip,
    fetch_animeapi,
    fetch_ids_moe,
    fetch_kitsu_mappings,
)

import requests

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

    pool = POOL_ENRICH.executor()
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
        # Alternative provider when ARM still sparse on externals
        still_sparse = not enriched.get("simkl") or not enriched.get("imdb") or not enriched.get("tvdb")
        if still_sparse and (enriched.get("mal") or enriched.get("anilist") or enriched.get("kitsu")):
            alt = fetch_animeapi(enriched, use_cache=True)
            time.sleep(0.05)
            if alt:
                for k in ["mal", "anilist", "kitsu", "anidb", "simkl", "imdb", "tvdb", "tmdb", "title"]:
                    if alt.get(k) and not enriched.get(k):
                        enriched[k] = alt[k]
                if alt.get("_source") and not enriched.get("_source"):
                    enriched["_source"] = alt["_source"]
        if (not enriched.get("simkl") or not enriched.get("imdb")) and (
            enriched.get("mal") or enriched.get("anilist")
        ):
            moe = fetch_ids_moe(enriched, use_cache=True)
            if moe:
                for k in ["mal", "anilist", "kitsu", "anidb", "simkl", "imdb", "tvdb", "tmdb", "title"]:
                    if moe.get(k) and not enriched.get(k):
                        enriched[k] = moe[k]

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


def fill_missing_simkl_ids(max_lookups=200):
    """Backfill SIMKL IDs for entries that lack them.

    Order: ARM cache/API → SIMKL search-by-id (mal / anilist) when client_id set.
    """
    ensure_loaded()
    client_id = os.getenv("SIMKL_CLIENT_ID") or ""
    entries = db.get("entries") or {}
    missing = [
        (k, d) for k, d in entries.items()
        if not (d.get("ids") or {}).get("simkl")
    ]
    if not missing:
        print("   SIMKL fill: nothing missing")
        return 0

    print(f"   SIMKL fill: {len(missing)} entries without simkl id")
    filled = 0
    lookups = 0

    def _simkl_id_lookup(ids):
        nonlocal lookups
        if not client_id or lookups >= max_lookups:
            return None
        for param, key in (("mal", "mal"), ("anilist", "anilist")):
            if not ids.get(key):
                continue
            lookups += 1
            try:
                r = request_with_retries(
                    "GET",
                    "https://api.simkl.com/search/id",
                    params={param: ids[key], "client_id": client_id},
                    timeout=15,
                )
            except (CircuitOpenError, TimeoutError, Exception) as e:
                print(f"   SIMKL id lookup error: {e}")
                return None
            if not r.ok:
                continue
            data = r.json()
            # API returns list or dict with anime key
            items = data if isinstance(data, list) else (data.get("anime") or data.get("shows") or [])
            if isinstance(data, dict) and data.get("ids"):
                items = [data]
            for it in items or []:
                sid = (it.get("ids") or {}).get("simkl") or it.get("simkl_id") or it.get("simkl")
                if sid:
                    return str(sid)
            time.sleep(0.15)
        return None

    for key, data in missing:
        ids = dict(data.get("ids") or {})
        simkl = None
        # 1) ARM (multi-source sparse retry)
        try:
            arm = fetch_arm(ids, use_cache=True)
            if arm:
                if arm.get("simkl"):
                    simkl = str(arm["simkl"])
                for f in ("imdb", "tvdb", "tmdb", "anidb", "mal", "anilist", "kitsu"):
                    if arm.get(f) and not ids.get(f):
                        ids[f] = arm[f]
        except Exception:
            pass
        # 2) animeapi.my.id alternative mappings
        if not simkl:
            try:
                alt = fetch_animeapi(ids, use_cache=True)
                if alt:
                    if alt.get("simkl"):
                        simkl = str(alt["simkl"])
                    for f in ("imdb", "tvdb", "tmdb", "anidb", "mal", "anilist", "kitsu", "title"):
                        if alt.get(f) and not ids.get(f):
                            ids[f] = alt[f]
            except Exception:
                pass
        # 3) SIMKL official id search
        if not simkl:
            simkl = _simkl_id_lookup(ids)
        if not simkl:
            continue
        ids["simkl"] = simkl
        data["ids"] = normalize_ids(ids)
        entries[key] = data
        filled += 1

    if filled:
        db["entries"] = entries
        print(f"   SIMKL fill: +{filled} (lookups={lookups})")
    else:
        print(f"   SIMKL fill: +0 (lookups={lookups})")
    return filled




