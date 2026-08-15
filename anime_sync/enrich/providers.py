"""Secondary metadata providers: AniZip, Kitsu mappings, animeapi, ids.moe."""
import json
import os
from datetime import datetime, timezone

from anime_sync.http import request_with_retries, CircuitOpenError
from anime_sync.ids import _normalize_imdb, _normalize_tmdb
from anime_sync.storage import id_cache

import requests

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
        r = request_with_retries("GET", url, timeout=15)
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
        r = request_with_retries("GET", url, timeout=15)
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




def fetch_ids_moe(ids_dict, use_cache=True):
    """Optional ids.moe mapping (requires IDS_MOE_API_KEY for non-trivial use).

    Public endpoints are limited; when key is set, query by MAL/AniList.
    """
    api_key = os.getenv("IDS_MOE_API_KEY") or os.getenv("IDS_MOE_TOKEN")
    if not api_key:
        return {}
    ids_dict = ids_dict or {}
    if ids_dict.get("mal"):
        path, cache_key = f"mal/{ids_dict['mal']}", f"idsmoe_mal_{ids_dict['mal']}"
    elif ids_dict.get("anilist"):
        path, cache_key = f"anilist/{ids_dict['anilist']}", f"idsmoe_anilist_{ids_dict['anilist']}"
    else:
        return {}
    if use_cache and cache_key in id_cache:
        cached = id_cache[cache_key]
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                cached.get("_cached_at", "2000-01-01T00:00:00+00:00")
            )).days
            if age < 30:
                return cached
        except (ValueError, TypeError):
            pass
    headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
    for base in ("https://api.ids.moe", "https://ids.moe"):
        try:
            r = request_with_retries(
                "GET", f"{base}/{path}", headers=headers, timeout=15,
            )
            if not r.ok:
                continue
            data = r.json() if r.content else {}
            if not isinstance(data, dict):
                continue
            result = {
                "mal": data.get("myanimelist") or data.get("mal"),
                "anilist": data.get("anilist"),
                "kitsu": data.get("kitsu"),
                "anidb": data.get("anidb"),
                "simkl": data.get("simkl"),
                "imdb": _normalize_imdb(data.get("imdb")) if data.get("imdb") else None,
                "tvdb": data.get("thetvdb") or data.get("tvdb"),
                "tmdb": data.get("themoviedb") or data.get("tmdb"),
                "title": data.get("title"),
                "_cached_at": datetime.now(timezone.utc).isoformat(),
                "_source": "ids_moe",
            }
            result = {k: v for k, v in result.items() if v is not None and v != ""}
            if cache_key and result:
                id_cache[cache_key] = result
            return result
        except Exception:
            continue
    return {}



def fetch_animeapi(ids_dict, use_cache=True):
    """Alternative metadata provider: animeapi.my.id (nattadasu fork).

    Broader platform coverage than ARM for some titles (IMDb, Trakt, Notify, …).
    Public, no auth. Use when ARM is sparse on externals.
    """
    ids_dict = ids_dict or {}
    # Prefer MAL path, then AniList
    path = None
    cache_key = None
    if ids_dict.get("mal"):
        path = f"myanimelist/{ids_dict['mal']}"
        cache_key = f"animeapi_mal_{ids_dict['mal']}"
    elif ids_dict.get("anilist"):
        path = f"anilist/{ids_dict['anilist']}"
        cache_key = f"animeapi_anilist_{ids_dict['anilist']}"
    elif ids_dict.get("kitsu"):
        path = f"kitsu/{ids_dict['kitsu']}"
        cache_key = f"animeapi_kitsu_{ids_dict['kitsu']}"
    else:
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

    try:
        r = request_with_retries(
            "GET",
            f"https://animeapi.my.id/{path}",
            timeout=15,
            use_circuit=True,
        )
        if not r.ok:
            return {}
        data = r.json() if r.content else {}
    except (requests.RequestException, ValueError, TypeError, json.JSONDecodeError, CircuitOpenError, TimeoutError):
        return {}

    if not isinstance(data, dict):
        return {}

    result = {
        "mal": data.get("myanimelist") or data.get("mal"),
        "anilist": data.get("anilist"),
        "kitsu": data.get("kitsu"),
        "anidb": data.get("anidb"),
        "simkl": data.get("simkl"),
        "imdb": _normalize_imdb(data.get("imdb")) if data.get("imdb") else None,
        "tvdb": data.get("thetvdb") or data.get("tvdb"),
        "tmdb": data.get("themoviedb") or data.get("tmdb"),
        "title": data.get("title"),
        "_cached_at": datetime.now(timezone.utc).isoformat(),
        "_source": "animeapi",
    }
    result = {k: v for k, v in result.items() if v is not None and v != ""}
    if cache_key and len([k for k in result if not k.startswith("_")]) >= 1:
        id_cache[cache_key] = result
    return result



