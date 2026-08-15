"""CSV exports, title resolution, and job/push reports."""
import time
import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from anime_sync.http import (
    bulkhead_status,
    circuit_status,
    rate_limiter_status,
    request_with_retries,
    CircuitOpenError,
)
from anime_sync.storage import db, ensure_loaded, id_cache
from anime_sync.ids import normalize_ids

import requests

CSV_PATH_DEFAULT = Path("anime_pairings.csv")
PUSH_REPORT_PATH = Path("push_report.csv")
UNMATCHED_PATH = Path("unmatched.csv")

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
            r = request_with_retries(
                "POST",
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
            r = request_with_retries("GET", f"https://api.jikan.moe/v4/anime/{int(ids['mal'])}", timeout=12, base_sleep=2.0)
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
            r = request_with_retries("GET", f"https://kitsu.io/api/edge/anime/{ids['kitsu']}", timeout=12)
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
        "status", "progress", "score", "last_updated", "source", "media_type",
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
            "media_type": ids.get("media_type") or data.get("media_type") or "",
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
        # Western / non-anime (Avatar, Korra, etc.) are not expected to have MAL/AniList
        if data.get("non_anime") or ids.get("non_anime") or data.get("media_type") == "western" or ids.get("media_type") == "western":
            continue
        # Only flag as unmatched if missing core anime pairing (MAL + AniList)
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



