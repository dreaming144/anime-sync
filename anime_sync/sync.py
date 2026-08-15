"""Orchestration: conflict policy, run_once, push reporting, dry-run flag."""
import csv
import os
from datetime import datetime, timezone
from pathlib import Path

from concurrent.futures import as_completed

from anime_sync.enrich import (
    apply_offline_ids_to_db,
    apply_offline_titles_to_db,
    enrich_ids,
    enrich_ids_batch,
    fill_missing_simkl_ids,
    is_fully_resolved,
)
from anime_sync.export import (
    CSV_PATH_DEFAULT,
    PUSH_REPORT_PATH,
    UNMATCHED_PATH,
    export_csv,
    export_unmatched,
)
from anime_sync.http import (
    POOL_LOAD,
    bulkhead_status,
    circuit_status,
    rate_limiter_status,
    write_circuit_metrics,
)
from anime_sync.ids import dedupe_entries, get_canonical_key, hash_state, normalize_ids
from anime_sync.platforms import (
    LOADERS,
    PUSHERS,
    load_anilist,
    load_kitsu,
    load_mal,
    load_simkl,
)
from anime_sync.storage import db, ensure_loaded, id_cache, save_db
from anime_sync.stats import write_watch_stats

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


DRY_RUN = False
_push_report_rows = []

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




def record_push(platform, ids, state, action="planned", detail=""):
    """Append a row for push_report.csv (always recorded; dry-run skips HTTP)."""
    _push_report_rows.append({
        "platform": platform,
        "action": action,
        "mal": (ids or {}).get("mal") or "",
        "anilist": (ids or {}).get("anilist") or "",
        "kitsu": (ids or {}).get("kitsu") or "",
        "simkl": (ids or {}).get("simkl") or "",
        "status": (state or {}).get("status") or "",
        "progress": (state or {}).get("progress") or "",
        "score": (state or {}).get("score") or "",
        "detail": detail,
        "dry_run": str(bool(DRY_RUN)).lower(),
    })



def write_push_report(path=PUSH_REPORT_PATH):
    if not _push_report_rows:
        print("   Push report: no pushes planned")
        return None
    fields = ["platform", "action", "mal", "anilist", "kitsu", "simkl", "status", "progress", "score", "detail", "dry_run"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(_push_report_rows)
    print(f"   Push report: {len(_push_report_rows)} rows → {path}")
    return path



def write_job_summary(path="job_summary.md"):
    """Markdown summary for GitHub Actions step summary / artifact."""
    entries = (db.get("entries") or {}) if isinstance(db, dict) else {}
    lines = [
        "# Anime Sync Job Summary",
        "",
        f"- Entries: **{len(entries)}**",
        f"- Dry run: **{DRY_RUN}**",
    ]
    for field in ("mal", "anilist", "kitsu", "simkl", "imdb", "tvdb"):
        n = sum(1 for e in entries.values() if (e.get("ids") or {}).get(field))
        lines.append(f"- With {field}: **{n}**")
    try:
        cs = circuit_status()
        if cs:
            lines.append("")
            lines.append("## Circuits")
            for k, v in cs.items():
                lines.append(f"- `{k}`: {v.get('state')} ok={v.get('successes')} fail={v.get('failures')} skip={v.get('short_circuits')}")
    except Exception:
        pass
    try:
        rl = rate_limiter_status()
        if rl:
            lines.append("")
            lines.append("## Rate limits")
            for k, v in rl.items():
                lines.append(
                    f"- `{k}`: {v.get('total')} req, {v.get('waits')} waits, "
                    f"interval={v.get('min_interval')}s (base {v.get('base_interval')}), "
                    f"throttle={v.get('throttle_events')} recover={v.get('recover_events')}"
                )
    except Exception:
        pass
    lines.append("")
    lines.append(f"## Pushes recorded: {len(_push_report_rows)}")
    try:
        stats = write_watch_stats(entries)
        lines.append("")
        lines.append("## Watch history (summary)")
        tot = stats.get("totals") or {}
        lines.append(
            f"- Completed **{tot.get('completed', 0)}** / watching **{tot.get('watching', 0)}** / PTW **{tot.get('plantowatch', 0)}** / dropped **{tot.get('dropped', 0)}**"
        )
        lines.append(
            f"- Scored **{tot.get('scored', 0)}** · Σ progress eps **{tot.get('episodes_progress_sum', 0)}** · completion **{float(tot.get('completion_rate') or 0)*100:.1f}%**"
        )
        lines.append("- Full report: `watch_history_stats.md` / `.json`")
    except Exception as e:
        lines.append(f"- Watch stats error: {e}")
    Path(path).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"   Wrote {path}")
    return path



def run_once(enrich_new=True, export_csv_flag=False, csv_file=CSV_PATH_DEFAULT, export_unmatched_flag=True, write_json_backup=True):
    ensure_loaded()
    # Always apply offline IMDb/TVDB/TMDB + titles (refresh dumps if stale)
    apply_offline_ids_to_db()
    apply_offline_titles_to_db()
    fill_missing_simkl_ids()
    all_items=[]
    # Bulkhead: each platform loader runs in the isolated load pool
    load_pool = POOL_LOAD.executor()
    loader_futs = {
        load_pool.submit(loader): loader.__name__
        for loader in (load_anilist, load_simkl, load_mal, load_kitsu)
    }
    for fut in as_completed(loader_futs):
        name = loader_futs[fut]
        try:
            items = fut.result()
            all_items.extend(items or [])
            print(f"   loader {name}: {len(items or [])} items")
        except Exception as e:
            print(f"Loader {name} failed: {e}")

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
                    record_push(platform, existing.get("ids"), item["state"], action="change")
                    if DRY_RUN:
                        print(f"   [DRY-RUN] skip push {platform} {key}")
                        existing["last_synced"][platform] = incoming_hash
                        changes += 1
                        continue
                    pusher(existing, item["state"])
                    existing["last_synced"][platform] = incoming_hash
                    changes += 1
                except Exception as e:
                    record_push(platform, existing.get("ids"), item["state"], action="error", detail=str(e)[:120])
                    print(f"Push to {platform} failed: {e}")
        else:
            # Incoming is older / lower priority → backfill the stored state to this platform if needed
            if existing["last_synced"].get(item["platform"]) != hash_state(existing["state"]):
                print(f"[BACKFILL] {key} -> {item['platform']} (kept stored state: {reason})")
                try:
                    record_push(item["platform"], existing.get("ids"), existing["state"], action="backfill")
                    if DRY_RUN:
                        print(f"   [DRY-RUN] skip backfill {item['platform']} {key}")
                        existing["last_synced"][item["platform"]] = hash_state(existing["state"])
                        changes += 1
                        continue
                    PUSHERS[item["platform"]](existing, existing["state"])
                    existing["last_synced"][item["platform"]] = hash_state(existing["state"])
                    changes += 1
                except Exception as e:
                    record_push(item["platform"], existing.get("ids"), existing["state"], action="error", detail=str(e)[:120])
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
    write_push_report()
    write_job_summary()
    _cs = circuit_status()
    if _cs:
        print(
            "   Circuits: "
            + ", ".join(
                f"{k}={v['state']}(ok={v['successes']}/fail={v['failures']}/skip={v['short_circuits']})"
                for k, v in _cs.items()
            )
        )
    _bh = bulkhead_status()
    if _bh:
        print(
            "   Bulkheads: "
            + ", ".join(f"{k}={v['total']}calls/{v['rejected']}rej" for k, v in _bh.items())
        )
    _rl = rate_limiter_status()
    if _rl:
        print(
            "   RateLimits: "
            + ", ".join(
                f"{k}={v['total']}req/{v['waits']}w/i={v['min_interval']}s"
                for k, v in _rl.items()
            )
        )
    try:
        write_circuit_metrics("circuit_metrics.json")
        print("   Wrote circuit_metrics.json")
    except Exception as e:
        print(f"   circuit metrics write skipped: {e}")

    if export_csv_flag:
        export_csv(csv_file)
    
    if export_unmatched_flag:
        export_unmatched(UNMATCHED_PATH)


