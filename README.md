# anime-sync

Universal bidirectional anime list sync across **AniList**, **MyAnimeList**, **Kitsu**, and **SIMKL**.

Runs on a GitHub Actions schedule (every 2 hours), with optional manual runs.

## Features

- Two-way status/progress/score sync with configurable conflict policy
- Offline ID enrichment (Fribb) + title metadata (Manami)
- ARM multi-source sparse retry + **animeapi.my.id** fallback for cross-IDs
- Kitsu mappings for seasonal titles ARM has not indexed
- Deduped canonical rows (`mal_{id}` preferred)
- Western cartoons tagged `media_type=western` with TVDB/IMDb (excluded from hard-unmatched)
- Resilience: retries, **circuit breakers** (with metrics), **bulkheads**
- MAL token auto-refresh when secrets allow
- Weekly prune of Actions runs/artifacts older than 30 days

## Secrets

See [SETUP_TOKENS.md](SETUP_TOKENS.md) for AniList / MAL / Kitsu OAuth.

| Secret | Purpose |
|--------|---------|
| `ANILIST_USERNAME` / `ANILIST_TOKEN` | AniList read + write |
| `MAL_ACCESS_TOKEN` (+ refresh/client secrets) | MAL read + write |
| `KITSU_USERNAME` / `KITSU_EMAIL` / `KITSU_PASSWORD` | Kitsu read + write |
| `SIMKL_CLIENT_ID` / `SIMKL_ACCESS_TOKEN` | SIMKL |
| `SECRETS_WRITE_TOKEN` | Optional: persist rotated MAL tokens |

## Outputs

| File | Description |
|------|-------------|
| `sync.db` | SQLite source of truth |
| `anime_pairings.csv` | Full export (titles, IDs, status) |
| `unmatched.csv` | Anime missing MAL **and** AniList (western excluded) |
| `circuit_metrics.json` | Circuit/bulkhead stats (Actions artifact) |
| `manual_overrides.json` | Forced ID pairings |

## Local / one-off

```bash
pip install requests
python universal_anime_sync_github.py --export-csv
python universal_anime_sync_github.py --export-only
python universal_anime_sync_github.py --dry-run --export-csv   # plan pushes only
python universal_anime_sync_github.py --no-push --enrich-all   # deep enrich, no remote writes
python -m unittest test_sync_core.py -v
```

Optional secret: `IDS_MOE_API_KEY` for ids.moe mappings.

`sync_db.json` is no longer committed every run (artifact only). Weekly workflow `enrich-weekly.yml` runs deep enrich without pushes.

## Next Steps

### Done recently
- **Phase 6 modularization** — workflows use `python -m anime_sync`; shim deprecated as CLI
- **Phase 5 modularization** — `sync.py`, `export.py`, `cli.py`; `python -m anime_sync`
- **Phase 4 modularization** — `anime_sync/platforms/` (load+push per service)
- **Phase 3 modularization** — `anime_sync/enrich/` (offline, ARM, providers, core)
- **Phase 2 modularization** — `anime_sync/storage.py`, `anime_sync/ids.py`
- **Phase 1 modularization** — `anime_sync/http/` (rate limit, circuit, bulkhead, client); monolith is a facade
- Adaptive rate limiting (interval multiplies on 429, decays on success)
- All HTTP via `request_with_retries` (rate limit → bulkhead → circuit → retry)
- `--dry-run` / `--no-push`, `push_report.csv`, `job_summary.md`
- Weekly enrich workflow; stop committing `sync_db.json` every run
- Unit tests (`test_sync_core.py`); branch protection on `main`
- Optional `IDS_MOE_API_KEY` / `fetch_ids_moe`

### Still open
1. **Failure alerts** — email/Slack on workflow failure (Actions already has a failure step)
2. **Override via workflow_dispatch** — append one override without editing JSON
3. **Conflict report CSV** — where platforms disagreed and which policy won
4. **README badge** — last workflow status shield
5. **Daemon mode** — long-running local process (later)

### Not planned near-term
- Full async rewrite (`httpx`/`aiohttp`) — rate limits dominate; stay on `requests` + thread pools
- Resilience4j (Java-only)
- Fake MAL/AniList IDs for western cartoons

---

## Modularization roadmap

`universal_anime_sync_github.py` is ~3k lines. Split is optional and should be **incremental** so Actions keeps working.

### Target layout

```text
anime_sync/
  __init__.py
  config.py          # CONFIG, paths, env
  http/
    client.py        # request_with_retries
    rate_limit.py    # RateLimiter (adaptive)
    circuit.py       # CircuitBreaker
    bulkhead.py      # Bulkhead + pools
  storage.py         # SQLite load/save, ensure_loaded
  ids.py             # normalize_ids, get_canonical_key, dedupe
  enrich/
    offline.py       # Fribb, Manami
    arm.py           # ARM v2 + sparse retry
    providers.py     # AniZip, Kitsu mappings, animeapi, ids.moe
  platforms/
    anilist.py       # load + push
    mal.py
    kitsu.py
    simkl.py
  sync.py            # run_once, conflict policy
  export.py          # CSV, unmatched, push_report, job_summary
  cli.py             # argparse entry
main.py              # thin: from anime_sync.cli import main
```

### Phased plan

| Phase | Scope | Risk | Exit criteria |
|-------|--------|------|----------------|
| **0** | Keep monolith; tests green | None | ~~Current~~ → Phase 1 done |
| **1** | Extract `http/` (rate limit, circuit, bulkhead, `request_with_retries`) | Low | **Done** (`anime_sync/http/`); monolith re-exports |
| **2** | Extract `storage.py` + `ids.py` | Low | **Done** — SQLite/JSON + normalize/canonical/dedupe |
| **3** | Extract `enrich/` offline + ARM | Medium | **Done** — offline, arm, providers, core |
| **4** | Extract `platforms/*` loaders/pushers | Medium | **Done** — anilist/mal/kitsu/simkl + status maps |
| **5** | Extract `sync.py` + `export.py` + `cli.py` | Medium | **Done** — `python -m anime_sync` / thin `universal_anime_sync_github.py` |
| **6** | Actions → `python -m anime_sync`; shim kept as API/compat only | Low | **Done** — workflows updated; file is deprecated CLI |

### Rules while splitting

1. **One phase per PR/commit**; keep a thin `universal_anime_sync_github.py` that re-exports until Actions switches.
2. **No behavior change** in a pure extract commit (move code only).
3. Keep **global `db` / `id_cache`** behind storage helpers before introducing explicit context objects.
4. Preserve **env-based secrets** and workflow CLI flags (`--dry-run`, `--no-json-backup`, etc.).
5. Run `python -m unittest test_sync_core.py` and a local `--dry-run --export-csv` after each phase.

### When to start

Only when you need clearer ownership (e.g. testing one platform in isolation) or the file becomes painful to review. Until then Phase 0 is intentional.

---

*Bot commits use `[skip ci]` for routine sync pushes to avoid nested runs.*


## SIMKL rewatch cleanup

Inventory (and optionally remove) Pro/VIP **rewatch sessions**, keeping the canonical/oldest watch.

```bash
# Dry-run inventory (writes simkl_rewatches.json / .csv)
SIMKL_CLIENT_ID=... SIMKL_ACCESS_TOKEN=... \
  python scripts/simkl_rewatch_cleanup.py --type anime

# Best-effort API removal of rewatch sessions only
SIMKL_CLIENT_ID=... SIMKL_ACCESS_TOKEN=... \
  python scripts/simkl_rewatch_cleanup.py --type anime --execute
```

Or run the **SIMKL rewatch cleanup** workflow (`workflow_dispatch`). Default is inventory-only; set `execute=true` to attempt removals.

**Note:** Status sync already avoids creating new rewatches on completed titles (`push_simkl` history skip). Cleanup targets sessions that already exist.


## Watch history stats

From the local SQLite library (`sync.db`):

```bash
python scripts/watch_history_stats.py
# → watch_history_stats.md + watch_history_stats.json
```

Each normal sync also writes these files and embeds a short summary in `job_summary.md`.


## MyAnimeList API v2 (dates)

List fetch uses nested fields so user watch dates are returned:

```
list_status{status,score,num_episodes_watched,is_rewatching,start_date,finish_date,updated_at,num_times_rewatched}
```

Writes use `PUT /anime/{id}/my_list_status` with zero-padded `start_date` / `finish_date` (YYYY-MM-DD). Only **older** canonical dates are written over MAL’s existing dates; `is_rewatching` is not forced on.

## Oldest watch-date sync

Each full sync:

1. Loads **started / completed** dates from AniList, MAL, Kitsu, and SIMKL (`last_watched` as a completed hint).
2. Keeps the **oldest** `started_at` and `completed_at` per show in `sync.db`.
3. Propagates those dates to platforms that support writes:
   - **AniList** — `startedAt` / `completedAt` (FuzzyDate)
   - **MAL** — `start_date` / `finish_date`
   - **Kitsu** — `startedAt` / `finishedAt`
   - **SIMKL** — list status only (history posts for completed titles stay skipped to avoid rewatches)

```bash
python -m anime_sync --sync-dates-only          # load + propagate dates
python -m anime_sync --dry-run --sync-dates-only # plan only
```
