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
| **2** | Extract `storage.py` + `ids.py` | Low | DB load/save + dedupe unchanged |
| **3** | Extract `enrich/` offline + ARM | Medium | Offline fill + ARM parity |
| **4** | Extract `platforms/*` loaders/pushers | Medium | Load/push parity; dry-run OK |
| **5** | Extract `sync.py` + `export.py` + `cli.py` | Medium | Actions entrypoint = `python -m anime_sync` or `main.py` |
| **6** | Delete monolith shim after one green weekly enrich + two scheduled syncs | Low | Deprecate single-file path |

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
