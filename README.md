# anime-sync

Universal bidirectional anime list sync across **AniList**, **MyAnimeList**, **Kitsu**, and **SIMKL**.

Runs on a GitHub Actions schedule (every 2 hours), with optional manual runs.

## Features

- Two-way status/progress/score sync with configurable conflict policy
- Offline ID enrichment (Fribb) + title metadata (Manami)
- ARM / Kitsu mappings for cross-IDs (MAL, AniList, AniDB, IMDb, TVDB, …)
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
# export only:
python universal_anime_sync_github.py --export-only
```

## Next Steps

Suggested follow-ups when you return to this project:

### Reliability & ops
1. **Branch protection** on `main` (require PR or restrict bot-only pushes if desired)
2. **Alert on failure** — GitHub Actions email/Slack when the sync workflow fails
3. **RateLimiter module** — token-bucket per API (complements breaker + bulkhead)
4. **Prune dry-run check** — monthly manual run with `dry_run=true` to confirm 30-day cleanup

### Data quality
5. **Kitsu mapping bulkhead for western** — auto-pull TVDB from Kitsu mappings for any future non-anime entries
6. **Override UI/workflow** — `workflow_dispatch` input to append one override without editing JSON
7. **SIMKL coverage** — only ~half the list has SIMKL IDs; optional enrichment pass if useful for your apps

### Product / UX
8. **Daemon mode** (roadmap) — long-running local process vs Actions-only
9. **Conflict report** — CSV of entries where platforms disagreed and which policy won
10. **README badge** — last workflow status shield on this file

### Not planned near-term
- Resilience4j (Java-only; Python stack already covers breaker/bulkhead/retry)
- Fake MAL/AniList IDs for western cartoons

---

*Bot commits use `[skip ci]` for routine sync pushes to avoid nested runs.*
