# Handover: anime-sync (dreaming144/anime-sync)

**Audience:** successor AI / developer maintaining and extending this repo  
**Last updated:** 2026-08-15  
**Owner intent:** set-and-forget bidirectional anime list sync across AniList, Kitsu, SIMKL, MAL, with GitHub Actions automation, offline ID enrichment, and conservative API behaviour.

---

## 1. What this project is

Python package + GitHub Actions that:

1. **Loads** user anime lists from AniList (GraphQL), Kitsu (JSON:API), SIMKL (REST), MAL (API v2).
2. **Merges** them into a SQLite DB (`sync.db`) with cross-platform ID pairing.
3. **Enriches** missing IDs via offline dumps (Fribb anime-list, Manami offline DB), ARM, optional ids.moe, Jikan/MAL search fallbacks.
4. **Pushes** status / progress / oldest dates back to platforms that lag behind (conservative conflict rules).
5. **Exports** `anime_pairings.csv`, `unmatched.csv`, job summaries for Actions.

**Primary entrypoint (Actions and local):**

```bash
python -m anime_sync [flags]
```

Legacy shim: `universal_anime_sync_github.py` (facade only; prefer `-m anime_sync`).

Repo: `https://github.com/dreaming144/anime-sync`  
Branch: `main` (often protected; bot commits use `[skip ci]`).

---

## 2. Architecture (current)

```text
anime_sync/
  __main__.py          # python -m anime_sync
  cli.py               # argparse → sync.main paths
  sync.py              # orchestration: load → merge → enrich → push → reports
  storage.py           # SQLite schema, last_synced, conflicts, overrides
  dates.py             # parse/validate/sanitize/merge oldest dates (incl. leap years)
  ids.py               # canonical keys, override application
  export.py            # CSV / markdown exports
  stats.py             # watch history stats helpers
  http/
    client.py          # request_with_retries (rate → bulkhead → circuit → retry)
    rate_limit.py      # adaptive intervals + 429 backoff (full jitter); rate_limit_state.json
    circuit.py         # distributed circuit breaker → circuit_state.json
    bulkhead.py        # per-service concurrency isolation
    util.py
  platforms/
    anilist.py         # GraphQL load + push (fuzzy dates)
    kitsu.py           # load + push (library entries)
    mal.py             # v2 list + nested list_status dates; token refresh
    simkl.py           # load + push (**add-to-list only** by default)
    status.py          # STATUS_MAP / REVERSE_STATUS
  enrich/
    offline.py         # Fribb + Manami dumps
    arm.py             # ARM relations
    providers.py       # live provider helpers
    core.py            # enrich pipeline

scripts/
  simkl_rewatch_cleanup.py   # inventory/remove VIP rewatch sessions
  render_run_summary.py      # mobile-friendly Actions summary markdown
  watch_history_stats.py

.github/workflows/
  sync.yml                     # every 2h + workflow_dispatch
  enrich-weekly.yml            # Monday deep enrich, no push
  simkl-rewatch-cleanup.yml    # manual cleanup
  prune-actions.yml            # 30-day Actions history prune
```

**Data / state files (often committed by bot):**

| File | Purpose |
|------|---------|
| `sync.db` | Source of truth SQLite |
| `id_cache.json` | ID resolution cache |
| `manual_overrides.json` | User-forced ID maps |
| `circuit_state.json` | Cross-run circuit breaker state |
| `rate_limit_state.json` | Cross-run adaptive rate limits |
| `anime_pairings.csv` / `unmatched.csv` | Human-readable pairing export |
| `anime-list-mini.json` / `anime-offline-database-minified.json` | Offline mapping dumps (cached in Actions) |

**Do not** recommit huge `sync_db.json` every run (artifact-only).

---

## 3. Secrets & env (GitHub Actions)

Required for full bidirectional sync:

| Secret | Use |
|--------|-----|
| `ANILIST_USERNAME` | Read |
| `ANILIST_TOKEN` | Write (~1 year OAuth) |
| `KITSU_USERNAME` | Read |
| `KITSU_TOKEN` **or** `KITSU_EMAIL` + `KITSU_PASSWORD` | Write |
| `SIMKL_CLIENT_ID` | Read/write |
| `SIMKL_ACCESS_TOKEN` | Read/write |
| `MAL_ACCESS_TOKEN` | Write (short-lived) |
| `MAL_REFRESH_TOKEN` + `MAL_CLIENT_ID` (+ secret if used) | Auto-refresh |
| `IDS_MOE_API_KEY` | Optional ID enrichment |
| `SECRETS_WRITE_TOKEN` | Optional: write refreshed MAL token back to repo secrets |

See `SETUP_TOKENS.md` for OAuth steps.

**Critical SIMKL flag:**

- `SIMKL_ALLOW_HISTORY` default **off** (`0`).  
- When `1`, `push_simkl` may POST `/sync/history` for watching/on_hold progress.  
- **Do not enable** on automated syncs — creates VIP rewatch sessions / inflated “plays”.

`SIMKL_WRITE_INTERVAL` (default ~1.0s) spaces SIMKL POSTs.

---

## 4. Workflows (plain-language dispatch)

All dispatch UIs use descriptive labels (not bare true/false).

### `sync.yml` — Universal Anime Sync
- Schedule: `0 */2 * * *`
- **Run mode:** Normal / Preview (dry-run) / Fetch only / Export CSVs only / Re-enrich all IDs
- **Override** title + MAL/AniList/SIMKL ids + scope (then sync vs override-only)
- Steps include **Easy summary** → `scripts/render_run_summary.py` → `$GITHUB_STEP_SUMMARY`
- Commits DB/CSV with `[skip ci]` when changed

### `enrich-weekly.yml`
- Monday deep enrich, `--no-push`
- Scope: full re-enrich vs export-only

### `simkl-rewatch-cleanup.yml`
- Media: all / anime / shows / movies
- Mode: List only vs Delete rewatch sessions
- Max deletes safety cap
- **Only** targets `is_rewatch=true` rows via `POST /sync/history/remove?allow_rewatch=yes`

### `prune-actions.yml`
- Retention days + List only vs Delete

---

## 5. Sync policy (do not casually change)

1. **Oldest dates win** — `merge_platform_dates` / `propagate_oldest_dates`; never invent future dates; invalid/leap dates sanitized (`dates.py`).
2. **No rewatch creation on SIMKL** — status via `/sync/add-to-list` only unless explicitly opted in.
3. **Conflicts** — recorded in `conflict_report.csv`; acceptance rules in storage/sync (prefer not to thrash status).
4. **Manual overrides** — `manual_overrides.json` + workflow_dispatch / CLI; highest priority for IDs.
5. **Resilience stack** (order matters): rate limit → bulkhead → circuit → retries with Retry-After / full jitter.
6. **Distributed state** — circuit + rate limit JSON files must persist across Actions runs (atomic write + lock where used).

---

## 6. SIMKL “multiple plays” issue (known)

**Symptom:** Profile “Most watched” shows 10–26+ plays for titles the user watched once.

**Cause:** Historical `POST /sync/history` from earlier sync versions stacked episode history and/or VIP rewatch sessions. Profile **plays ≠** only `is_rewatch` rows.

**Mitigations in code:**
- `push_simkl` history disabled by default (`3982839` and later).
- Cleanup script for formal rewatch sessions (`scripts/simkl_rewatch_cleanup.py`).

**Limitation:** Cleanup **cannot** fully clear inflated play stats if they live in normal episode history. Website paths that work:
- Title → Manage Watch History / Rewatches → **Clear All**
- Bulk delete watched episodes (then re-mark completed once if needed)

Official SIMKL guidance: **never** use `allow_rewatch=yes` on background syncs (phantom sessions).

Docs: https://api.simkl.org/guides/rewatches , https://api.simkl.org/guides/mark-as-watched

---

## 7. CLI flags (high level)

```text
python -m anime_sync --export-csv
python -m anime_sync --export-only
python -m anime_sync --dry-run --export-csv
python -m anime_sync --no-push --enrich-all
python -m anime_sync --override-title "..." --override-mal N --override-only
```

See `anime_sync/cli.py` for full list.

Tests:

```bash
python -m unittest test_dates.py test_circuit.py test_sync_core.py -v
# rate limit tests may live in test_rate_limit.py if present
```

Deps: `requests` (see `requirements.txt`). Stay on sync `requests` + thread pools unless rate limits stop dominating; async not planned near-term.

---

## 8. What was finished recently (context)

- Modularization Phases 1–6 (http → storage/ids → enrich → platforms → sync/export/cli → Actions use `-m anime_sync`)
- MAL API v2 dates; date validation + leap-year tests
- Show report / job summary / conflict CSV / Easy summary on all workflows
- Adaptive 429 backoff + file-backed rate limit + distributed circuit breaker
- Plain-language workflow_dispatch labels
- SIMKL history posts disabled by default; cleanup `--type all`; API error retries on cleanup

---

## 9. Open / suggested next work

| Priority | Item | Notes |
|----------|------|--------|
| High | SIMKL play-stat diagnosis | Read-only dump of `all-items` for high-play titles; distinguish rewatch vs episode history |
| High | Monitor post-fix syncs | Confirm play counts stop climbing without `SIMKL_ALLOW_HISTORY` |
| Medium | Safer episode-history trim | Optional script using `POST /sync/history/remove` at season/episode grain — **dangerous**; prefer website for bulk |
| Medium | MAL token refresh reliability | Ensure refresh + optional secrets write works before token expiry |
| Low | Daemon mode | Local long-running process (explicitly deferred) |
| Low | Slack/email webhook | Beyond Actions summary / optional issue on fail |
| Avoid | Async HTTP rewrite | Not needed yet |
| Avoid | Re-enabling SIMKL history on schedule | Will recreate the plays problem |

---

## 10. Operating rules for the successor AI

1. **Never commit secrets** (tokens, client secrets) into the repo or logs.
2. Prefer **small, reversible PRs/commits** with clear messages; bot already uses `chore: sync … [skip ci]`.
3. Run **unit tests** after date/circuit/rate-limit changes.
4. After workflow YAML edits, validate YAML and keep **Easy summary** step.
5. When changing SIMKL push: re-read official rewatches guide; default must remain **no history posts**.
6. User prefers **set-and-forget** automation and **readable** Actions summaries (tables, not raw CSV dumps).
7. User is on **iPhone** often — summary markdown should stay mobile-scannable.
8. Do not invent platform IDs; use offline DB + overrides.
9. Credentials the user pastes in chat should be treated as **rotated** if ever exposed; guide them to update GitHub secrets only.

---

## 11. Quick health check

```bash
git pull origin main
python -m unittest test_dates.py test_circuit.py test_sync_core.py -v
python -m anime_sync --export-only --no-json-backup   # needs local DB
# Actions: latest sync.yml success + Summary tab shows Coverage + Changes
```

Coverage snapshot (approx as of 2026-08-15): ~772 shows, MAL/AniList high 760s, unmatched often 0 after overrides.

---

## 12. Contact context

- Maintainer uses GitHub Actions as primary runtime (not always local secrets).
- Strong preference for privacy-conscious automation and fixing cross-platform ID mismatches.
- Related past work: custom Grok GitHub skill for audit/improve/test on this repo.

**When in doubt:** preserve oldest-date policy, keep SIMKL history off, prefer readable summaries, and verify with a **Preview only** workflow run before enabling writes.
