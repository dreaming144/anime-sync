"""
Universal Anime Sync — thin entry facade (Phases 1–6 modularization).

Preferred entrypoint (Actions + local):
    python -m anime_sync [options]

This module remains a compatibility shim that re-exports the public API for
tests and older scripts. New code should import from `anime_sync.*` or run
`python -m anime_sync`.

DEPRECATED as a primary CLI path — use `python -m anime_sync` instead.
"""

from pathlib import Path
import sys

# Re-exports for tests and external scripts
from anime_sync.http import *  # noqa: F401,F403
from anime_sync.storage import (  # noqa: F401
    CACHE_PATH,
    DB_PATH,
    OVERRIDES_PATH,
    SQLITE_PATH,
    db,
    ensure_loaded,
    id_cache,
    load_db,
    manual_overrides,
    save_db,
)
from anime_sync.ids import (  # noqa: F401
    _normalize_imdb,
    _normalize_tmdb,
    dedupe_entries,
    get_canonical_key,
    get_override_for_ids,
    hash_state,
    normalize_ids,
)
from anime_sync.enrich import (  # noqa: F401
    FRIBB_PATH,
    FRIBB_URL,
    MANAMI_PATH,
    MANAMI_URL,
    OFFLINE_MAX_AGE_SEC,
    _arm_is_sparse,
    apply_offline_ids_to_db,
    apply_offline_titles_to_db,
    enrich_ids,
    enrich_ids_batch,
    fetch_arm,
    fill_missing_simkl_ids,
    is_fully_resolved,
)
from anime_sync.platforms import (  # noqa: F401
    LOADERS,
    PUSHERS,
    REVERSE_STATUS,
    STATUS_MAP,
    load_anilist,
    load_kitsu,
    load_mal,
    load_simkl,
    push_anilist,
    push_kitsu,
    push_mal,
    push_simkl,
)
from anime_sync.export import (  # noqa: F401
    CSV_PATH_DEFAULT,
    PUSH_REPORT_PATH,
    UNMATCHED_PATH,
    export_csv,
    export_unmatched,
    resolve_title,
)
from anime_sync.sync import (  # noqa: F401
    CONFIG,
    DRY_RUN,
    record_push,
    run_once,
    should_accept_update,
    write_job_summary,
    write_push_report,
)
from anime_sync.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
