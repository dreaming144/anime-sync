"""Enrichment: offline DBs, ARM, providers, and orchestrators."""
from anime_sync.enrich.arm import (
    _arm_is_sparse,
    _arm_normalize_entry,
    _arm_pick_source,
    _arm_source_candidates,
    fetch_arm,
    fetch_arm_batch,
)
from anime_sync.enrich.offline import (
    FRIBB_PATH,
    FRIBB_URL,
    MANAMI_PATH,
    MANAMI_URL,
    OFFLINE_MAX_AGE_SEC,
    apply_offline_ids_to_db,
    apply_offline_titles_to_db,
    ensure_offline_file,
    fetch_fribb,
    load_fribb_index,
    load_manami_title_index,
)
from anime_sync.enrich.providers import (
    fetch_animeapi,
    fetch_anizip,
    fetch_ids_moe,
    fetch_kitsu_mappings,
)
from anime_sync.enrich.core import (
    enrich_ids,
    enrich_ids_batch,
    fill_missing_simkl_ids,
    is_fully_resolved,
)

__all__ = [
    "FRIBB_PATH", "FRIBB_URL", "MANAMI_PATH", "MANAMI_URL", "OFFLINE_MAX_AGE_SEC",
    "_arm_is_sparse", "_arm_normalize_entry", "_arm_pick_source", "_arm_source_candidates",
    "apply_offline_ids_to_db", "apply_offline_titles_to_db",
    "enrich_ids", "enrich_ids_batch", "ensure_offline_file",
    "fetch_animeapi", "fetch_anizip", "fetch_arm", "fetch_arm_batch",
    "fetch_fribb", "fetch_ids_moe", "fetch_kitsu_mappings",
    "fill_missing_simkl_ids", "is_fully_resolved",
    "load_fribb_index", "load_manami_title_index",
]
