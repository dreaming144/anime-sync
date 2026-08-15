"""CLI entrypoint for universal anime sync."""
import argparse
import sys
from pathlib import Path

from anime_sync.export import (
    UNMATCHED_PATH,
    export_csv,
    export_unmatched,
)
from anime_sync.enrich import (
    apply_offline_ids_to_db,
    apply_offline_titles_to_db,
    fill_missing_simkl_ids,
)
from anime_sync.ids import dedupe_entries
from anime_sync.storage import apply_manual_override, db, ensure_loaded, id_cache, save_db
import anime_sync.sync as sync_mod
from anime_sync.sync import propagate_oldest_dates, run_once


def main(argv=None):
    parser = argparse.ArgumentParser(description="Universal Anime Sync")
    parser.add_argument("--no-enrich", action="store_true", help="Skip network ID enrichment")
    parser.add_argument("--enrich-all", action="store_true", help="Clear ID cache and re-enrich everything")
    parser.add_argument("--export-csv", action="store_true", help="Export anime_pairings.csv after sync")
    parser.add_argument("--export-csv-file", default="anime_pairings.csv")
    parser.add_argument("--export-only", action="store_true", help="Only export CSVs, no fetch/sync")
    parser.add_argument("--no-unmatched", action="store_true", help="Skip unmatched report")
    parser.add_argument("--no-json-backup", action="store_true", help="Skip writing legacy JSON backups")
    parser.add_argument("--dry-run", action="store_true", help="Plan pushes but do not write to remote lists")
    parser.add_argument("--no-push", action="store_true", help="Fetch/enrich only; skip all remote pushes")
    parser.add_argument(
        "--sync-dates-only",
        action="store_true",
        help="After loading DB, only propagate oldest started/completed dates to platforms",
    )
    # Single manual override (workflow_dispatch friendly)
    parser.add_argument("--override-key", default="", help="Override lookup key (defaults to title)")
    parser.add_argument("--override-title", default="", help="Title for manual override")
    parser.add_argument("--override-mal", default="", help="MAL id")
    parser.add_argument("--override-anilist", default="", help="AniList id")
    parser.add_argument("--override-kitsu", default="", help="Kitsu id")
    parser.add_argument("--override-simkl", default="", help="SIMKL id")
    parser.add_argument("--override-imdb", default="", help="IMDb id")
    parser.add_argument("--override-tvdb", default="", help="TVDB id")
    parser.add_argument(
        "--override-only",
        action="store_true",
        help="Only apply override and exit (no full sync)",
    )
    args = parser.parse_args(argv)

    if args.dry_run or args.no_push:
        sync_mod.DRY_RUN = True
        print("-> DRY-RUN / no-push: remote list writes disabled")

    ensure_loaded()

    if any([args.override_key, args.override_title, args.override_mal, args.override_anilist,
            args.override_kitsu, args.override_simkl, args.override_imdb, args.override_tvdb]):
        apply_manual_override(
            key=args.override_key or None,
            title=args.override_title or None,
            mal=args.override_mal or None,
            anilist=args.override_anilist or None,
            kitsu=args.override_kitsu or None,
            simkl=args.override_simkl or None,
            imdb=args.override_imdb or None,
            tvdb=args.override_tvdb or None,
        )
        if args.override_only:
            return 0

    if args.export_only:
        apply_offline_ids_to_db()
        apply_offline_titles_to_db()
        fill_missing_simkl_ids()
        dedupe_entries()
        save_db(db, id_cache, write_json_backup=not args.no_json_backup)
        export_csv(args.export_csv_file)
        if not args.no_unmatched:
            export_unmatched(UNMATCHED_PATH)
        return 0

    if args.sync_dates_only:
        # Collect dates from a normal load first unless DB already has dates
        run_once(
            enrich_new=False,
            export_csv_flag=False,
            export_unmatched_flag=False,
            write_json_backup=not args.no_json_backup,
        )
        # run_once already calls propagate_oldest_dates; done
        return 0

    if args.enrich_all:
        id_cache.clear()

    run_once(
        enrich_new=not args.no_enrich,
        export_csv_flag=args.export_csv,
        csv_file=Path(args.export_csv_file),
        export_unmatched_flag=not args.no_unmatched,
        write_json_backup=not args.no_json_backup,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
