from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPT_PATH = Path(__file__).parent / "scripts" / "simkl_play_stats_diagnostic.py"
SPEC = importlib.util.spec_from_file_location("simkl_play_stats_diagnostic", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
DIAGNOSTIC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DIAGNOSTIC)


class _GetOnlySession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response

    def post(self, *args, **kwargs):  # pragma: no cover - must never be invoked
        raise AssertionError("Read-only diagnostic attempted a POST request")


class TestSimklPlayStatsDiagnostic(unittest.TestCase):
    def test_fetch_requests_extended_rewatch_rows_with_bounded_history(self):
        response = SimpleNamespace(
            ok=True,
            json=lambda: [{"show": {"title": "Example", "ids": {"simkl": 42}}}],
        )
        session = _GetOnlySession(response)

        rows = DIAGNOSTIC.fetch_all_items(
            session,
            client_id="client-id",
            token="access-token",
            media_type="anime",
            date_from="2000-01-01",
        )

        self.assertEqual(1, len(rows))
        self.assertEqual(1, len(session.calls))
        url, kwargs = session.calls[0]
        self.assertEqual("https://api.simkl.com/sync/all-items/anime", url)
        self.assertEqual("GET".lower(), "get")  # documents the only exposed session method
        self.assertEqual("2000-01-01", kwargs["params"]["date_from"])
        self.assertEqual("yes", kwargs["params"]["allow_rewatch"])
        self.assertEqual("full", kwargs["params"]["extended"])
        self.assertEqual("yes", kwargs["params"]["episode_watched_at"])
        self.assertEqual("Bearer access-token", kwargs["headers"]["Authorization"])

    def test_build_rows_keeps_canonical_and_rewatch_data_separate(self):
        entries = [
            {
                "show": {"title": "Sample Anime", "ids": {"simkl": 99, "mal": 100}},
                "is_rewatch": False,
                "status": "completed",
                "watched_episodes_count": 12,
                "last_watched_at": "2022-03-01T00:00:00Z",
            },
            {
                "show": {"title": "Sample Anime", "ids": {"simkl": 99, "mal": 100}},
                "is_rewatch": True,
                "rewatch_id": 4,
                "rewatch_status": "active",
                "watched_episodes_count": 3,
                "last_watched_at": "2026-08-01T00:00:00Z",
                "seasons": [{"number": 1, "episodes": [{"number": 1}]}],
            },
        ]

        rows, totals = DIAGNOSTIC.build_rows(
            entries, media_type="anime", title_terms=["sample"]
        )

        self.assertEqual({"canonical": 1, "rewatch": 1, "selected": 2}, totals)
        self.assertEqual(1, len(rows))
        self.assertEqual("completed", rows[0]["canonical_status"])
        self.assertEqual(12, rows[0]["canonical_progress"])
        self.assertEqual(1, rows[0]["rewatch_session_count"])
        self.assertEqual("4", rows[0]["rewatch_sessions"][0]["rewatch_id"])
        self.assertEqual(3, rows[0]["rewatch_sessions"][0]["progress"])

    def test_source_and_workflow_are_read_only_and_manual_only(self):
        source = SCRIPT_PATH.read_text(encoding="utf-8")
        workflow = (
            Path(__file__).parent
            / ".github"
            / "workflows"
            / "simkl-play-stat-diagnostic.yml"
        ).read_text(encoding="utf-8")

        self.assertIn("session.get(", source)
        self.assertNotIn(".post(", source)
        self.assertNotIn("/sync/history", source)
        self.assertIn("workflow_dispatch:", workflow)
        self.assertNotIn("schedule:", workflow)
        self.assertIn("contents: read", workflow)
        self.assertIn("/sync/all-items", workflow)

    def test_report_is_compact_and_does_not_emit_raw_episode_history(self):
        rows = [
            {
                "media_type": "anime",
                "simkl": "99",
                "mal": 100,
                "anilist": "",
                "title": "Sample Anime",
                "canonical_status": "completed",
                "canonical_progress": 12,
                "canonical_last_watched_at": "2022-03-01T00:00:00Z",
                "rewatch_session_count": 1,
                "rewatch_sessions": [
                    {
                        "rewatch_id": "4",
                        "status": "active",
                        "progress": 3,
                        "last_watched_at": "2026-08-01T00:00:00Z",
                    }
                ],
                "diagnosis": "Explicit session returned.",
            }
        ]
        totals = {"canonical": 1, "rewatch": 1, "selected": 2}

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            markdown_out = root / "report.md"
            csv_out = root / "report.csv"
            json_out = root / "report.json"
            DIAGNOSTIC.write_reports(
                rows,
                media_type="anime",
                date_from="2000-01-01",
                title_terms=["sample"],
                totals=totals,
                markdown_out=markdown_out,
                csv_out=csv_out,
                json_out=json_out,
            )
            markdown = markdown_out.read_text(encoding="utf-8")
            payload = json_out.read_text(encoding="utf-8")

        self.assertIn("GET `/sync/all-items` only", markdown)
        self.assertIn("Sample Anime", markdown)
        self.assertIn('"rewatch_id": "4"', payload)
        self.assertNotIn("seasons", payload)
        self.assertNotIn("episodes", payload)


if __name__ == "__main__":
    unittest.main()
