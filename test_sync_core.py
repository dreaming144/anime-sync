"""Lightweight unit tests for anime-sync core helpers (no network)."""
import unittest

from anime_sync.enrich.arm import _arm_is_sparse
from anime_sync.http import RateLimiter
from anime_sync.ids import _normalize_imdb, dedupe_entries, normalize_ids
from anime_sync.storage import db, ensure_loaded


class TestNormalizeIds(unittest.TestCase):
    def test_mal_string(self):
        out = normalize_ids({"mal": "123", "anilist": 456})
        self.assertEqual(str(out.get("mal")), "123")
        self.assertTrue(out.get("anilist"))

    def test_imdb_normalize(self):
        self.assertEqual(_normalize_imdb("123"), "tt123")
        self.assertEqual(_normalize_imdb("tt456"), "tt456")
        self.assertIsNone(_normalize_imdb(None) or None)


class TestArmSparse(unittest.TestCase):
    def test_empty_sparse(self):
        self.assertTrue(_arm_is_sparse({}))

    def test_rich_not_sparse(self):
        rich = {
            "mal": 1,
            "anilist": 1,
            "simkl": 9,
            "imdb": "tt1",
            "tvdb": 1,
            "kitsu": 1,
            "anidb": 1,
        }
        self.assertFalse(_arm_is_sparse(rich, {}))


class TestRateLimiterAdaptive(unittest.TestCase):
    def test_throttle_and_recover(self):
        rl = RateLimiter("t", min_interval=0.2, per_minute=100, max_interval=4.0)
        rl.record_throttle(429, retry_after=0)
        self.assertGreaterEqual(rl.min_interval, 0.39)
        for _ in range(20):
            rl.record_success()
        self.assertLess(rl.min_interval, 0.4)


class TestDedupe(unittest.TestCase):
    def test_dedupe_merges_same_mal(self):
        ensure_loaded()
        db["entries"]["tmp_a"] = {
            "ids": {"mal": "999001", "title": "Tmp A"},
            "state": {"status": "watching", "progress": 1},
            "last_updated": "2020-01-01T00:00:00+00:00",
        }
        db["entries"]["tmp_b"] = {
            "ids": {"mal": "999001", "title": "Tmp B"},
            "state": {"status": "completed", "progress": 12},
            "last_updated": "2024-01-01T00:00:00+00:00",
        }
        dedupe_entries()
        mals = [
            str((e.get("ids") or {}).get("mal"))
            for e in db["entries"].values()
        ]
        self.assertEqual(mals.count("999001"), 1)
        for k in list(db["entries"]):
            if str((db["entries"][k].get("ids") or {}).get("mal")) == "999001":
                del db["entries"][k]



class TestInvalidDates(unittest.TestCase):
    def test_rejects_bad_values(self):
        from anime_sync.dates import parse_date, sanitize_dates_for_push, merge_platform_dates
        self.assertIsNone(parse_date("2020-02-30"))
        self.assertIsNone(parse_date("1899-01-01"))
        self.assertIsNone(parse_date("not-a-date"))
        clean = sanitize_dates_for_push({"started_at": "bogus", "completed_at": "2020-05-01"})
        self.assertNotIn("started_at", clean)
        self.assertEqual(clean.get("completed_at"), "2020-05-01")
        m = merge_platform_dates(None, "mal", {"started_at": "2022-01-01", "completed_at": "2020-01-01"})
        # inverted pair: completed cleared
        self.assertEqual(m.get("started_at"), "2022-01-01")
        self.assertIsNone(m.get("completed_at"))



class TestFormatError(unittest.TestCase):
    def test_format_error_http(self):
        from anime_sync.sync import format_error
        class Resp:
            status_code = 401
            text = "unauthorized token"
        class Err(Exception):
            def __init__(self):
                super().__init__("auth failed")
                self.response = Resp()
        s = format_error(Err())
        self.assertIn("401", s)
        self.assertIn("auth failed", s)
        self.assertIn("hint=check-token", s)

    def test_format_error_timeout(self):
        from anime_sync.sync import format_error
        s = format_error(TimeoutError("read timed out"))
        self.assertIn("TimeoutError", s)
        self.assertIn("hint=retry-later", s)


if __name__ == "__main__":
    unittest.main()
