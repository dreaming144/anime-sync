"""Lightweight unit tests for anime-sync core helpers (no network)."""
import unittest
import time

# Import after path ok
import universal_anime_sync_github as u


class TestNormalizeIds(unittest.TestCase):
    def test_mal_string(self):
        out = u.normalize_ids({"mal": "123", "anilist": 456})
        self.assertEqual(str(out.get("mal")), "123")
        self.assertTrue(out.get("anilist"))

    def test_imdb_normalize(self):
        self.assertEqual(u._normalize_imdb("123"), "tt123")
        self.assertEqual(u._normalize_imdb("tt456"), "tt456")
        self.assertIsNone(u._normalize_imdb(None) or None)


class TestArmSparse(unittest.TestCase):
    def test_empty_sparse(self):
        self.assertTrue(u._arm_is_sparse({}))

    def test_rich_not_sparse(self):
        rich = {"mal": 1, "anilist": 1, "simkl": 9, "imdb": "tt1"}
        self.assertFalse(u._arm_is_sparse(rich, {}))


class TestRateLimiterAdaptive(unittest.TestCase):
    def test_throttle_and_recover(self):
        rl = u.RateLimiter("t", min_interval=0.2, per_minute=100, max_interval=4.0)
        rl.record_throttle(429, retry_after=0)
        self.assertGreaterEqual(rl.min_interval, 0.39)
        for _ in range(20):
            rl.record_success()
        self.assertLess(rl.min_interval, 0.4)


class TestDedupe(unittest.TestCase):
    def test_dedupe_merges_same_mal(self):
        u.ensure_loaded()
        # snapshot
        before = len(u.db.get("entries") or {})
        # inject two keys same mal
        u.db["entries"]["tmp_a"] = {
            "ids": {"mal": "999001", "title": "Tmp A"},
            "state": {"status": "watching", "progress": 1},
            "last_updated": "2020-01-01T00:00:00+00:00",
        }
        u.db["entries"]["tmp_b"] = {
            "ids": {"mal": "999001", "title": "Tmp B"},
            "state": {"status": "completed", "progress": 12},
            "last_updated": "2024-01-01T00:00:00+00:00",
        }
        removed = u.dedupe_entries()
        mals = [
            str((e.get("ids") or {}).get("mal"))
            for e in u.db["entries"].values()
        ]
        self.assertEqual(mals.count("999001"), 1)
        # cleanup test rows
        for k in list(u.db["entries"]):
            if str((u.db["entries"][k].get("ids") or {}).get("mal")) == "999001":
                del u.db["entries"][k]
        # don't save


if __name__ == "__main__":
    unittest.main()
