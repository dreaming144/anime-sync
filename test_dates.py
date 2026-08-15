"""Unit tests for anime_sync.dates — parse, validate, merge, sanitize."""
from __future__ import annotations

import unittest
from datetime import date, datetime, timezone

from anime_sync.dates import (
    InvalidDateError,
    dates_need_push,
    merge_platform_dates,
    older,
    parse_date,
    safe_parse_date,
    sanitize_dates_for_push,
    to_fuzzy,
    to_iso_date,
    to_kitsu_dt,
    to_mal_date,
    validate_date_pair,
)


class TestParseDateValid(unittest.TestCase):
    def test_none_and_empty(self):
        for v in (None, "", False, "null", "None", "undefined", "0000-00-00"):
            self.assertIsNone(parse_date(v), msg=repr(v))

    def test_iso_string(self):
        self.assertEqual(parse_date("2020-01-05"), date(2020, 1, 5))
        self.assertEqual(parse_date("2020-1-5"), date(2020, 1, 5))  # MAL-style
        self.assertEqual(parse_date("1999-12-31"), date(1999, 12, 31))

    def test_iso_datetime(self):
        self.assertEqual(parse_date("2020-06-15T12:30:00Z"), date(2020, 6, 15))
        self.assertEqual(parse_date("2020-06-15T12:30:00+00:00"), date(2020, 6, 15))

    def test_year_only(self):
        self.assertEqual(parse_date("2018"), date(2018, 1, 1))

    def test_date_and_datetime_objects(self):
        self.assertEqual(parse_date(date(2021, 3, 4)), date(2021, 3, 4))
        self.assertEqual(
            parse_date(datetime(2021, 3, 4, 8, 0, tzinfo=timezone.utc)),
            date(2021, 3, 4),
        )

    def test_fuzzy_dict(self):
        self.assertEqual(
            parse_date({"year": 2022, "month": 5, "day": 10}),
            date(2022, 5, 10),
        )
        # Missing month/day → defaults to 1
        self.assertEqual(parse_date({"year": 2022}), date(2022, 1, 1))
        self.assertEqual(parse_date({"year": 2022, "month": 7}), date(2022, 7, 1))

    def test_fuzzy_missing_year(self):
        self.assertIsNone(parse_date({"month": 5, "day": 1}))
        self.assertIsNone(parse_date({}))


class TestParseDateInvalid(unittest.TestCase):
    def test_impossible_calendar_days(self):
        self.assertIsNone(parse_date("2020-02-30"))
        self.assertIsNone(parse_date("2021-04-31"))
        self.assertIsNone(parse_date("2019-13-01"))
        self.assertIsNone(parse_date({"year": 2020, "month": 2, "day": 30}))
        self.assertIsNone(parse_date({"year": 2021, "month": 13, "day": 1}))
        # month/day 0 treated as missing → defaults to 1 (year-only FuzzyDate)
        self.assertEqual(parse_date({"year": 2021, "month": 0, "day": 0}), date(2021, 1, 1))

    def test_out_of_range_years(self):
        self.assertIsNone(parse_date("1899-01-01"))
        self.assertIsNone(parse_date("1949-12-31"))
        self.assertIsNone(parse_date("2101-01-01"))
        self.assertIsNone(parse_date({"year": 1800, "month": 1, "day": 1}))

    def test_garbage_strings(self):
        for v in ("not-a-date", "bogus", "true", "false", "0", "1", "abc-def-ghi"):
            self.assertIsNone(parse_date(v), msg=repr(v))

    def test_strict_raises(self):
        with self.assertRaises(InvalidDateError):
            parse_date("bogus", strict=True)
        with self.assertRaises(InvalidDateError):
            parse_date("2020-02-30", strict=True)
        with self.assertRaises(InvalidDateError):
            parse_date("1890-01-01", strict=True)
        # Empty still returns None even in strict mode
        self.assertIsNone(parse_date(None, strict=True))
        self.assertIsNone(parse_date("", strict=True))

    def test_safe_parse_never_raises(self):
        self.assertIsNone(safe_parse_date("%%%"))
        self.assertIsNone(safe_parse_date(object()))
        self.assertEqual(safe_parse_date("2020-03-03"), date(2020, 3, 3))


class TestValidateDatePair(unittest.TestCase):
    def test_both_none(self):
        self.assertEqual(validate_date_pair(None, None), (None, None))

    def test_only_started(self):
        s = date(2020, 1, 1)
        self.assertEqual(validate_date_pair(s, None), (s, None))

    def test_only_completed(self):
        c = date(2020, 6, 1)
        self.assertEqual(validate_date_pair(None, c), (None, c))

    def test_valid_order(self):
        s, c = date(2019, 1, 1), date(2019, 3, 1)
        self.assertEqual(validate_date_pair(s, c), (s, c))

    def test_same_day_ok(self):
        d = date(2020, 5, 5)
        self.assertEqual(validate_date_pair(d, d), (d, d))

    def test_inverted_clears_completed(self):
        s, c = date(2022, 1, 1), date(2020, 1, 1)
        out_s, out_c = validate_date_pair(s, c)
        self.assertEqual(out_s, s)
        self.assertIsNone(out_c)


class TestFormatters(unittest.TestCase):
    def test_to_iso(self):
        self.assertEqual(to_iso_date(date(2020, 1, 5)), "2020-01-05")
        self.assertIsNone(to_iso_date(None))
        self.assertIsNone(to_iso_date(date(1890, 1, 1)))  # out of range

    def test_to_mal(self):
        self.assertEqual(to_mal_date(date(2020, 1, 5)), "2020-01-05")
        self.assertEqual(to_mal_date(date(1999, 12, 3)), "1999-12-03")
        self.assertIsNone(to_mal_date(None))

    def test_to_fuzzy(self):
        self.assertEqual(
            to_fuzzy(date(2021, 8, 9)),
            {"year": 2021, "month": 8, "day": 9},
        )
        self.assertIsNone(to_fuzzy(None))

    def test_to_kitsu(self):
        s = to_kitsu_dt(date(2020, 1, 5))
        self.assertIsNotNone(s)
        self.assertTrue(s.startswith("2020-01-05T12:00:00"))
        self.assertTrue(s.endswith("Z"))
        self.assertIsNone(to_kitsu_dt(None))


class TestOlder(unittest.TestCase):
    def test_both(self):
        self.assertEqual(older(date(2019, 1, 1), date(2020, 1, 1)), date(2019, 1, 1))
        self.assertEqual(older(date(2020, 1, 1), date(2019, 1, 1)), date(2019, 1, 1))

    def test_one_none(self):
        d = date(2020, 1, 1)
        self.assertEqual(older(None, d), d)
        self.assertEqual(older(d, None), d)
        self.assertIsNone(older(None, None))


class TestMergePlatformDates(unittest.TestCase):
    def test_single_platform(self):
        m = merge_platform_dates(
            None, "mal", {"started_at": "2019-03-10", "completed_at": "2019-04-01"}
        )
        self.assertEqual(m["started_at"], "2019-03-10")
        self.assertEqual(m["completed_at"], "2019-04-01")
        self.assertIn("mal", m["sources"])

    def test_oldest_wins(self):
        m = merge_platform_dates(
            None, "anilist", {"started_at": {"year": 2022, "month": 5, "day": 1}}
        )
        m = merge_platform_dates(
            m, "mal", {"started_at": "2019-03-10", "completed_at": "2019-04-01"}
        )
        self.assertEqual(m["started_at"], "2019-03-10")
        self.assertEqual(m["completed_at"], "2019-04-01")

    def test_invalid_skipped(self):
        m = merge_platform_dates(
            None, "mal", {"started_at": "bad", "completed_at": "2019-04-01"}
        )
        self.assertIsNone(m.get("started_at"))
        self.assertEqual(m.get("completed_at"), "2019-04-01")

    def test_inverted_pair_from_platform(self):
        m = merge_platform_dates(
            None,
            "mal",
            {"started_at": "2022-01-01", "completed_at": "2020-01-01"},
        )
        self.assertEqual(m.get("started_at"), "2022-01-01")
        self.assertIsNone(m.get("completed_at"))

    def test_does_not_overwrite_with_invalid(self):
        m = merge_platform_dates(
            None, "mal", {"started_at": "2018-01-01", "completed_at": "2018-02-01"}
        )
        m = merge_platform_dates(m, "anilist", {"started_at": "not-a-date"})
        self.assertEqual(m["started_at"], "2018-01-01")
        self.assertEqual(m["completed_at"], "2018-02-01")


class TestSanitizeDatesForPush(unittest.TestCase):
    def test_clean_passthrough(self):
        out = sanitize_dates_for_push(
            {"started_at": "2019-03-10", "completed_at": "2019-04-01"}
        )
        self.assertEqual(out["started_at"], "2019-03-10")
        self.assertEqual(out["completed_at"], "2019-04-01")

    def test_drops_invalid(self):
        out = sanitize_dates_for_push(
            {"started_at": "bogus", "completed_at": "2020-05-01"}
        )
        self.assertNotIn("started_at", out)
        self.assertEqual(out["completed_at"], "2020-05-01")

    def test_inverted_pair(self):
        out = sanitize_dates_for_push(
            {"started_at": "2022-01-01", "completed_at": "2020-01-01"}
        )
        self.assertEqual(out.get("started_at"), "2022-01-01")
        self.assertNotIn("completed_at", out)

    def test_empty(self):
        self.assertEqual(sanitize_dates_for_push(None), {})
        self.assertEqual(sanitize_dates_for_push({}), {})


class TestDatesNeedPush(unittest.TestCase):
    def test_missing_on_platform(self):
        self.assertTrue(
            dates_need_push({}, {"started_at": "2019-01-01"})
        )

    def test_platform_newer(self):
        self.assertTrue(
            dates_need_push(
                {"started_at": "2022-01-01"},
                {"started_at": "2019-01-01"},
            )
        )

    def test_platform_already_oldest(self):
        self.assertFalse(
            dates_need_push(
                {"started_at": "2019-01-01"},
                {"started_at": "2019-01-01"},
            )
        )

    def test_platform_older_than_canonical_skip(self):
        # Platform already has older — no push needed for that field
        self.assertFalse(
            dates_need_push(
                {"started_at": "2015-01-01"},
                {"started_at": "2019-01-01"},
            )
        )

    def test_invalid_canonical_ignored(self):
        self.assertFalse(dates_need_push({}, {"started_at": "not-a-date"}))


if __name__ == "__main__":
    unittest.main()
