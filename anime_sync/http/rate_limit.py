"""Adaptive per-service rate limiting."""
import threading
import time

from .util import service_key as _service_key

class RateLimiter:
    """Adaptive rate limiter: base spacing + per-minute budget, adjusts on feedback.

    - Starts at base min_interval / per_minute
    - On 429/503: multiplies current interval (up to max_interval)
    - On sustained success: slowly decays interval toward base
    - Honors X-RateLimit-* headers when present
    """

    def __init__(self, name, min_interval=0.0, per_minute=None, max_interval=None):
        self.name = name
        self.base_interval = float(min_interval)
        self.min_interval = float(min_interval)  # current adaptive interval
        self.per_minute = int(per_minute) if per_minute else None
        self.base_per_minute = self.per_minute
        self.max_interval = float(max_interval) if max_interval else max(self.base_interval * 8, 8.0)
        self._lock = threading.Lock()
        self._last = 0.0
        self._window_start = 0.0
        self._window_count = 0
        self.waits = 0
        self.total = 0
        self.success_streak = 0
        self.throttle_events = 0
        self.recover_events = 0

    def acquire(self):
        with self._lock:
            now = time.time()
            wait = 0.0
            if self.min_interval > 0 and self._last > 0:
                wait = max(wait, self.min_interval - (now - self._last))
            if self.per_minute:
                if now - self._window_start >= 60.0:
                    self._window_start = now
                    self._window_count = 0
                if self._window_count >= self.per_minute:
                    wait = max(wait, 60.0 - (now - self._window_start) + 0.05)
            if wait > 0:
                self.waits += 1
                time.sleep(wait)
                now = time.time()
                if self.per_minute and now - self._window_start >= 60.0:
                    self._window_start = now
                    self._window_count = 0
            self._last = time.time()
            self._window_count += 1
            self.total += 1

    def record_success(self):
        """Call after a healthy 2xx/4xx-client response; gradually speed up."""
        with self._lock:
            self.success_streak += 1
            # Every 8 successes, ease interval 15% toward base
            if self.success_streak >= 8 and self.min_interval > self.base_interval + 0.01:
                old = self.min_interval
                self.min_interval = max(
                    self.base_interval,
                    self.min_interval * 0.85,
                )
                if self.base_per_minute:
                    # restore budget slowly
                    self.per_minute = min(
                        self.base_per_minute,
                        int((self.per_minute or self.base_per_minute) * 1.1) or self.base_per_minute,
                    )
                self.success_streak = 0
                self.recover_events += 1
                # Log at most every other recover to reduce noise
                if old - self.min_interval > 0.05 and self.recover_events % 2 == 1:
                    print(
                        f"   rate {self.name}: recover interval "
                        f"{old:.2f}s → {self.min_interval:.2f}s"
                    )

    def record_throttle(self, status_code=429, retry_after=None):
        """Call on 429/503 — back off hard."""
        with self._lock:
            self.success_streak = 0
            self.throttle_events += 1
            old = self.min_interval
            factor = 2.0 if status_code == 429 else 1.5
            self.min_interval = min(self.max_interval, max(self.base_interval, self.min_interval * factor))
            if self.per_minute and self.base_per_minute:
                self.per_minute = max(5, int(self.per_minute * 0.5))
            extra = 0.0
            if retry_after is not None:
                try:
                    extra = float(retry_after)
                except (TypeError, ValueError):
                    extra = 0.0
            print(
                f"   rate {self.name}: throttle ({status_code}) "
                f"interval {old:.2f}s → {self.min_interval:.2f}s"
                + (f" +wait {extra:.0f}s" if extra else "")
            )
            if extra > 0:
                time.sleep(min(extra, 120))

    def note_headers(self, headers):
        """Slow down if upstream reports low remaining quota (AniList etc.)."""
        if not headers:
            return
        remaining = headers.get("X-RateLimit-Remaining") or headers.get("x-ratelimit-remaining")
        limit = headers.get("X-RateLimit-Limit") or headers.get("x-ratelimit-limit")
        try:
            if remaining is not None and str(remaining).isdigit():
                rem = int(remaining)
                if rem <= 2:
                    reset = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
                    if reset and str(reset).isdigit():
                        wait = max(1, int(reset) - int(time.time()) + 1)
                        wait = min(wait, 90)
                    else:
                        wait = 15
                    print(f"   rate {self.name}: remaining={rem}; pause {wait}s")
                    with self._lock:
                        self.min_interval = min(
                            self.max_interval,
                            max(self.min_interval, self.base_interval * 2),
                        )
                        self.success_streak = 0
                        self.throttle_events += 1
                    time.sleep(wait)
                elif rem <= 5 and limit and str(limit).isdigit() and int(limit) <= 90:
                    with self._lock:
                        self.min_interval = min(
                            self.max_interval,
                            max(self.min_interval, self.base_interval * 1.5),
                        )
                    time.sleep(1.0)
        except (TypeError, ValueError):
            pass


_rate_limiters = {}

# Conservative published / observed limits (Aug 2026)
# AniList: docs say 90/min, currently degraded to ~30/min — use 25/min + 2s spacing
# Jikan: 3/sec, 60/min → 0.4s min interval
# MAL official: no strict public number; be polite ~1.5/s
# Kitsu: undocumented; ~2/s safe
# SIMKL: community defaults ~10 GET/s, 1 write/s — we use 0.15s GET spacing
# ARM / animeapi / anizip: polite public scrapers
RATE_LIMITS = {
    "anilist": {"min_interval": 2.1, "per_minute": 25},   # degraded AniList
    "jikan": {"min_interval": 0.4, "per_minute": 55},
    "mal": {"min_interval": 0.35, "per_minute": 90},
    "kitsu": {"min_interval": 0.5, "per_minute": 60},
    "simkl": {"min_interval": 0.2, "per_minute": 120},
    "arm": {"min_interval": 0.12, "per_minute": 200},
    "animeapi": {"min_interval": 0.25, "per_minute": 100},
    "anizip": {"min_interval": 0.3, "per_minute": 80},
    "github": {"min_interval": 0.5, "per_minute": 60},
}


def get_rate_limiter(url):
    key = _service_key(url)
    if key not in _rate_limiters:
        cfg = RATE_LIMITS.get(key, {"min_interval": 0.2, "per_minute": 120})
        _rate_limiters[key] = RateLimiter(key, **cfg)
    return _rate_limiters[key]


def rate_limiter_status():
    return {
        name: {
            "total": r.total,
            "waits": r.waits,
            "base_interval": r.base_interval,
            "min_interval": round(r.min_interval, 3),
            "per_minute": r.per_minute,
            "throttle_events": r.throttle_events,
            "recover_events": r.recover_events,
        }
        for name, r in _rate_limiters.items()
    }


