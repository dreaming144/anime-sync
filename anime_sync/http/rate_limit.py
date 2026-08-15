"""Adaptive per-service rate limiting with 429 feedback."""
from __future__ import annotations

import json
import os
import random
import threading
import time
from pathlib import Path
from email.utils import parsedate_to_datetime

from .util import service_key as _service_key


class RateLimiter:
    """Adaptive rate limiter: base spacing + per-minute budget, adjusts on feedback.

    - Starts at base min_interval / per_minute
    - On 429/503: multiplies interval (exponential, capped); tracks consecutive throttles
    - On sustained success: decays interval toward base
    - Honors X-RateLimit-* and Retry-After when present
    """

    def __init__(self, name, min_interval=0.0, per_minute=None, max_interval=None):
        self.name = name
        self.base_interval = float(min_interval)
        self.min_interval = float(min_interval)
        self.per_minute = int(per_minute) if per_minute else None
        self.base_per_minute = self.per_minute
        self.max_interval = float(max_interval) if max_interval else max(self.base_interval * 16, 30.0)
        self._lock = threading.Lock()
        self._last = 0.0
        self._window_start = 0.0
        self._window_count = 0
        self.waits = 0
        self.total = 0
        self.success_streak = 0
        self.throttle_events = 0
        self.recover_events = 0
        self.consecutive_throttles = 0
        self.last_throttle_at = 0.0
        self.last_retry_after = 0.0

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
        """Call after a healthy response; gradually speed up."""
        with self._lock:
            self.success_streak += 1
            self.consecutive_throttles = 0
            if self.success_streak >= 8 and self.min_interval > self.base_interval + 0.01:
                old = self.min_interval
                self.min_interval = max(self.base_interval, self.min_interval * 0.85)
                if self.base_per_minute:
                    self.per_minute = min(
                        self.base_per_minute,
                        int((self.per_minute or self.base_per_minute) * 1.1)
                        or self.base_per_minute,
                    )
                self.success_streak = 0
                self.recover_events += 1
                if old - self.min_interval > 0.05 and self.recover_events % 2 == 1:
                    print(
                        f"   rate {self.name}: recover interval "
                        f"{old:.2f}s → {self.min_interval:.2f}s"
                    )

    def record_throttle(self, status_code=429, retry_after=None):
        """Call on 429/503 — raise spacing; does NOT sleep (caller backs off).

        Returns suggested wait seconds (0 if none from Retry-After).
        """
        extra = parse_retry_after(retry_after)
        with self._lock:
            self.success_streak = 0
            self.throttle_events += 1
            self.consecutive_throttles += 1
            self.last_throttle_at = time.time()
            self.last_retry_after = extra
            old = self.min_interval
            # Stronger multiplier on consecutive 429s
            if status_code == 429:
                factor = min(4.0, 2.0 * (1.25 ** (self.consecutive_throttles - 1)))
            else:
                factor = 1.5
            self.min_interval = min(
                self.max_interval,
                max(self.base_interval, self.min_interval * factor),
            )
            if self.per_minute and self.base_per_minute:
                self.per_minute = max(5, int(self.per_minute * 0.5))
            print(
                f"   rate {self.name}: throttle ({status_code}) "
                f"#{self.consecutive_throttles} "
                f"interval {old:.2f}s → {self.min_interval:.2f}s"
                + (f" retry-after={extra:.0f}s" if extra else "")
            )
        return extra

    def note_headers(self, headers):
        """Slow down if upstream reports low remaining quota."""
        if not headers:
            return
        remaining = headers.get("X-RateLimit-Remaining") or headers.get("x-ratelimit-remaining")
        limit = headers.get("X-RateLimit-Limit") or headers.get("x-ratelimit-limit")
        try:
            if remaining is not None and str(remaining).isdigit():
                rem = int(remaining)
                if rem <= 2:
                    reset = headers.get("X-RateLimit-Reset") or headers.get("x-ratelimit-reset")
                    wait = 15.0
                    if reset and str(reset).isdigit():
                        wait = max(1.0, float(int(reset) - int(time.time()) + 1))
                        wait = min(wait, 90.0)
                    print(f"   rate {self.name}: remaining={rem}; pause {wait:.0f}s")
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

    def metrics(self):
        return {
            "min_interval": round(self.min_interval, 3),
            "base_interval": self.base_interval,
            "max_interval": self.max_interval,
            "per_minute": self.per_minute,
            "total": self.total,
            "waits": self.waits,
            "throttle_events": self.throttle_events,
            "recover_events": self.recover_events,
            "consecutive_throttles": self.consecutive_throttles,
            "success_streak": self.success_streak,
        }


def parse_retry_after(value) -> float:
    """Parse Retry-After as seconds (int/float) or HTTP-date. Returns 0 if unknown."""
    if value is None or value is False:
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    try:
        return max(0.0, float(s))
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(s)
        if dt is not None:
            return max(0.0, dt.timestamp() - time.time())
    except (TypeError, ValueError, OverflowError):
        pass
    return 0.0


def compute_backoff(
    attempt: int,
    *,
    base_sleep: float = 1.0,
    retry_after: float = 0.0,
    reset_epoch: int | None = None,
    status_code: int = 429,
    max_wait: float = 180.0,
    min_wait: float = 2.0,
) -> float:
    """Adaptive wait for 429/5xx: prefer Retry-After, else exp backoff + full jitter.

    full jitter: random.uniform(0, min(cap, base * 2^attempt))
    """
    if retry_after > 0:
        return min(max(retry_after + random.uniform(0.1, 1.0), min_wait), max_wait)
    if reset_epoch is not None:
        try:
            until = float(reset_epoch) - time.time() + 1.0
            if until > 0:
                return min(max(until, min_wait), max_wait)
        except (TypeError, ValueError):
            pass
    # Exponential with full jitter (AWS-style)
    cap = min(max_wait, base_sleep * (2 ** attempt) * (2.0 if status_code == 429 else 1.5))
    wait = random.uniform(min_wait, max(min_wait, cap))
    return min(wait, max_wait)


_rate_limiters = {}

# Conservative published / observed limits (Aug 2026)
RATE_LIMITS = {
    "anilist": {"min_interval": 2.1, "per_minute": 25},
    "jikan": {"min_interval": 0.4, "per_minute": 55},
    "mal": {"min_interval": 0.35, "per_minute": 90},
    "kitsu": {"min_interval": 0.5, "per_minute": 60},
    # SIMKL: community ~10 GET/s, 1 write/s — stay conservative
    "simkl": {"min_interval": 0.2, "per_minute": 120},
    "arm": {"min_interval": 0.12, "per_minute": 200},
    "animeapi": {"min_interval": 0.25, "per_minute": 100},
    "anizip": {"min_interval": 0.3, "per_minute": 80},
    "github": {"min_interval": 0.5, "per_minute": 60},
}


def get_rate_limiter(url):
    ensure_rate_limits_loaded()
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
            "consecutive_throttles": r.consecutive_throttles,
        }
        for name, r in _rate_limiters.items()
    }


def reset_rate_limiters():
    """Tests only."""
    _rate_limiters.clear()


# ---------------------------------------------------------------------------
# Distributed (cross-run) rate-limit state — mirrors circuit_state.json
# ---------------------------------------------------------------------------
RATE_LIMIT_STATE_PATH = os.getenv("RATE_LIMIT_STATE_PATH", "rate_limit_state.json")
_rate_state_loaded = False


def load_rate_limit_state(path: str | None = None) -> dict:
    """Restore adaptive intervals from disk so 429 backoff survives Actions runs."""
    global _rate_state_loaded
    path = path or RATE_LIMIT_STATE_PATH
    p = Path(path)
    if not p.is_file():
        _rate_state_loaded = True
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"   rate state load skipped: {e}")
        _rate_state_loaded = True
        return {}

    limiters = raw.get("limiters") or {}
    now = time.time()
    restored = 0
    for name, snap in limiters.items():
        if not isinstance(snap, dict):
            continue
        if name not in _rate_limiters:
            base = float(snap.get("base_interval") or RATE_LIMITS.get(name, {}).get("min_interval", 0.2))
            pm = snap.get("base_per_minute") or RATE_LIMITS.get(name, {}).get("per_minute", 120)
            max_i = float(snap.get("max_interval") or max(base * 16, 30.0))
            _rate_limiters[name] = RateLimiter(name, min_interval=base, per_minute=pm, max_interval=max_i)

        rl = _rate_limiters[name]
        try:
            saved_interval = float(snap.get("min_interval") or rl.base_interval)
            # Decay toward base if the last throttle was long ago (>1h)
            last_t = float(snap.get("last_throttle_at") or 0)
            if last_t and (now - last_t) > 3600:
                # Soft recovery: halfway between saved and base
                saved_interval = (saved_interval + rl.base_interval) / 2.0
            rl.min_interval = min(rl.max_interval, max(rl.base_interval, saved_interval))
            if snap.get("per_minute") is not None:
                rl.per_minute = max(5, int(snap["per_minute"]))
            if snap.get("base_per_minute") is not None:
                rl.base_per_minute = int(snap["base_per_minute"])
            rl.throttle_events = int(snap.get("throttle_events") or 0)
            rl.recover_events = int(snap.get("recover_events") or 0)
            rl.consecutive_throttles = int(snap.get("consecutive_throttles") or 0)
            if last_t:
                rl.last_throttle_at = last_t
            restored += 1
            if rl.min_interval > rl.base_interval + 0.05:
                print(
                    f"   rate {name}: restored interval "
                    f"{rl.min_interval:.2f}s (base {rl.base_interval:.2f}s)"
                )
        except (TypeError, ValueError) as e:
            print(f"   rate {name}: restore skip ({e})")

    if restored:
        print(f"   Rate limits: restored {restored} limiter(s) from {path}")
    _rate_state_loaded = True
    return limiters


def save_rate_limit_state(path: str | None = None) -> str | None:
    """Persist current adaptive intervals for the next process/run."""
    path = path or RATE_LIMIT_STATE_PATH
    limiters = {}
    for name, rl in _rate_limiters.items():
        limiters[name] = {
            "min_interval": rl.min_interval,
            "base_interval": rl.base_interval,
            "max_interval": rl.max_interval,
            "per_minute": rl.per_minute,
            "base_per_minute": rl.base_per_minute,
            "throttle_events": rl.throttle_events,
            "recover_events": rl.recover_events,
            "consecutive_throttles": rl.consecutive_throttles,
            "last_throttle_at": rl.last_throttle_at or None,
            "total": rl.total,
            "waits": rl.waits,
        }
    payload = {"version": 1, "updated_at": time.time(), "limiters": limiters}
    p = Path(path)
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, p)
        print(f"   Rate limits: saved {len(limiters)} limiter(s) → {path}")
        return path
    except OSError as e:
        print(f"   rate state save failed: {e}")
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass
        return None


def ensure_rate_limits_loaded(path: str | None = None) -> None:
    global _rate_state_loaded
    if _rate_state_loaded:
        return
    load_rate_limit_state(path)
