"""Circuit breaker for upstream HTTP services.

States: CLOSED → OPEN → HALF_OPEN → CLOSED (or back to OPEN on trial failure).

Design notes (classic + practical refinements):
  - Trip OPEN after `failure_threshold` consecutive failures while CLOSED.
  - Stay OPEN for `recovery_timeout` (grows exponentially up to a cap after
    repeated opens, so a chronically down API backs off harder).
  - HALF_OPEN allows up to `half_open_max` trial calls; needs
    `success_threshold` successes to fully CLOSE again.
  - Any failure in HALF_OPEN immediately re-opens.
  - Metrics are process-lifetime (good for Actions artifacts / job summary).
"""
from __future__ import annotations

import os
import time
from typing import Any

from .util import service_key as _service_key


class CircuitBreaker:
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max: int = 2,
        success_threshold: int = 2,
        max_recovery_timeout: float = 300.0,
        recovery_backoff: float = 2.0,
    ):
        self.name = name
        self.failure_threshold = max(1, int(failure_threshold))
        self.base_recovery_timeout = float(recovery_timeout)
        self.recovery_timeout = float(recovery_timeout)
        self.max_recovery_timeout = float(max_recovery_timeout)
        self.recovery_backoff = float(recovery_backoff)
        self.half_open_max = max(1, int(half_open_max))
        self.success_threshold = max(1, int(success_threshold))

        self.state = self.CLOSED
        self.consecutive_failures = 0
        self.half_open_trials = 0
        self.half_open_successes = 0
        self.opened_at = 0.0

        # metrics
        self.calls = 0
        self.successes = 0
        self.failures = 0
        self.short_circuits = 0
        self.open_count = 0
        self.half_open_count = 0
        self.close_count = 0
        self.last_failure_at = 0.0
        self.last_success_at = 0.0
        self.last_open_at = 0.0
        self.last_close_at = 0.0
        self.last_error: str = ""

    # ------------------------------------------------------------------ API

    def allow(self) -> bool:
        """Return True if a call may proceed; False if short-circuited."""
        self.calls += 1
        if self.state == self.CLOSED:
            return True

        if self.state == self.OPEN:
            if time.time() - self.opened_at >= self.recovery_timeout:
                self._to_half_open()
                return True
            self.short_circuits += 1
            return False

        # HALF_OPEN — limited probe traffic
        if self.half_open_trials < self.half_open_max:
            self.half_open_trials += 1
            return True
        self.short_circuits += 1
        return False

    def record_success(self) -> None:
        self.successes += 1
        self.last_success_at = time.time()
        self.consecutive_failures = 0

        if self.state == self.HALF_OPEN:
            self.half_open_successes += 1
            if self.half_open_successes >= self.success_threshold:
                self._to_closed()
        # CLOSED: nothing else to do

    def record_failure(self, error: str | None = None) -> None:
        self.failures += 1
        self.consecutive_failures += 1
        self.last_failure_at = time.time()
        if error:
            self.last_error = str(error)[:200]

        if self.state == self.HALF_OPEN:
            # Probe failed — trip open again, lengthen cooldown
            self._to_open(reason="half_open_trial_failed")
            return

        if self.state == self.CLOSED and self.consecutive_failures >= self.failure_threshold:
            self._to_open(reason=f"{self.consecutive_failures}_consecutive_failures")

    def reset(self) -> None:
        """Force CLOSED (tests / manual recovery)."""
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self.half_open_trials = 0
        self.half_open_successes = 0
        self.recovery_timeout = self.base_recovery_timeout
        self.last_close_at = time.time()

    def metrics(self) -> dict[str, Any]:
        now = time.time()
        open_remaining = 0.0
        if self.state == self.OPEN and self.opened_at:
            open_remaining = max(0.0, self.recovery_timeout - (now - self.opened_at))
        return {
            "state": self.state,
            "calls": self.calls,
            "successes": self.successes,
            "failures": self.failures,
            "short_circuits": self.short_circuits,
            "consecutive_failures": self.consecutive_failures,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "open_remaining_s": round(open_remaining, 1),
            "open_count": self.open_count,
            "half_open_count": self.half_open_count,
            "close_count": self.close_count,
            "half_open_trials": self.half_open_trials,
            "half_open_successes": self.half_open_successes,
            "success_threshold": self.success_threshold,
            "last_error": self.last_error,
            "last_failure_at": self.last_failure_at or None,
            "last_success_at": self.last_success_at or None,
            "last_open_at": self.last_open_at or None,
            "last_close_at": self.last_close_at or None,
        }

    # --------------------------------------------------------------- states

    def _to_open(self, reason: str = "") -> None:
        was = self.state
        # Exponential cooldown after repeated opens
        if was == self.HALF_OPEN or self.open_count > 0:
            self.recovery_timeout = min(
                self.max_recovery_timeout,
                self.recovery_timeout * self.recovery_backoff,
            )
        self.state = self.OPEN
        self.opened_at = time.time()
        self.last_open_at = self.opened_at
        self.open_count += 1
        self.half_open_trials = 0
        self.half_open_successes = 0
        print(
            f"   circuit {self.name}: {was.upper()} → OPEN "
            f"(reason={reason or 'threshold'}; cooldown={self.recovery_timeout:.0f}s)"
        )

    def _to_half_open(self) -> None:
        self.state = self.HALF_OPEN
        self.half_open_trials = 0
        self.half_open_successes = 0
        self.half_open_count += 1
        print(f"   circuit {self.name}: OPEN → HALF_OPEN (trial window)")

    def _to_closed(self) -> None:
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self.half_open_trials = 0
        self.half_open_successes = 0
        self.recovery_timeout = self.base_recovery_timeout  # reset backoff
        self.close_count += 1
        self.last_close_at = time.time()
        print(f"   circuit {self.name}: HALF_OPEN → CLOSED (recovered)")


class CircuitOpenError(RuntimeError):
    """Raised when a circuit breaker is OPEN and requests are short-circuited."""

    def __init__(self, message: str, circuit: CircuitBreaker | None = None):
        super().__init__(message)
        self.circuit = circuit
        if circuit is not None:
            m = circuit.metrics()
            self.remaining_s = m.get("open_remaining_s", 0)
        else:
            self.remaining_s = 0


# Per-service overrides (name substring → kwargs)
_SERVICE_DEFAULTS: dict[str, dict[str, Any]] = {
    "anilist": {"failure_threshold": 5, "recovery_timeout": 45.0, "success_threshold": 2},
    "mal": {"failure_threshold": 4, "recovery_timeout": 60.0, "success_threshold": 2},
    "simkl": {"failure_threshold": 5, "recovery_timeout": 60.0, "success_threshold": 2},
    "kitsu": {"failure_threshold": 5, "recovery_timeout": 60.0, "success_threshold": 2},
    "arm": {"failure_threshold": 6, "recovery_timeout": 30.0, "success_threshold": 1},
    "jikan": {"failure_threshold": 4, "recovery_timeout": 90.0, "success_threshold": 2},
}

_circuit_breakers: dict[str, CircuitBreaker] = {}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def get_circuit(
    url: str,
    failure_threshold: int | None = None,
    recovery_timeout: float | None = None,
    half_open_max: int | None = None,
    success_threshold: int | None = None,
) -> CircuitBreaker:
    """Get or create a circuit breaker for the service behind `url`."""
    key = _service_key(url)

    if key not in _circuit_breakers:
        cfg: dict[str, Any] = {
            "failure_threshold": _env_int("CIRCUIT_FAILURE_THRESHOLD", 5),
            "recovery_timeout": _env_float("CIRCUIT_RECOVERY_TIMEOUT", 60.0),
            "half_open_max": _env_int("CIRCUIT_HALF_OPEN_MAX", 2),
            "success_threshold": _env_int("CIRCUIT_SUCCESS_THRESHOLD", 2),
            "max_recovery_timeout": _env_float("CIRCUIT_MAX_RECOVERY_TIMEOUT", 300.0),
        }
        key_l = key.lower()
        if key_l in _SERVICE_DEFAULTS:
            cfg.update(_SERVICE_DEFAULTS[key_l])
        else:
            for needle, overrides in _SERVICE_DEFAULTS.items():
                if needle in key_l:
                    cfg.update(overrides)
                    break
        if failure_threshold is not None:
            cfg["failure_threshold"] = failure_threshold
        if recovery_timeout is not None:
            cfg["recovery_timeout"] = recovery_timeout
        if half_open_max is not None:
            cfg["half_open_max"] = half_open_max
        if success_threshold is not None:
            cfg["success_threshold"] = success_threshold
        _circuit_breakers[key] = CircuitBreaker(key, **cfg)

    return _circuit_breakers[key]


def circuit_status() -> dict[str, dict[str, Any]]:
    """Return a full metrics snapshot for every circuit breaker."""
    return {name: b.metrics() for name, b in _circuit_breakers.items()}


def reset_all_circuits() -> None:
    """Clear registry (tests)."""
    _circuit_breakers.clear()
