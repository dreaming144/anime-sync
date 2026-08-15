"""Circuit breaker for upstream HTTP services."""
import time

from .util import service_key as _service_key

class CircuitBreaker:
    """Per-service circuit breaker: CLOSED → OPEN → HALF_OPEN → CLOSED.

    OPEN after `failure_threshold` consecutive failures; stays open for
    `recovery_timeout` seconds, then allows a trial request (HALF_OPEN).

    Metrics (reset only on process start):
      calls, successes, failures, short_circuits, state_transitions,
      last_failure_at, last_success_at, last_open_at, open_count
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, name, failure_threshold=5, recovery_timeout=60.0, half_open_max=2):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max = half_open_max
        self.state = self.CLOSED
        self.consecutive_failures = 0
        self.half_open_trials = 0
        self.opened_at = 0.0
        # ---- metrics ----
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

    def allow(self):
        self.calls += 1
        if self.state == self.CLOSED:
            return True
        if self.state == self.OPEN:
            if time.time() - self.opened_at >= self.recovery_timeout:
                self.state = self.HALF_OPEN
                self.half_open_trials = 0
                self.half_open_count += 1
                print(f"   circuit {self.name}: OPEN → HALF_OPEN (trial)")
                return True
            self.short_circuits += 1
            return False
        # HALF_OPEN
        if self.half_open_trials < self.half_open_max:
            self.half_open_trials += 1
            return True
        self.short_circuits += 1
        return False

    def record_success(self):
        self.successes += 1
        self.last_success_at = time.time()
        self.consecutive_failures = 0
        if self.state == self.HALF_OPEN:
            self.state = self.CLOSED
            self.close_count += 1
            self.last_close_at = time.time()
            print(f"   circuit {self.name}: HALF_OPEN → CLOSED (recovered)")

    def record_failure(self):
        self.failures += 1
        self.consecutive_failures += 1
        self.last_failure_at = time.time()
        if self.state == self.HALF_OPEN:
            self.state = self.OPEN
            self.opened_at = time.time()
            self.open_count += 1
            self.last_open_at = self.opened_at
            print(f"   circuit {self.name}: HALF_OPEN → OPEN (trial failed)")
            return
        if self.consecutive_failures >= self.failure_threshold and self.state != self.OPEN:
            self.state = self.OPEN
            self.opened_at = time.time()
            self.open_count += 1
            self.last_open_at = self.opened_at
            print(
                f"   circuit {self.name}: CLOSED → OPEN "
                f"after {self.consecutive_failures} failures "
                f"(pause {self.recovery_timeout:.0f}s)"
            )

    def metrics(self):
        """Snapshot of counters + derived rates for logging/export."""
        total = max(self.calls, 1)
        return {
            "name": self.name,
            "state": self.state,
            "calls": self.calls,
            "successes": self.successes,
            "failures": self.failures,
            "short_circuits": self.short_circuits,
            "success_rate": round(self.successes / total, 4),
            "failure_rate": round(self.failures / total, 4),
            "open_count": self.open_count,
            "half_open_count": self.half_open_count,
            "close_count": self.close_count,
            "consecutive_failures": self.consecutive_failures,
            "opened_at": self.opened_at or None,
            "last_failure_at": self.last_failure_at or None,
            "last_success_at": self.last_success_at or None,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }




class CircuitOpenError(RuntimeError):
    """Raised when a circuit breaker is OPEN and requests are short-circuited."""


_circuit_breakers = {}


def get_circuit(url, failure_threshold=5, recovery_timeout=60.0):
    key = _service_key(url)
    defaults = {
        "anilist": (5, 90.0),
        "jikan": (3, 120.0),
        "mal": (5, 60.0),
        "kitsu": (5, 60.0),
        "arm": (5, 45.0),
        "animeapi": (5, 60.0),
        "simkl": (5, 60.0),
    }
    ft, rt = defaults.get(key, (failure_threshold, recovery_timeout))
    if key not in _circuit_breakers:
        _circuit_breakers[key] = CircuitBreaker(key, failure_threshold=ft, recovery_timeout=rt)
    return _circuit_breakers[key]


def circuit_status():
    """Return a full metrics snapshot for every circuit breaker."""
    return {name: b.metrics() for name, b in _circuit_breakers.items()}
