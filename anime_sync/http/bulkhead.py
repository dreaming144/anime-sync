"""Bulkheads: limit concurrent HTTP calls and subsystem thread pools."""
import threading
from concurrent.futures import ThreadPoolExecutor

from .util import service_key as _service_key

class Bulkhead:
    """Limit concurrent in-flight calls per service (resource isolation).

    Prevents one slow/failing API from consuming all worker threads.
    """

    def __init__(self, name, max_concurrent=3, acquire_timeout=30.0):
        self.name = name
        self.max_concurrent = max_concurrent
        self.acquire_timeout = acquire_timeout
        self._sem = __import__("threading").Semaphore(max_concurrent)
        self.in_flight = 0
        self.rejected = 0
        self.total = 0
        self._lock = __import__("threading").Lock()

    def __enter__(self):
        ok = self._sem.acquire(timeout=self.acquire_timeout)
        if not ok:
            with self._lock:
                self.rejected += 1
            raise TimeoutError(
                f"bulkhead {self.name}: no slot within {self.acquire_timeout:.0f}s "
                f"(max_concurrent={self.max_concurrent})"
            )
        with self._lock:
            self.in_flight += 1
            self.total += 1
        return self

    def __exit__(self, exc_type, exc, tb):
        with self._lock:
            self.in_flight = max(0, self.in_flight - 1)
        self._sem.release()
        return False


_bulkheads = {}

# Max concurrent HTTP calls per upstream (bulkhead sizes)
BULKHEAD_LIMITS = {
    "anilist": 1,   # degraded ~30/min — serialize
    "mal": 2,
    "kitsu": 2,
    "simkl": 2,
    "arm": 3,
    "jikan": 1,   # 3/sec hard limit
    "anizip": 1,
    "animeapi": 1,
    "github": 2,
}


def get_bulkhead(url):
    key = _service_key(url)
    if key not in _bulkheads:
        limit = BULKHEAD_LIMITS.get(key, 3)
        _bulkheads[key] = Bulkhead(key, max_concurrent=limit)
    return _bulkheads[key]


def bulkhead_status():
    return {
        name: {
            "max": b.max_concurrent,
            "in_flight": b.in_flight,
            "total": b.total,
            "rejected": b.rejected,
        }
        for name, b in _bulkheads.items()
    }


class BulkheadPool:
    """Named thread-pool bulkhead for isolating whole subsystems (load vs enrich vs push)."""

    def __init__(self, name, max_workers):
        self.name = name
        self.max_workers = max_workers
        self._executor = None

    def executor(self):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self.max_workers,
                thread_name_prefix=f"bh-{self.name}",
            )
        return self._executor

    def shutdown(self, wait=True):
        if self._executor is not None:
            self._executor.shutdown(wait=wait)
            self._executor = None


# Subsystem pools — loaders / enrichment / pushes cannot steal each other's threads
POOL_LOAD = BulkheadPool("load", max_workers=4)      # one worker per platform loader
POOL_ENRICH = BulkheadPool("enrich", max_workers=2)  # keep under AniList/Jikan budgets
POOL_PUSH = BulkheadPool("push", max_workers=3)      # outbound list updates
