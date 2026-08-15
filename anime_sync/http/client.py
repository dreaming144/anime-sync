"""Unified HTTP entrypoint: rate limit → bulkhead → circuit → request + retry."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from .bulkhead import bulkhead_status, get_bulkhead
from .circuit import CircuitOpenError, circuit_status, get_circuit
from .rate_limit import get_rate_limiter, rate_limiter_status
from .util import service_key as _service_key

def request_with_retries(method, url, *, max_retries=5, base_sleep=1.0, use_circuit=True, use_bulkhead=True, use_rate_limit=True, **kwargs):
    """HTTP helper: rate limit → bulkhead → circuit → request + 429 backoff.

    kwargs passed to requests.request (headers, json, data, params, timeout, ...).
    Returns the final Response (may still be non-OK after retries exhausted).
    Raises CircuitOpenError if the service circuit is OPEN.
    Raises TimeoutError if the bulkhead cannot acquire a slot.
    """
    timeout = kwargs.pop("timeout", 30)
    breaker = get_circuit(url) if use_circuit else None
    if breaker and not breaker.allow():
        raise CircuitOpenError(
            f"circuit open for {_service_key(url)} "
            f"(retry after {breaker.recovery_timeout:.0f}s)"
        )

    limiter = get_rate_limiter(url) if use_rate_limit else None
    bulkhead_ctx = get_bulkhead(url) if use_bulkhead else None

    def _run():
        last = None
        for attempt in range(max_retries):
            if limiter:
                limiter.acquire()
            try:
                last = requests.request(method, url, timeout=timeout, **kwargs)
            except requests.RequestException as e:
                if breaker:
                    breaker.record_failure()
                if attempt >= max_retries - 1:
                    raise
                wait = base_sleep * (2 ** attempt)
                print(f"   network error {e}; retry in {wait:.1f}s")
                time.sleep(wait)
                continue

            if limiter and last is not None:
                try:
                    limiter.note_headers(last.headers)
                except Exception:
                    pass

            if last.status_code in (401, 404):
                if breaker:
                    breaker.record_success()
                return last

            if last.status_code in (429, 503, 502, 500):
                if breaker:
                    breaker.record_failure()
                retry_after = last.headers.get("Retry-After")
                if limiter:
                    try:
                        limiter.record_throttle(
                            last.status_code,
                            retry_after if last.status_code == 429 else None,
                        )
                    except Exception:
                        pass
                if attempt >= max_retries - 1:
                    return last
                reset = last.headers.get("X-RateLimit-Reset") or last.headers.get("x-ratelimit-reset")
                if retry_after and str(retry_after).replace(".", "", 1).isdigit():
                    wait = float(retry_after) + 1
                elif reset and str(reset).isdigit():
                    wait = max(5, int(reset) - int(time.time()) + 2)
                else:
                    wait = min(120, base_sleep * (2 ** attempt) * 2)
                wait = min(max(wait, 2), 180)
                print(
                    f"   API {last.status_code} {url[:60]} — backoff {wait:.0f}s "
                    f"(attempt {attempt+1}/{max_retries})"
                )
                time.sleep(wait)
                continue

            if last.ok or last.status_code < 500:
                if breaker:
                    breaker.record_success()
                if limiter and last.ok:
                    try:
                        limiter.record_success()
                    except Exception:
                        pass
                return last

            if breaker:
                breaker.record_failure()
            return last
        return last

    if bulkhead_ctx is not None:
        with bulkhead_ctx:
            return _run()
    return _run()




def write_circuit_metrics(path="circuit_metrics.json"):
    """Persist circuit + bulkhead metrics for Actions artifacts / debugging."""
    payload = {
        "circuits": circuit_status(),
        "bulkheads": {},
        "rate_limiters": {},
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        payload["bulkheads"] = bulkhead_status()
    except Exception:
        pass
    try:
        payload["rate_limiters"] = rate_limiter_status()
    except Exception:
        pass
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
