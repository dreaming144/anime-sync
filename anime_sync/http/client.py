"""Unified HTTP entrypoint: rate limit → bulkhead → circuit → request + retry."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from .bulkhead import bulkhead_status, get_bulkhead
from .circuit import CircuitOpenError, circuit_status, get_circuit, save_circuit_state, ensure_circuits_loaded
from .rate_limit import (
    compute_backoff,
    ensure_rate_limits_loaded,
    get_rate_limiter,
    parse_retry_after,
    rate_limiter_status,
    save_rate_limit_state,
)
from .util import service_key as _service_key

def request_with_retries(method, url, *, max_retries=5, base_sleep=1.0, use_circuit=True, use_bulkhead=True, use_rate_limit=True, **kwargs):
    ensure_circuits_loaded()
    """HTTP helper: rate limit → bulkhead → circuit → request + 429 backoff.

    kwargs passed to requests.request (headers, json, data, params, timeout, ...).
    Returns the final Response (may still be non-OK after retries exhausted).
    Raises CircuitOpenError if the service circuit is OPEN.
    Raises TimeoutError if the bulkhead cannot acquire a slot.
    """
    timeout = kwargs.pop("timeout", 30)
    breaker = get_circuit(url) if use_circuit else None
    if breaker and not breaker.allow():
        remaining = 0.0
        if breaker.opened_at:
            remaining = max(0.0, breaker.recovery_timeout - (time.time() - breaker.opened_at))
        err_bit = f"; last_error={breaker.last_error[:80]!r}" if breaker.last_error else ""
        raise CircuitOpenError(
            f"circuit open for {_service_key(url)} "
            f"(retry after {remaining:.0f}s; cooldown={breaker.recovery_timeout:.0f}s{err_bit})",
            circuit=breaker,
        )

    limiter = get_rate_limiter(url) if use_rate_limit else None
    bulkhead_ctx = get_bulkhead(url) if use_bulkhead else None

    def _run():
        last = None
        for attempt in range(max_retries):
            if limiter:
                limiter.acquire()
            # SIMKL community guidance: ~1 write/sec (POST/PUT/DELETE)
            if method.upper() in ("POST", "PUT", "PATCH", "DELETE"):
                from anime_sync.http.util import service_key as _sk
                if _sk(url) == "simkl":
                    import time as _time
                    extra = float(__import__("os").getenv("SIMKL_WRITE_INTERVAL", "1.0"))
                    if extra > 0:
                        _time.sleep(extra)
            try:
                last = requests.request(method, url, timeout=timeout, **kwargs)
            except requests.RequestException as e:
                if breaker:
                    breaker.record_failure(error=str(e))
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
                    breaker.record_failure(error=f"HTTP {last.status_code}")
                retry_hdr = last.headers.get("Retry-After") or last.headers.get("retry-after")
                retry_s = parse_retry_after(retry_hdr) if last.status_code == 429 else 0.0
                if limiter:
                    try:
                        # Adjust adaptive spacing; sleep is done below once
                        suggested = limiter.record_throttle(
                            last.status_code,
                            retry_hdr if last.status_code == 429 else None,
                        )
                        if suggested and suggested > retry_s:
                            retry_s = suggested
                    except Exception:
                        pass
                if attempt >= max_retries - 1:
                    return last
                reset = last.headers.get("X-RateLimit-Reset") or last.headers.get("x-ratelimit-reset")
                reset_epoch = int(reset) if reset and str(reset).isdigit() else None
                wait = compute_backoff(
                    attempt,
                    base_sleep=base_sleep,
                    retry_after=retry_s,
                    reset_epoch=reset_epoch,
                    status_code=last.status_code,
                )
                print(
                    f"   API {last.status_code} {url[:60]} — adaptive backoff {wait:.1f}s "
                    f"(attempt {attempt+1}/{max_retries}"
                    f"{'; retry-after=' + str(retry_hdr) if retry_hdr else ''})"
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
                breaker.record_failure(error=f"HTTP {last.status_code}")
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
    try:
        save_circuit_state()
        save_rate_limit_state()
    except Exception:
        pass
    return payload
