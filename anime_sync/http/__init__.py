"""HTTP resilience: rate limiting, circuit breakers, bulkheads, unified client."""
from .bulkhead import (
    BULKHEAD_LIMITS,
    Bulkhead,
    BulkheadPool,
    POOL_ENRICH,
    POOL_LOAD,
    POOL_PUSH,
    bulkhead_status,
    get_bulkhead,
)
from .circuit import (
    CircuitBreaker,
    CircuitOpenError,
    circuit_status,
    ensure_circuits_loaded,
    get_circuit,
    load_circuit_state,
    reset_all_circuits,
    save_circuit_state,
)
from .client import request_with_retries, write_circuit_metrics
from .rate_limit import (
    RATE_LIMITS,
    RateLimiter,
    get_rate_limiter,
    rate_limiter_status,
)
from .util import _service_key, service_key

__all__ = [
    "BULKHEAD_LIMITS",
    "Bulkhead",
    "BulkheadPool",
    "CircuitBreaker",
    "CircuitOpenError",
    "POOL_ENRICH",
    "POOL_LOAD",
    "POOL_PUSH",
    "RATE_LIMITS",
    "RateLimiter",
    "_service_key",
    "bulkhead_status",
    "circuit_status",
    "get_bulkhead",
    "get_circuit",
    "reset_all_circuits",
    "ensure_circuits_loaded",
    "save_circuit_state",
    "load_circuit_state",
    "get_rate_limiter",
    "rate_limiter_status",
    "request_with_retries",
    "service_key",
    "write_circuit_metrics",
]
