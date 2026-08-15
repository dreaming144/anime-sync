"""Unit tests for circuit breaker state machine."""
from __future__ import annotations

import time
import unittest

from anime_sync.http.circuit import (
    CircuitBreaker,
    CircuitOpenError,
    circuit_status,
    get_circuit,
    reset_all_circuits,
)


class TestCircuitStateMachine(unittest.TestCase):
    def setUp(self):
        self.cb = CircuitBreaker(
            "test",
            failure_threshold=3,
            recovery_timeout=0.15,
            half_open_max=2,
            success_threshold=2,
            max_recovery_timeout=1.0,
            recovery_backoff=2.0,
        )

    def test_starts_closed(self):
        self.assertEqual(self.cb.state, CircuitBreaker.CLOSED)
        self.assertTrue(self.cb.allow())

    def test_trips_after_threshold(self):
        for _ in range(2):
            self.assertTrue(self.cb.allow())
            self.cb.record_failure(error="boom")
            self.assertEqual(self.cb.state, CircuitBreaker.CLOSED)
        self.assertTrue(self.cb.allow())
        self.cb.record_failure(error="boom")
        self.assertEqual(self.cb.state, CircuitBreaker.OPEN)
        self.assertEqual(self.cb.open_count, 1)

    def test_short_circuits_while_open(self):
        for _ in range(3):
            self.cb.allow()
            self.cb.record_failure()
        self.assertEqual(self.cb.state, CircuitBreaker.OPEN)
        self.assertFalse(self.cb.allow())
        self.assertFalse(self.cb.allow())
        self.assertGreaterEqual(self.cb.short_circuits, 2)

    def test_half_open_after_timeout(self):
        for _ in range(3):
            self.cb.allow()
            self.cb.record_failure()
        self.assertEqual(self.cb.state, CircuitBreaker.OPEN)
        time.sleep(0.16)
        self.assertTrue(self.cb.allow())
        self.assertEqual(self.cb.state, CircuitBreaker.HALF_OPEN)

    def test_half_open_needs_success_threshold(self):
        for _ in range(3):
            self.cb.allow()
            self.cb.record_failure()
        time.sleep(0.16)
        self.assertTrue(self.cb.allow())  # trial 1
        self.cb.record_success()
        self.assertEqual(self.cb.state, CircuitBreaker.HALF_OPEN)  # need 2
        self.assertTrue(self.cb.allow())  # trial 2
        self.cb.record_success()
        self.assertEqual(self.cb.state, CircuitBreaker.CLOSED)

    def test_half_open_failure_reopens(self):
        for _ in range(3):
            self.cb.allow()
            self.cb.record_failure()
        time.sleep(0.16)
        self.assertTrue(self.cb.allow())
        self.cb.record_failure(error="still down")
        self.assertEqual(self.cb.state, CircuitBreaker.OPEN)
        self.assertEqual(self.cb.open_count, 2)
        # recovery timeout should have grown
        self.assertGreater(self.cb.recovery_timeout, self.cb.base_recovery_timeout)

    def test_success_resets_consecutive_failures(self):
        self.cb.allow()
        self.cb.record_failure()
        self.cb.allow()
        self.cb.record_failure()
        self.assertEqual(self.cb.consecutive_failures, 2)
        self.cb.allow()
        self.cb.record_success()
        self.assertEqual(self.cb.consecutive_failures, 0)
        self.assertEqual(self.cb.state, CircuitBreaker.CLOSED)

    def test_metrics_snapshot(self):
        self.cb.allow()
        self.cb.record_success()
        m = self.cb.metrics()
        self.assertEqual(m["state"], "closed")
        self.assertEqual(m["successes"], 1)
        self.assertIn("open_remaining_s", m)

    def test_reset(self):
        for _ in range(3):
            self.cb.allow()
            self.cb.record_failure()
        self.cb.reset()
        self.assertEqual(self.cb.state, CircuitBreaker.CLOSED)
        self.assertEqual(self.cb.consecutive_failures, 0)
        self.assertEqual(self.cb.recovery_timeout, self.cb.base_recovery_timeout)


class TestCircuitRegistry(unittest.TestCase):
    def setUp(self):
        reset_all_circuits()

    def tearDown(self):
        reset_all_circuits()

    def test_get_circuit_per_service(self):
        a = get_circuit("https://graphql.anilist.co/query")
        b = get_circuit("https://graphql.anilist.co/other")
        c = get_circuit("https://api.simkl.com/sync")
        self.assertIs(a, b)
        self.assertIsNot(a, c)

    def test_service_defaults_applied(self):
        mal = get_circuit("https://api.myanimelist.net/v2/anime")
        self.assertEqual(mal.failure_threshold, 4)

    def test_circuit_status(self):
        get_circuit("https://kitsu.io/api/edge")
        st = circuit_status()
        self.assertTrue(any("kitsu" in k for k in st))

    def test_circuit_open_error_carries_circuit(self):
        cb = CircuitBreaker("x", failure_threshold=1, recovery_timeout=30)
        cb.allow()
        cb.record_failure()
        err = CircuitOpenError("open", circuit=cb)
        self.assertIs(err.circuit, cb)
        self.assertGreaterEqual(err.remaining_s, 0)



class TestDistributedCircuit(unittest.TestCase):
    def setUp(self):
        reset_all_circuits()
        self.path = "test_circuit_state.json"
        import os
        if os.path.exists(self.path):
            os.unlink(self.path)

    def tearDown(self):
        reset_all_circuits()
        import os
        if os.path.exists(self.path):
            os.unlink(self.path)

    def test_persist_open_across_processes(self):
        from anime_sync.http.circuit import (
            CircuitBreaker,
            _circuit_breakers,
            save_circuit_state,
            load_circuit_state,
            reset_all_circuits,
        )
        cb = CircuitBreaker("dist-svc", failure_threshold=2, recovery_timeout=30.0)
        _circuit_breakers["dist-svc"] = cb
        cb.allow()
        cb.record_failure(error="e1")
        cb.allow()
        cb.record_failure(error="e2")
        self.assertEqual(cb.state, CircuitBreaker.OPEN)
        save_circuit_state(self.path)

        # Simulate new process
        reset_all_circuits()
        self.assertEqual(len(_circuit_breakers), 0)
        load_circuit_state(self.path)
        self.assertIn("dist-svc", _circuit_breakers)
        restored = _circuit_breakers["dist-svc"]
        self.assertEqual(restored.state, CircuitBreaker.OPEN)
        self.assertFalse(restored.allow())  # still short-circuits
        self.assertIn("e2", restored.last_error)

    def test_expired_open_becomes_half_open(self):
        from anime_sync.http.circuit import (
            CircuitBreaker,
            _circuit_breakers,
            save_circuit_state,
            load_circuit_state,
            reset_all_circuits,
        )
        cb = CircuitBreaker("old-svc", failure_threshold=1, recovery_timeout=0.05)
        _circuit_breakers["old-svc"] = cb
        cb.allow()
        cb.record_failure(error="down")
        self.assertEqual(cb.state, CircuitBreaker.OPEN)
        # Backdate opened_at so cooldown is already over
        cb.opened_at = time.time() - 10
        save_circuit_state(self.path)
        reset_all_circuits()
        load_circuit_state(self.path)
        restored = _circuit_breakers["old-svc"]
        self.assertEqual(restored.state, CircuitBreaker.HALF_OPEN)

    def test_closed_restores_as_closed(self):
        from anime_sync.http.circuit import (
            CircuitBreaker,
            _circuit_breakers,
            save_circuit_state,
            load_circuit_state,
            reset_all_circuits,
        )
        cb = CircuitBreaker("ok-svc", failure_threshold=5)
        _circuit_breakers["ok-svc"] = cb
        cb.allow()
        cb.record_success()
        save_circuit_state(self.path)
        reset_all_circuits()
        load_circuit_state(self.path)
        self.assertEqual(_circuit_breakers["ok-svc"].state, CircuitBreaker.CLOSED)


if __name__ == "__main__":
    unittest.main()
