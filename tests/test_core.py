"""Tests for socket_connection_pool.core.

We avoid real network I/O entirely. ``acquire`` on an empty pool would
normally call ``socket.create_connection``; we never exercise that path
with a real host. Instead we test pooling behaviour by directly
manipulating the idle set via ``release`` of sockets we construct with
``socket.socketpair``.

This is deliberate: the brief says tests must be deterministic and the
test container has no network. We test the pool's bookkeeping, not the
OS's TCP stack.
"""

import socket
import unittest

from socket_connection_pool.core import ConnectionPool, PooledSocket


class FakeClock:
    """A controllable monotonic clock for tests."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


def make_pooled(pool, key, raw_sock=None):
    """Build a PooledSocket that is 'checked out', bypassing the network."""
    if raw_sock is None:
        a, b = socket.socketpair()
        b.close()
        raw_sock = a
    return PooledSocket(raw_sock, key, pool, pool._clock())


class TestConstruction(unittest.TestCase):
    def test_rejects_negative_per_key(self):
        with self.assertRaises(ValueError):
            ConnectionPool(max_per_key=-1)

    def test_rejects_negative_total(self):
        with self.assertRaises(ValueError):
            ConnectionPool(max_total=-1)

    def test_rejects_negative_timeout(self):
        with self.assertRaises(ValueError):
            ConnectionPool(idle_timeout=-1)

    def test_defaults(self):
        p = ConnectionPool()
        self.assertEqual(p.stats(), {"idle_total": 0, "keys": 0})


class TestReleaseAndAcquire(unittest.TestCase):
    def test_release_then_acquire_returns_same_socket(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        ps = make_pooled(pool, ("h", 1))
        pool.release(ps)
        got = pool.acquire("h", 1)
        self.assertIs(got.sock, ps.sock)

    def test_acquire_on_empty_pool_opens_socket(self):
        # We can't test a real connect, but we can test that acquire on an
        # empty pool does NOT find an idle socket: it should raise because
        # create_connection can't reach a real host. We patch the pool's
        # _open to return a known socket instead.
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        sentinel = make_pooled(pool, ("h", 1))
        called = []

        def fake_open(key):
            called.append(key)
            return sentinel

        pool._open = fake_open
        got = pool.acquire("h", 1)
        self.assertIs(got, sentinel)
        self.assertEqual(called, [("h", 1)])

    def test_lru_order_within_key(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        first = make_pooled(pool, ("h", 1))
        pool.release(first)
        clock.advance(1)
        second = make_pooled(pool, ("h", 1))
        pool.release(second)
        # Acquire should return the most recently used (second).
        got = pool.acquire("h", 1)
        self.assertIs(got.sock, second.sock)
        got.release()
        # After release the MRU socket is still second, so we get it again.
        got = pool.acquire("h", 1)
        self.assertIs(got.sock, second.sock)

    def test_keys_are_isolated(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        a = make_pooled(pool, ("h", 1))
        b = make_pooled(pool, ("h", 2))
        pool.release(a)
        pool.release(b)
        got = pool.acquire("h", 2)
        self.assertIs(got.sock, b.sock)


class TestCapacity(unittest.TestCase):
    def test_per_key_cap_drops_excess(self):
        clock = FakeClock()
        pool = ConnectionPool(max_per_key=2, clock=clock)
        socks = [make_pooled(pool, ("h", 1)) for _ in range(3)]
        for s in socks:
            pool.release(s)
        # Only 2 should be idle; the oldest (first) should be closed.
        self.assertEqual(pool.stats()["idle_total"], 2)
        # The two retained should be the most recent.
        got = pool.acquire("h", 1)
        self.assertIs(got.sock, socks[2].sock)

    def test_global_cap_evicts_oldest_across_keys(self):
        clock = FakeClock()
        pool = ConnectionPool(max_per_key=10, max_total=2, clock=clock)
        a = make_pooled(pool, ("h", 1))
        pool.release(a)
        clock.advance(1)
        b = make_pooled(pool, ("h", 2))
        pool.release(b)
        clock.advance(1)
        c = make_pooled(pool, ("h", 3))
        pool.release(c)
        # Global cap is 2; the oldest (a) should have been evicted.
        self.assertEqual(pool.stats()["idle_total"], 2)
        self.assertNotIn(("h", 1), pool._idle)

    def test_max_total_zero_closes_on_release(self):
        clock = FakeClock()
        pool = ConnectionPool(max_total=0, clock=clock)
        ps = make_pooled(pool, ("h", 1))
        pool.release(ps)
        self.assertEqual(pool.stats()["idle_total"], 0)
        # The socket should be closed.
        with self.assertRaises(OSError):
            ps.sock.send(b"x")

    def test_idle_timeout_zero_closes_on_release(self):
        clock = FakeClock()
        pool = ConnectionPool(idle_timeout=0, clock=clock)
        ps = make_pooled(pool, ("h", 1))
        pool.release(ps)
        self.assertEqual(pool.stats()["idle_total"], 0)


class TestSweep(unittest.TestCase):
    def test_sweep_evicts_expired(self):
        clock = FakeClock()
        pool = ConnectionPool(idle_timeout=10, clock=clock)
        a = make_pooled(pool, ("h", 1))
        pool.release(a)
        clock.advance(5)
        b = make_pooled(pool, ("h", 1))
        pool.release(b)
        clock.advance(6)  # a is now 11s old, b is 6s old
        closed = pool.sweep()
        self.assertEqual(closed, 1)
        self.assertEqual(pool.stats()["idle_total"], 1)
        got = pool.acquire("h", 1)
        self.assertIs(got.sock, b.sock)

    def test_sweep_noop_when_nothing_expired(self):
        clock = FakeClock()
        pool = ConnectionPool(idle_timeout=10, clock=clock)
        ps = make_pooled(pool, ("h", 1))
        pool.release(ps)
        clock.advance(3)
        closed = pool.sweep()
        self.assertEqual(closed, 0)
        self.assertEqual(pool.stats()["idle_total"], 1)

    def test_sweep_noop_when_timeout_zero(self):
        clock = FakeClock()
        pool = ConnectionPool(idle_timeout=0, clock=clock)
        closed = pool.sweep()
        self.assertEqual(closed, 0)

    def test_sweep_removes_empty_buckets(self):
        clock = FakeClock()
        pool = ConnectionPool(idle_timeout=5, clock=clock)
        ps = make_pooled(pool, ("h", 1))
        pool.release(ps)
        clock.advance(10)
        pool.sweep()
        self.assertNotIn(("h", 1), pool._idle)


class TestDiscard(unittest.TestCase):
    def test_discard_does_not_return_to_pool(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        ps = make_pooled(pool, ("h", 1))
        ps.discard()
        self.assertEqual(pool.stats()["idle_total"], 0)
        # Socket should be closed.
        with self.assertRaises(OSError):
            ps.sock.send(b"x")

    def test_double_release_is_noop(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        ps = make_pooled(pool, ("h", 1))
        ps.release()
        before = pool.stats()["idle_total"]
        ps.release()  # second call must not double-count
        self.assertEqual(pool.stats()["idle_total"], before)

    def test_context_manager_releases_on_clean_exit(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        ps = make_pooled(pool, ("h", 1))
        with ps:
            pass
        self.assertEqual(pool.stats()["idle_total"], 1)

    def test_context_manager_discards_on_exception(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        ps = make_pooled(pool, ("h", 1))

        class Boom(Exception):
            pass

        with self.assertRaises(Boom):
            with ps:
                raise Boom()
        self.assertEqual(pool.stats()["idle_total"], 0)


class TestCloseAll(unittest.TestCase):
    def test_close_all_empties_pool(self):
        clock = FakeClock()
        pool = ConnectionPool(clock=clock)
        for i in range(3):
            pool.release(make_pooled(pool, ("h", i)))
        pool.close_all()
        self.assertEqual(pool.stats(), {"idle_total": 0, "keys": 0})


if __name__ == "__main__":
    unittest.main()
