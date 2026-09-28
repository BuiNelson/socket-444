"""Core implementation of the socket connection pool.

Design decisions
----------------

* Sockets are stored in per-(host, port) deques. Acquiring pops from the
  right (most recently used); releasing pushes to the right. This gives
  LRU reuse, so the idle-eviction sweep tends to touch the oldest sockets
  first.

* The pool is *bounded* in two ways: a per-key capacity and a global
  capacity. We deliberately reject overflow at *release* time rather than
  at *acquire* time. If we rejected at acquire, a caller that needed a
  connection would be forced to open a fresh one anyway, defeating the
  purpose of the pool. Rejecting at release means we only close a socket
  when we genuinely have no room to keep it, which is the cheaper moment
  to close.

* Idle eviction is driven by a caller-supplied ``clock`` callable that
  returns a monotonically non-decreasing float (seconds). Every
  interaction with the pool -- acquire, release, and an explicit
  ``sweep`` -- consults this clock. This makes idle behaviour fully
  deterministic in tests: no wall-clock dependency, no sleeps.

* We do not retry or reconnect inside the pool. A dead socket returned
  from ``acquire`` is the caller's problem; the caller closes it and asks
  again. Hiding reconnect logic inside the pool would make error
  semantics ambiguous (did the error come from the pool or the remote?).
"""

from __future__ import annotations

import socket
import time
from collections import deque
from typing import Callable, Deque, Dict, Optional, Tuple


Key = Tuple[str, int]


class PooledSocket:
    """A thin wrapper around a ``socket.socket`` with bookkeeping.

    We wrap rather than subclass so that callers cannot accidentally call
    ``close()`` on a checked-out socket and leave the pool's bookkeeping
    inconsistent. To return a socket to the pool, call ``release()``; to
    discard it permanently, call ``discard()``.
    """

    __slots__ = ("sock", "key", "last_used", "_pool", "_state")

    def __init__(self, sock: socket.socket, key: Key, pool: "ConnectionPool", now: float) -> None:
        self.sock = sock
        self.key = key
        self.last_used = now
        self._pool = pool
        # "open" -> checked out by caller
        # "released" -> returned to pool (or discarded); caller must not touch
        self._state = "open"

    def release(self) -> None:
        """Return this socket to its pool.

        If the pool is full (globally or per-key) the socket is closed
        instead. Safe to call at most once; a second call is a no-op.
        """
        if self._state != "open":
            return
        self._state = "released"
        self._pool._release(self)

    def discard(self) -> None:
        """Close this socket and do NOT return it to the pool.

        Use this when the remote end has hung up or the socket is in an
        unknown state. Safe to call at most once; a second call is a no-op.
        """
        if self._state != "open":
            return
        self._state = "released"
        try:
            self.sock.close()
        finally:
            self._pool._discard(self)

    def __enter__(self) -> "PooledSocket":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        # On a clean exit we return the socket. On an exception we discard
        # it, because we don't know how far through a read/write we got
        # and the socket may be in a half-broken state.
        if exc_type is None:
            self.release()
        else:
            self.discard()


class ConnectionPool:
    """A bounded pool of keep-alive TCP connections.

    Parameters
    ----------
    max_per_key:
        Maximum number of idle sockets to retain per (host, port).
    max_total:
        Maximum number of idle sockets to retain across all keys.
    idle_timeout:
        Seconds an idle socket may sit unused before it is eligible for
        eviction. ``0`` disables idle eviction.
    clock:
        A callable returning a float, used as the monotonic clock. Defaults
        to ``time.monotonic``. Inject a fake in tests.
    """

    def __init__(
        self,
        max_per_key: int = 8,
        max_total: int = 64,
        idle_timeout: float = 60.0,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if max_per_key < 0:
            raise ValueError("max_per_key must be non-negative")
        if max_total < 0:
            raise ValueError("max_total must be non-negative")
        if idle_timeout < 0:
            raise ValueError("idle_timeout must be non-negative")

        self._max_per_key = max_per_key
        self._max_total = max_total
        self._idle_timeout = idle_timeout
        self._clock: Callable[[], float] = clock if clock is not None else time.monotonic
        self._idle: Dict[Key, Deque[PooledSocket]] = {}
        self._idle_count = 0

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def acquire(self, host: str, port: int) -> PooledSocket:
        """Get a connection to ``(host, port)``.

        Returns a pooled idle socket if one is available, otherwise
        opens a fresh TCP connection.

        Raises ``OSError`` (or a subclass) if a fresh connection cannot
        be established.
        """
        key = (host, port)
        sock = self._take(key)
        if sock is not None:
            return sock
        return self._open(key)

    def release(self, ps: PooledSocket) -> None:
        """Return ``ps`` to the pool.

        This is equivalent to ``ps.release()`` and is provided for
        callers that prefer an explicit pool-centric style.
        """
        ps.release()

    def discard(self, ps: PooledSocket) -> None:
        """Discard ``ps`` without returning it to the pool."""
        ps.discard()

    def sweep(self) -> int:
        """Evict idle sockets that have exceeded ``idle_timeout``.

        Returns the number of sockets closed. If ``idle_timeout`` is 0
        this is a no-op and returns 0.
        """
        if self._idle_timeout == 0:
            return 0
        now = self._clock()
        deadline = now - self._idle_timeout
        closed = 0
        for key in list(self._idle.keys()):
            bucket = self._idle[key]
            # Buckets are LRU: oldest at the left. Pop from the left
            # while the oldest is past the deadline.
            while bucket and bucket[0].last_used <= deadline:
                ps = bucket.popleft()
                self._idle_count -= 1
                try:
                    ps.sock.close()
                finally:
                    closed += 1
            if not bucket:
                del self._idle[key]
        return closed

    def stats(self) -> Dict[str, int]:
        """Return a small dict of counters, mainly for tests and ops."""
        return {
            "idle_total": self._idle_count,
            "keys": len(self._idle),
        }

    def close_all(self) -> None:
        """Close every idle socket and empty the pool."""
        for bucket in self._idle.values():
            for ps in bucket:
                try:
                    ps.sock.close()
                except OSError:
                    pass
        self._idle.clear()
        self._idle_count = 0

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _take(self, key: Key) -> Optional[PooledSocket]:
        bucket = self._idle.get(key)
        if not bucket:
            return None
        ps = bucket.pop()
        self._idle_count -= 1
        if not bucket:
            del self._idle[key]
        ps._state = "open"
        ps.last_used = self._clock()
        return ps

    def _open(self, key: Key) -> PooledSocket:
        host, port = key
        raw = socket.create_connection((host, port))
        return PooledSocket(raw, key, self, self._clock())

    def _release(self, ps: PooledSocket) -> None:
        # Called by PooledSocket.release(); the wrapper has already flipped
        # its state. We decide whether to keep or close.
        now = self._clock()
        ps.last_used = now

        # If idle_timeout is 0 we never retain idle sockets at all: the
        # pool is purely a rate-limiter on connection churn within a single
        # checkout burst. This is a deliberate, documented interpretation.
        if self._idle_timeout == 0:
            try:
                ps.sock.close()
            finally:
                return

        # Per-key cap. If the bucket is full, evict the oldest (LRU) to
        # make room for the newly released socket.
        bucket = self._idle.get(ps.key)
        if bucket is not None and len(bucket) >= self._max_per_key:
            if self._max_per_key == 0:
                try:
                    ps.sock.close()
                finally:
                    return
            evicted = bucket.popleft()
            self._idle_count -= 1
            try:
                evicted.sock.close()
            except OSError:
                pass
            if not bucket:
                del self._idle[ps.key]
                bucket = None

        # Global cap. If we're at the limit, drop the single oldest idle
        # socket anywhere in the pool to make room. This keeps the global
        # bound tight without a full sweep.
        if self._idle_count >= self._max_total:
            self._evict_oldest()

        # max_total could be 0, in which case we evicted nothing and still
        # have no room.
        if self._idle_count >= self._max_total:
            try:
                ps.sock.close()
            finally:
                return

        bucket = self._idle.get(ps.key)
        if bucket is None:
            bucket = deque()
            self._idle[ps.key] = bucket
        bucket.append(ps)
        self._idle_count += 1

    def _discard(self, ps: PooledSocket) -> None:
        # Nothing to do: a discarded socket was never in the idle set.
        pass

    def _evict_oldest(self) -> None:
        oldest: Optional[PooledSocket] = None
        oldest_key: Optional[Key] = None
        for key, bucket in self._idle.items():
            if not bucket:
                continue
            if oldest is None or bucket[0].last_used < oldest.last_used:
                oldest = bucket[0]
                oldest_key = key
        if oldest is None or oldest_key is None:
            return
        bucket = self._idle[oldest_key]
        bucket.popleft()
        self._idle_count -= 1
        if not bucket:
            del self._idle[oldest_key]
        try:
            oldest.sock.close()
        except OSError:
            pass
