"""Socket Connection Pool.

A bounded, keep-alive TCP connection pool keyed by (host, port) with
idle eviction.
"""

from .core import ConnectionPool, PooledSocket

__all__ = ["ConnectionPool", "PooledSocket"]
