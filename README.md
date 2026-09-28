# Socket Connection Pool

A bounded, keep-alive TCP connection pool keyed by `(host, port)` with idle eviction. Standard library only.

## Usage

```python
import socket
from socket_connection_pool import ConnectionPool

# Start a trivial echo server so the example actually runs.
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(("127.0.0.1", 0))
server.listen(1)
host, port = server.getsockname()

pool = ConnectionPool(max_per_key=8, max_total=64, idle_timeout=60.0)

with pool.acquire(host, port) as ps:
    conn, _ = server.accept()
    ps.sock.sendall(b"PING\r\n")
    data = conn.recv(1024)
# On a clean exit the socket is returned to the pool.
# On an exception it is discarded.

# Evict sockets idle for more than idle_timeout seconds:
pool.sweep()

# Shut everything down:
pool.close_all()
server.close()
```

`ConnectionPool` is the main class. `PooledSocket` is what `acquire` returns; it wraps a plain `socket.socket` (accessible as `.sock`) and exposes `release()` and `discard()` plus context-manager semantics.

## Why this exists

Opening a TCP connection has real latency — DNS, SYN, ACK, TLS. When a workload makes many short requests to a small set of hosts, that overhead dominates. This pool keeps idle sockets alive so repeated requests to the same `(host, port)` reuse an established connection.

The trade-off: the pool holds file descriptors open. A bounded pool caps that cost. We bound in two dimensions — per-key and global — so a single chatty host cannot starve others.

## Edge cases you will hit

- **`idle_timeout=0` disables pooling.** Sockets are closed on release rather than retained. This is a deliberate interpretation: a zero timeout means "no idle retention," not "retain forever."

- **Overflow is rejected at release time, not acquire time.** If the pool is full when you release a socket, that socket is closed. We never block a caller that needs a connection.

- **Dead sockets are your problem.** The pool does not probe idle sockets. If the remote end has closed the connection while it sat idle, `acquire` will hand you a dead socket and your first `send`/`recv` will fail. Handle that by calling `discard()` and retrying.

- **The clock is injectable.** `ConnectionPool(clock=...)` takes a callable returning a float. In production use `time.monotonic` (the default). In tests, pass a fake.

## Running the tests

```
PYTHONPATH=src python -m unittest discover -s tests
```
