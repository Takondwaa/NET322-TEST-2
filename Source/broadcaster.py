"""
broadcaster.py — WebSocket live-feed broadcaster.

A single Broadcaster instance is shared across the entire server.
When a sensor reading arrives, handle_sensor_connection() calls
publish(), which drops the reading into every registered client queue.

Each WebSocket client has its own asyncio.Queue(maxsize=100).
If a client falls behind and its queue is full, put_nowait() raises
QueueFull and the reading is silently dropped for that client only —
the ingestion path and all other clients are unaffected.
"""

import asyncio
import logging

log = logging.getLogger(__name__)


class Broadcaster:
    def __init__(self) -> None:
        self._clients: set[asyncio.Queue] = set()

    def register(self) -> asyncio.Queue:
        """Create and register a new client queue. Returns the queue."""
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._clients.add(q)
        log.debug("WebSocket client registered  (total=%d)", len(self._clients))
        return q

    def unregister(self, q: asyncio.Queue) -> None:
        """Remove a client queue (called when the WebSocket connection closes)."""
        self._clients.discard(q)
        log.debug("WebSocket client unregistered (total=%d)", len(self._clients))

    def publish(self, reading: dict) -> None:
        """
        Push a reading to every registered client queue.
        Never awaits — safe to call from any coroutine without yielding.
        """
        drop_count = 0
        for q in self._clients:
            try:
                q.put_nowait(reading)
            except asyncio.QueueFull:
                drop_count += 1
        if drop_count:
            log.warning("Dropped reading for %d slow WebSocket client(s)", drop_count)

    @property
    def client_count(self) -> int:
        return len(self._clients)
