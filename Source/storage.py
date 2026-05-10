"""
storage.py — Async SQLite wrapper for the telemetry server.

All public methods are coroutines and must be awaited.
The database is opened in WAL mode to reduce contention between
the concurrent read (REST queries) and write (sensor ingestion) coroutines.
"""

import time
import aiosqlite


class Storage:
    def __init__(self, db_path: str = "sensors.db"):
        self._path = db_path
        self._db: aiosqlite.Connection | None = None

    # ── Lifecycle ────────────────────────────────────────────────

    async def connect(self) -> None:
        """Open the database and create tables if they do not exist."""
        self._db = await aiosqlite.connect(self._path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA foreign_keys=ON")
        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS sensors (
                id            TEXT    PRIMARY KEY,
                location      TEXT    NOT NULL,
                sensor_type   TEXT    NOT NULL,
                interval_s    INTEGER NOT NULL DEFAULT 10,
                registered_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS readings (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                sensor_id  TEXT    NOT NULL REFERENCES sensors(id),
                timestamp  INTEGER NOT NULL,
                value      REAL    NOT NULL,
                unit       TEXT    NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_readings_sensor_time
                ON readings (sensor_id, timestamp);
        """)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    # ── Sensors ──────────────────────────────────────────────────

    async def get_sensors(self) -> list[dict]:
        async with self._db.execute("SELECT * FROM sensors ORDER BY registered_at") as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]

    async def get_sensor(self, sensor_id: str) -> dict | None:
        async with self._db.execute(
            "SELECT * FROM sensors WHERE id = ?", (sensor_id,)
        ) as cur:
            row = await cur.fetchone()
        return dict(row) if row else None

    async def register_sensor(self, sensor_id: str, location: str,
                               sensor_type: str, interval_s: int = 10) -> dict:
        now = int(time.time() * 1000)
        await self._db.execute(
            "INSERT INTO sensors (id, location, sensor_type, interval_s, registered_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (sensor_id, location, sensor_type, interval_s, now)
        )
        await self._db.commit()
        return {
            "id": sensor_id,
            "location": location,
            "sensor_type": sensor_type,
            "interval_s": interval_s,
            "registered_at": now,
        }

    async def delete_sensor(self, sensor_id: str) -> bool:
        cur = await self._db.execute(
            "DELETE FROM sensors WHERE id = ?", (sensor_id,)
        )
        await self._db.commit()
        return cur.rowcount > 0

    async def sensor_exists(self, sensor_id: str) -> bool:
        async with self._db.execute(
            "SELECT 1 FROM sensors WHERE id = ?", (sensor_id,)
        ) as cur:
            return await cur.fetchone() is not None

    # ── Readings ─────────────────────────────────────────────────

    async def store_reading(self, sensor_id: str, timestamp: int,
                             value: float, unit: str) -> None:
        """
        Store a sensor reading.  If the sensor is not yet registered
        (e.g. the simulator connected before the REST POST), the row is
        silently dropped — the foreign-key constraint would reject it anyway.
        """
        try:
            await self._db.execute(
                "INSERT INTO readings (sensor_id, timestamp, value, unit) "
                "VALUES (?, ?, ?, ?)",
                (sensor_id, timestamp, value, unit)
            )
            await self._db.commit()
        except aiosqlite.IntegrityError:
            pass  # sensor not registered; drop the reading

    async def get_readings(self, sensor_id: str,
                            from_ts: int | None = None,
                            to_ts: int | None = None) -> list[dict]:
        query = "SELECT sensor_id, timestamp, value, unit FROM readings WHERE sensor_id = ?"
        params: list = [sensor_id]
        if from_ts is not None:
            query += " AND timestamp >= ?"
            params.append(from_ts)
        if to_ts is not None:
            query += " AND timestamp <= ?"
            params.append(to_ts)
        query += " ORDER BY timestamp"
        async with self._db.execute(query, params) as cur:
            rows = await cur.fetchall()
        return [dict(r) for r in rows]
