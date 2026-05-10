"""
sensor_simulator.py — IoT Sensor Simulator

Reads config/sensors.yaml and spawns one async task per sensor.
Each task:
  1. Connects to the server over TCP.
  2. Registers the sensor via POST /sensors (if not already registered).
  3. Loops: generates a plausible random reading, encodes it as a
     length-prefixed Protobuf frame, and sends it over TCP.
  4. Sleeps for report_interval_s, then repeats.
  5. On disconnect, waits briefly and reconnects automatically.

Usage:
    python sensor_simulator.py [--config config/sensors.yaml]

All network code uses asyncio — no threads, no multiprocessing.
"""

import asyncio
import argparse
import logging
import os
import random
import struct
import sys
import time

import yaml

sys.path.insert(0, "proto")     # import generated protobuf stubs
import telemetry_pb2            # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(name)-24s %(message)s",
    datefmt="%H:%M:%S",
)


def _make_reading(sensor: dict) -> telemetry_pb2.Reading:
    """Generate a plausible random reading for a sensor config entry."""
    reading = telemetry_pb2.Reading()
    reading.sensor_id = sensor["id"]
    reading.timestamp = int(time.time() * 1000)
    reading.unit      = sensor["unit"]

    lo, hi = sensor.get("min_value", 0.0), sensor.get("max_value", 100.0)

    # Add a small random walk so readings look like real sensor data
    # rather than pure uniform noise.
    mid   = (lo + hi) / 2
    sigma = (hi - lo) / 6
    reading.value = max(lo, min(hi, random.gauss(mid, sigma)))
    return reading


def _encode(msg) -> bytes:
    """Prefix a serialised protobuf message with a 4-byte big-endian length."""
    raw = msg.SerializeToString()
    return struct.pack(">I", len(raw)) + raw


async def run_sensor(sensor: dict, host: str, port: int) -> None:
    """
    Coroutine for a single sensor.  Reconnects automatically on any error.
    """
    log = logging.getLogger(sensor["id"])
    retry_delay = 2  # seconds; doubles on repeated failures, capped at 30 s

    while True:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            log.info("Connected to %s:%d", host, port)
            retry_delay = 2  # reset back-off on successful connect

            # Send an initial SensorInfo frame so the server can auto-register
            info = telemetry_pb2.SensorInfo()
            info.id          = sensor["id"]
            info.location    = sensor["location"]
            info.sensor_type = sensor["sensor_type"]
            info.interval_s  = int(sensor.get("report_interval_s", 10))
            writer.write(_encode(info))
            await writer.drain()

            interval = float(sensor.get("report_interval_s", 10))

            while True:
                reading = _make_reading(sensor)
                writer.write(_encode(reading))
                await writer.drain()
                log.info("Sent  value=%.3f %s", reading.value, reading.unit)
                await asyncio.sleep(interval)

        except (ConnectionRefusedError, OSError) as exc:
            log.warning("Connection failed (%s) — retrying in %ds", exc, retry_delay)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            log.warning("Connection lost — retrying in %ds", retry_delay)
        except asyncio.CancelledError:
            log.info("Sensor task cancelled")
            return
        except Exception as exc:
            log.exception("Unexpected error: %s — retrying in %ds", exc, retry_delay)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, 30)


async def main(config_path: str) -> None:
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    host = config["server"]["host"]
    port = config["server"]["port"]
    sensors = config["sensors"]

    log = logging.getLogger("simulator")
    log.info("Starting %d sensor(s) → %s:%d", len(sensors), host, port)

    tasks = [
        asyncio.create_task(run_sensor(s, host, port), name=s["id"])
        for s in sensors
    ]

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IoT Sensor Simulator")
    parser.add_argument(
        "--config", type=str,
        default=os.path.join(os.path.dirname(__file__), "config", "sensors.yaml")
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.config))
    except KeyboardInterrupt:
        logging.getLogger("simulator").info("Interrupted — bye")
