# IoT Telemetry Pipeline & Dashboard

NET322 · Assignment 3 — Source Code

---

## Overview

A greenhouse sensor monitoring system built with Python AsyncIO.  
Three sensors push Protobuf readings over TCP every few seconds; a single-threaded AsyncIO server stores them in SQLite, exposes a REST API with content negotiation, broadcasts a live WebSocket feed, and serves a simple HTML dashboard.

```
sensors (TCP·Protobuf) ──► server.py ──► SQLite
                                    ├──► REST API  (JSON / XML / YAML)
                                    └──► WebSocket (JSON)
```

---

## Repository Layout

```
Assignment 1.pdf
Assignment 2.pdf
Source/
├── server.py              Main server: TCP listener + REST API + WebSocket
├── sensor_simulator.py    Async sensor simulator (reads config/sensors.yaml)
├── storage.py             Async SQLite wrapper (aiosqlite)
├── broadcaster.py         WebSocket broadcaster (asyncio.Queue per client)
├── negotiation.py         HTTP content negotiation + serialisation helpers
├── proto/
│   ├── telemetry.proto    Protobuf schema (Reading + SensorInfo messages)
│   └── telemetry_pb2.py   Pre-generated Python stubs (do not edit)
├── config/
│   └── sensors.yaml       Sensor simulator configuration (4 sensors)
├── requirements.txt
└── README.md
```

---

## Setup

**Requirements:** Python 3.11 or later.

```bash
# 1. Clone and enter the repo
git clone <repo-url>
cd <repo-dir>/Source

# 2. (Recommended) create a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

The pre-generated `proto/telemetry_pb2.py` is included in the repo so no
protoc invocation is needed.  If you ever modify `telemetry.proto`, regenerate
the stubs with:

```bash
python -m grpc_tools.protoc -I proto --python_out=proto proto/telemetry.proto
```

---

## Running the System

Open **two terminals** inside the `Source/` directory.

### Terminal 1 — Start the server

```bash
python server.py
```

Default ports: TCP **9000** (sensor ingress), HTTP **8080** (REST + WebSocket).  
Override with flags:

```bash
python server.py --tcp-port 9001 --http-port 9090 --db my.db
```

You should see:

```
09:00:01  INFO     server  Database ready at sensors.db
09:00:01  INFO     server  TCP sensor listener on port 9000
09:00:01  INFO     server  HTTP/WebSocket server on http://0.0.0.0:8080
09:00:01  INFO     server  Dashboard: http://localhost:8080/
```

### Terminal 2 — Start the sensor simulator

```bash
python sensor_simulator.py
```

Use a custom config file:

```bash
python sensor_simulator.py --config config/sensors.yaml
```

The simulator reads `config/sensors.yaml`, spawns one async task per sensor,
and pushes Protobuf readings at the configured `report_interval_s`.

---

## Querying the REST API

All endpoints honour the `Accept` header.  
A `session_id` cookie is set automatically on the first request.

### List all registered sensors

```bash
# JSON (default)
curl http://localhost:8080/sensors

# XML
curl -H "Accept: application/xml" http://localhost:8080/sensors

# YAML
curl -H "Accept: application/x-yaml" http://localhost:8080/sensors
```

### Get a single sensor

```bash
curl http://localhost:8080/sensors/gh_a_temp_01
```

### Register a sensor manually

```bash
curl -X POST http://localhost:8080/sensors \
     -H "Content-Type: application/json" \
     -d '{"id":"manual_01","location":"Shed","sensor_type":"temperature","interval_s":30}'
```

### Delete a sensor

```bash
curl -X DELETE http://localhost:8080/sensors/manual_01
```

### Query historical readings

```bash
# All readings for a sensor
curl "http://localhost:8080/sensors/gh_a_temp_01/readings"

# With a time range (Unix milliseconds)
curl "http://localhost:8080/sensors/gh_a_temp_01/readings?from=1715100000000&to=1715200000000"

# As XML
curl -H "Accept: application/xml" \
     "http://localhost:8080/sensors/gh_b_hum_01/readings"
```

---

## Connecting to the WebSocket Feed

### Browser dashboard

Open **http://localhost:8080/** in any browser.  
The page connects automatically and plots all incoming readings in real time.

### Command-line (websocat)

```bash
# Install: cargo install websocat  OR  pip install websockets
websocat ws://localhost:8080/live

# Filter to a specific sensor by sending a JSON message
websocat ws://localhost:8080/live <<< '{"sensor_id":"gh_a_temp_01"}'
```

### Python client example

```python
import asyncio
import json
import websockets

async def watch():
    async with websockets.connect("ws://localhost:8080/live") as ws:
        # Optional: filter to one sensor
        await ws.send(json.dumps({"sensor_id": "gh_a_temp_01"}))
        async for message in ws:
            print(json.loads(message))

asyncio.run(watch())
```

Each message is a JSON object:

```json
{
  "sensor_id": "gh_a_temp_01",
  "timestamp": 1715200060000,
  "value": 24.71,
  "unit": "celsius"
}
```

---

## Configuration Reference (config/sensors.yaml)

```yaml
server:
  host: "127.0.0.1"    # server TCP host
  port: 9000           # server TCP port

sensors:
  - id: "gh_a_temp_01"          # unique ID (used in URLs and Protobuf frames)
    location: "Greenhouse A"    # human label
    sensor_type: "temperature"  # temperature | humidity | light | soil
    unit: "celsius"             # echoed in every Reading message
    report_interval_s: 5        # seconds between readings (integer, min 1)
    min_value: 15.0             # lower bound for random simulation
    max_value: 40.0             # upper bound
```

Add or remove entries under `sensors` to change the simulated fleet.

---

## Constraints Satisfied

| Requirement | Implementation |
|---|---|
| No threads / no multiprocessing | All I/O uses `asyncio` — `asyncio.start_server()`, `aiohttp`, `aiosqlite` |
| Sensor→server wire format: Protobuf | `telemetry_pb2.Reading`, length-prefixed over TCP (`struct.pack(">I", …)`) |
| Config format: YAML | `config/sensors.yaml` parsed with `PyYAML` |
| REST: JSON + XML (+ YAML) | `negotiation.py` inspects `Accept` header; falls back to JSON |
| WebSocket: JSON | `ws.send_json(reading)` in `websocket_handler` |
| Cookies for session tracking | `session_id` cookie set on first REST request via `secrets.token_hex` |
| Slow consumer safety | Per-client `asyncio.Queue(maxsize=100)` + `put_nowait()` drop policy |
| Sensor reconnect | Exponential back-off in `sensor_simulator.py` (`run_sensor` loop) |

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ModuleNotFoundError: telemetry_pb2` | Run from inside `Source/` directory |
| `ConnectionRefusedError` in simulator | Start `server.py` first |
| Readings not stored | Sensor must be registered (auto-register on TCP connect via `SensorInfo`) |
| Port already in use | Pass `--tcp-port` / `--http-port` to choose different ports |
