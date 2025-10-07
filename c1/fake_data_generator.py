#!/usr/bin/env python3
"""
IoT Fake Data Generator
- Sensor kinds: env, vibration, power, gps (or mixed)
- Output: JSONL (default) or CSV
- Optional MQTT publish (requires paho-mqtt)
- Occasional anomalies: spike/dropout

To understands and benchmark the process between a SQL database vs a noSQL database in 
read
write

"""

import argparse
import csv
import json
import math
import os
import random
import signal
import sys
import time
import uuid
import psycopg2
from psycopg2.extras import execute_values
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

# MQTT optional
try:
    import paho.mqtt.client as mqtt  # type: ignore
    HAS_MQTT = True
    print('running MQTT')
except Exception:
    HAS_MQTT = False
    mqtt = None  # type: ignore
    print('not runnin MQTT')

STOP = False
def _sig_handler(signum, frame):
    global STOP
    STOP = True

signal.signal(signal.SIGINT, _sig_handler)
signal.signal(signal.SIGTERM, _sig_handler)

KIND_CHOICES = ["env", "vibration", "power", "gps"]

@dataclass
class Sensor:
    id: str
    kind: str
    lat: float
    lon: float
    base_temp: float
    base_hum: float
    base_press: float
    battery_start: float

def make_sensors(n: int, kind: str, seed: Optional[int]) -> List[Sensor]:
    rng = random.Random(seed)
    sensors: List[Sensor] = []
    for _ in range(n):
        sensors.append(Sensor(
            id=str(uuid.uuid4()),
            kind=(kind if kind != "mixed" else rng.choice(KIND_CHOICES)),
            lat=rng.uniform(-60, 60),
            lon=rng.uniform(-150, 150),
            base_temp=rng.uniform(15, 30),
            base_hum=rng.uniform(30, 70),
            base_press=rng.uniform(980, 1030),
            battery_start=rng.uniform(60, 100),
        ))
    return sensors

def env_reading(s: Sensor, t: float, rng: random.Random) -> Dict:
    temp = s.base_temp + 6*math.sin((t/86400.0)*2*math.pi) + rng.gauss(0, 0.3)
    hum = s.base_hum + 10*math.sin((t/43200.0)*2*math.pi + 1.1) + rng.gauss(0, 1.2)
    hum = min(100.0, max(0.0, hum))
    press = s.base_press + 2*math.sin((t/3600.0)*2*math.pi) + rng.gauss(0, 0.5)
    return {"temperature_c": round(temp, 2), "humidity_pct": round(hum, 2), "pressure_hpa": round(press, 2)}

def vibration_reading(s: Sensor, t: float, rng: random.Random) -> Dict:
    base = 0.02 + 0.01*math.sin((t/5.0)*2*math.pi) + abs(rng.gauss(0,0.005))
    return {"rms_g": round(base,4), "peak_g": round(base*3 + abs(rng.gauss(0, 0.02)),4), "kurtosis": round(3 + abs(rng.gauss(0, 0.3)),3)}

def power_reading(s: Sensor, t: float, rng: random.Random) -> Dict:
    watts = 150 + 50*math.sin((t/60.0)*2*math.pi) + rng.gauss(0, 10)
    volts = 230 + rng.gauss(0, 1.5)
    amps = max(0.0, watts / max(1.0, volts) + rng.gauss(0, 0.05))
    pf = min(1.0, max(0.7, 0.95 + rng.gauss(0, 0.02)))
    return {"watts": round(watts,1), "volts": round(volts,1), "amps": round(amps,3), "power_factor": round(pf,3)}

def gps_reading(s: Sensor, t: float, rng: random.Random) -> Dict:
    s.lat += rng.gauss(0, 0.0005)
    s.lon += rng.gauss(0, 0.0005)
    speed = abs(rng.gauss(2.0, 0.7))
    return {"lat": round(s.lat,6), "lon": round(s.lon,6), "speed_m_s": round(speed,2)}

def battery_pct(start: float, t0: float, now: float) -> float:
    # ~0.5% per hour + tiny ripple
    hours = max(0.0, (now - t0) / 3600.0)
    val = start - 0.5*hours + 0.2*math.sin(now/300.0)
    return max(0.0, min(100.0, val))

def maybe_anomaly(rng: random.Random, payload: Dict) -> None:
    roll = rng.random()
    if roll < 0.005:         # dropout
        payload["status"] = "dropout"
    elif roll < 0.015:       # spike
        for k, v in list(payload.items()):
            if isinstance(v, (int, float)):
                payload[k] = round(v * (2.0 + rng.random()*2.0), 3)
        payload["status"] = "spike"

def build_record(s: Sensor, now: float, t0: float, rng: random.Random) -> Dict:
    if s.kind == "env":
        data = env_reading(s, now, rng)
    elif s.kind == "vibration":
        data = vibration_reading(s, now, rng)
    elif s.kind == "power":
        data = power_reading(s, now, rng)
    elif s.kind == "gps":
        data = gps_reading(s, now, rng)
    else:
        data = {"note": "unknown"}

    # small chance of anomaly
    maybe_anomaly(random.Random(hash((s.id, int(now)))), data)

    return {
        "ts": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
        "sensor_id": s.id,
        "kind": s.kind,
        "location": {"lat": round(s.lat,6), "lon": round(s.lon,6)},
        "battery_pct": round(battery_pct(s.battery_start, t0, now), 2),
        "data": data,
    }

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="IoT Fake Data Generator")
    p.add_argument("--sensors", type=int, default=5, help="number of sensors")
    p.add_argument("--kind", choices=["mixed","env","vibration","power","gps"], default="mixed", help="sensor type(s)")
    p.add_argument("--rate", type=float, default=2.0, help="messages per second per sensor")
    p.add_argument("--duration", type=float, default=0, help="seconds to run (0 = forever)")
    p.add_argument("--format", choices=["jsonl","csv"], default="jsonl", help="output format")
    p.add_argument("--out", default="-", help="'-' for stdout or a file path")
    p.add_argument("--seed", type=int, default=None, help="random seed")
    # MQTT
    p.add_argument("--mqtt-broker", default=None, help="host:port (optional)")
    p.add_argument("--mqtt-topic", default="iot/sensors", help="base topic")
    p.add_argument("--mqtt-qos", type=int, choices=[0,1,2], default=0)
    p.add_argument("--pg-dsn", default=None,
               help="Postgres DSN, e.g. postgresql://postgres:pw@localhost:5432/postgres")
    p.add_argument("--pg-batch", type=int, default=500,
                help="Batch size for inserts")
    p.add_argument("--pg-create-table", action="store_true",
                help="Create iot_readings table if missing")
    p.add_argument("--pg-mode", choices=["batch","immediate","queue"], default="batch",
                help="Insert mode: 'batch', 'immediate' (commit per row), or 'queue' (writer thread)")
    p.add_argument("--pg-queue-flush-ms", type=int, default=0,
                help="In 'queue' mode, flush delay in ms (0 = commit each row)")

    return p.parse_args()



def open_outputs(path: str, fmt: str):
    if path == "-" or path.lower() == "stdout":
        return sys.stdout, None
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if fmt == "csv":
        f = open(path, "w", newline="", encoding="utf-8")
        writer = csv.writer(f)
        writer.writerow(["ts","sensor_id","kind","lat","lon","battery_pct","data"])
        return f, writer
    f = open(path, "w", encoding="utf-8")
    return f, None

def mqtt_client_or_none(broker: Optional[str]):
    if not broker:
        return None
    if not HAS_MQTT:
        print("[warn] paho-mqtt not installed; MQTT disabled", file=sys.stderr)
        return None
    host, port = (broker.split(":", 1) + ["1883"])[:2]
    client = mqtt.Client()
    client.connect(host, int(port))
    client.loop_start()
    return client

def write_record(handle, writer, fmt: str, rec: Dict):
    if fmt == "csv":
        row = [
            rec["ts"], rec["sensor_id"], rec["kind"],
            rec["location"]["lat"], rec["location"]["lon"],
            rec["battery_pct"], json.dumps(rec["data"], separators=(",",":"))
        ]
        writer.writerow(row)  # type: ignore
    else:
        handle.write(json.dumps(rec, separators=(",", ":")) + "\n")

DDL = """
CREATE TABLE IF NOT EXISTS iot_readings (
  ts           timestamptz NOT NULL,
  sensor_id    uuid        NOT NULL,
  kind         text        NOT NULL,
  location     jsonb       NOT NULL,
  battery_pct  real        NOT NULL,
  data         jsonb       NOT NULL
);
CREATE INDEX IF NOT EXISTS iot_readings_sensor_ts_idx ON iot_readings (sensor_id, ts DESC);
CREATE INDEX IF NOT EXISTS iot_readings_ts_brin ON iot_readings USING brin (ts);
CREATE INDEX IF NOT EXISTS iot_readings_data_gin ON iot_readings USING gin (data);

CREATE TABLE IF NOT EXISTS iot_readings_fastwrite (
  ts           timestamptz NOT NULL,
  sensor_id    uuid        NOT NULL,
  kind         text        NOT NULL,
  location     jsonb       NOT NULL,
  battery_pct  real        NOT NULL,
  data         jsonb       NOT NULL
);
"""

def pg_connect(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn

def pg_create_table(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

def pg_flush(conn, buffer):
    if not buffer:
        return 0
    rows = [
        (
            rec["ts"],
            rec["sensor_id"],
            rec["kind"],
            json.dumps(rec["location"]),
            rec["battery_pct"],
            json.dumps(rec["data"]),
        )
        for rec in buffer
    ]
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO iot_readings
               (ts, sensor_id, kind, location, battery_pct, data)
               VALUES %s""",
            rows,
            page_size=min(1000, len(rows)),
        )
    conn.commit()
    buffer.clear()
    return len(rows)

def main():
    args = parse_args()
    rng = random.Random(args.seed)
    sensors = make_sensors(args.sensors, args.kind, args.seed)
    out_handle, csv_writer = open_outputs(args.out, args.format)
    mqttc = mqtt_client_or_none(args.mqtt_broker)

    # NEW: set up Postgres (optional)
    pg_conn = None
    pg_buf = []
    if args.pg_dsn:
        try:
            pg_conn = pg_connect(args.pg_dsn)
            if args.pg_create_table:
                pg_create_table(pg_conn)
            print("[pg] connected", file=sys.stderr)
        except Exception as e:
            print(f"[pg] disabled: {e}", file=sys.stderr)
            pg_conn = None

    start = time.time()
    period = 1.0 / max(0.0001, args.rate)

    try:
        while not STOP:
            now = time.time()
            if args.duration and (now - start) >= args.duration:
                break
            for s in sensors:
                rec = build_record(s, now, start, rng)
                write_record(out_handle, csv_writer, args.format, rec)

                if mqttc is not None:
                    topic = f"{args.mqtt_topic}/{s.kind}/{s.id}"
                    mqttc.publish(topic, json.dumps(rec), qos=args.mqtt_qos, retain=False)

                # NEW: buffer for Postgres and flush in batches
                if pg_conn is not None:
                    try:
                        pg_buf.append(rec)
                        if len(pg_buf) >= args.pg_batch:
                            pg_flush(pg_conn, pg_buf)
                    except Exception as e:
                        print(f"[pg] flush error: {e}", file=sys.stderr)

                time.sleep(period)  # per-sensor rate
    finally:
        # NEW: final flush + close PG
        if pg_conn is not None:
            try:
                pg_flush(pg_conn, pg_buf)
                pg_conn.close()
            except Exception as e:
                print(f"[pg] close error: {e}", file=sys.stderr)

        if out_handle not in (sys.stdout, sys.stderr):
            try:
                out_handle.close()
            except:
                pass
        if mqttc is not None:
            try:
                mqttc.loop_stop(); mqttc.disconnect()
            except:
                pass


if __name__ == "__main__":
    main()
