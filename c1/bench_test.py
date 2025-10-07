#!/usr/bin/env python3
import os, sys, time, json, uuid, random, threading, queue, statistics
from datetime import datetime, timezone, timedelta
from dateutil.tz import tzutc
import psycopg2
from psycopg2.extras import register_uuid
from pymongo import MongoClient, WriteConcern
# ---- Workload definitions ----------------------------------------------------
WORKLOADS = {
    "write-heavy": {"write_pct": 90, "read_pct": 10, "read_kind": "point"},
    "balanced":    {"write_pct": 50, "read_pct": 50, "read_kind": "point"},
    "read-heavy":  {"write_pct": 10, "read_pct": 90, "read_kind": "analytic"},
}

KINDS = ["temp", "humidity", "co2", "light"]
LOCATIONS = [
    {"site": "SFO", "room": "A"}, {"site": "SFO", "room": "B"},
    {"site": "NYC", "room": "A"}, {"site": "NYC", "room": "B"},
]

def now_utc():
    return datetime.now(tz=timezone.utc)

# ---- Metrics collector -------------------------------------------------------
class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.w_lat = []  # write latencies (s)
        self.r_lat = []  # read latencies (s)
        self.w_count = 0
        self.r_count = 0

    def add_write(self, lat):
        with self.lock:
            self.w_lat.append(lat); self.w_count += 1

    def add_read(self, lat):
        with self.lock:
            self.r_lat.append(lat); self.r_count += 1

    def snapshot(self):
        with self.lock:
            w = list(self.w_lat); r = list(self.r_lat)
            wc = self.w_count; rc = self.r_count
        return w, r, wc, rc

    @staticmethod
    def summarize(latencies):
        if not latencies: return (0.0, 0.0)
        mean = sum(latencies) / len(latencies)
        p95 = statistics.quantiles(latencies, n=100)[94] if len(latencies) >= 20 else max(latencies)
        return mean, p95

# ---- Postgres backend --------------------------------------------------------
class PgBackend:
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

    CREATE TABLE IF NOT EXISTS iot_readings_fastwrite (
    ts           timestamptz NOT NULL,
    sensor_id    uuid        NOT NULL,
    kind         text        NOT NULL,
    location     jsonb       NOT NULL,
    battery_pct  real        NOT NULL,
    data         jsonb       NOT NULL
    );
    """

    def __init__(self, dsn):
        import psycopg2
        self.psycopg2 = psycopg2
        self.conn = psycopg2.connect(dsn)
        register_uuid()
        self.conn.autocommit = False
        with self.conn.cursor() as cur:
            cur.execute(self.DDL)
        self.conn.commit()

    def insert_one(self, rec):
        t0 = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO iot_readings_fastwrite
                   (ts, sensor_id, kind, location, battery_pct, data)
                   VALUES (%s,%s,%s,%s,%s,%s)""",
                (
                    rec["ts"], rec["sensor_id"], rec["kind"],
                    json.dumps(rec["location"]), rec["battery_pct"],
                    json.dumps(rec["data"]),
                ),
            )
        self.conn.commit()
        return time.perf_counter() - t0

    def select_last_by_sensor(self, sensor_id, limit=10):
        t0 = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT ts, sensor_id, kind, battery_pct
                   FROM iot_readings_fastwrite
                   WHERE sensor_id = %s
                   ORDER BY ts DESC
                   LIMIT %s""",
                (sensor_id, limit),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def aggregate_last_minutes(self, minutes=1):
        t0 = time.perf_counter()
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT kind, count(*), min(ts), max(ts)
                   FROM iot_readings_fastwrite
                   WHERE ts >= NOW() - INTERVAL '%s minutes'
                   GROUP BY 1""",
                (minutes,),
            )
            cur.fetchall()
        return time.perf_counter() - t0

    def close(self):
        try: self.conn.close()
        except: pass

# ---- Mongo backend -----------------------------------------------------------
class MongoBackend:
    def __init__(self, uri):
        from pymongo import MongoClient, ASCENDING, DESCENDING
        self.client = MongoClient(uri, appname="bench_iot", uuidRepresentation="standard")
        self.db = self.client["bench"]
        wc = WriteConcern(w="majority", j=True)  # or w=1, j=True if you don’t want majority
        self.col = self.db.get_collection("iot_readings_fastwrite", write_concern=wc)
        # Schema-ish: create indexes
        self.col.create_index([("sensor_id", ASCENDING), ("ts", DESCENDING)])
        self.col.create_index([("ts", DESCENDING)])

    def insert_one(self, rec):
        t0 = time.perf_counter()
        doc = {
            "ts": rec["ts"],
            "sensor_id": str(rec["sensor_id"]),
            "kind": rec["kind"],
            "location": rec["location"],
            "battery_pct": rec["battery_pct"],
            "data": rec["data"],
        }
        self.col.insert_one(doc)
        return time.perf_counter() - t0

    def select_last_by_sensor(self, sensor_id, limit=10):
        t0 = time.perf_counter()
        list(self.col.find({"sensor_id": str(sensor_id)})
                 .sort("ts", -1).limit(limit))
        return time.perf_counter() - t0

    def aggregate_last_minutes(self, minutes=1):
        t0 = time.perf_counter()
        since = now_utc() - timedelta(minutes=minutes)
        pipeline = [
            {"$match": {"ts": {"$gte": since}}},
            {"$group": {"_id": "$kind", "n": {"$sum": 1},
                        "first_ts": {"$min": "$ts"}, "last_ts": {"$max": "$ts"}}}
        ]
        list(self.col.aggregate(pipeline, allowDiskUse=False))
        return time.perf_counter() - t0

    def close(self):
        try: self.client.close()
        except: pass

# ---- Record generator --------------------------------------------------------
def make_sensors(n):
    return [uuid.uuid4() for _ in range(n)]

def make_record(sensor_id, rng):
    return {
        "ts": now_utc(),
        "sensor_id": sensor_id,
        "kind": rng.choice(KINDS),
        "location": rng.choice(LOCATIONS),
        "battery_pct": rng.uniform(20.0, 100.0),
        "data": {"v": rng.uniform(0.0, 100.0)},
    }

# ---- Worker threads ----------------------------------------------------------
def writer_thread(backend, sensors, rate_per_thread, metrics, stop_ev, rng):
    # rate is per second, simple sleep pacing
    period = 1.0 / max(rate_per_thread, 0.0001)
    i = 0
    while not stop_ev.is_set():
        sensor_id = sensors[i % len(sensors)]
        rec = make_record(sensor_id, rng)
        lat = backend.insert_one(rec)
        metrics.add_write(lat)
        i += 1
        if period > 0: time.sleep(period)

def reader_thread_point(backend, sensors, rate_per_thread, metrics, stop_ev, rng):
    period = 1.0 / max(rate_per_thread, 0.0001)
    while not stop_ev.is_set():
        sensor_id = rng.choice(sensors)
        lat = backend.select_last_by_sensor(sensor_id, limit=10)
        metrics.add_read(lat)
        if period > 0: time.sleep(period)

def reader_thread_analytic(backend, rate_per_thread, metrics, stop_ev):
    period = 1.0 / max(rate_per_thread, 0.0001)
    while not stop_ev.is_set():
        lat = backend.aggregate_last_minutes(minutes=1)
        metrics.add_read(lat)
        if period > 0: time.sleep(period)

# ---- Runner ------------------------------------------------------------------
def run_bench(db, dsn, workload, duration, total_rate, writers, readers, sensors_n, seed):
    rng = random.Random(seed)
    sensors = make_sensors(sensors_n)
    metrics = Metrics()
    stop_ev = threading.Event()

    if db == "pg":
        backend = PgBackend(dsn)
    elif db == "mongo":
        backend = MongoBackend(dsn)
    else:
        print("db must be 'pg' or 'mongo'")
        sys.exit(2)

    cfg = WORKLOADS[workload]
    write_rate = total_rate * (cfg["write_pct"] / 100.0)
    read_rate  = total_rate * (cfg["read_pct"]  / 100.0)
    wr_rate_per_thread = write_rate / max(writers, 1)
    rd_rate_per_thread = read_rate  / max(readers, 1)

    threads = []
    for i in range(writers):
        t = threading.Thread(target=writer_thread,
                             args=(backend, sensors, wr_rate_per_thread, metrics, stop_ev, random.Random(seed+i+1)),
                             daemon=True)
        t.start(); threads.append(t)

    if cfg["read_kind"] == "point":
        for i in range(readers):
            t = threading.Thread(target=reader_thread_point,
                                 args=(backend, sensors, rd_rate_per_thread, metrics, stop_ev, random.Random(seed+100+i)),
                                 daemon=True)
            t.start(); threads.append(t)
    else:
        for i in range(readers):
            t = threading.Thread(target=reader_thread_analytic,
                                 args=(backend, rd_rate_per_thread, metrics, stop_ev),
                                 daemon=True)
            t.start(); threads.append(t)

    t0 = time.perf_counter()
    time.sleep(duration)
    stop_ev.set()
    for t in threads: t.join(timeout=5)
    backend.close()
    t1 = time.perf_counter()
    elapsed = max(t1 - t0, 1e-6)

    w_lat, r_lat, w_count, r_count = metrics.snapshot()
    w_mean, w_p95 = Metrics.summarize(w_lat)
    r_mean, r_p95 = Metrics.summarize(r_lat)
    w_tps = w_count / elapsed
    r_tps = r_count / elapsed

    print("\n=== Results ===")
    print(f"DB: {db}  Workload: {workload}")
    print(f"Duration: {duration:.1f}s  Total target rate: {total_rate:.1f} ops/s "
          f"(writes {write_rate:.1f}, reads {read_rate:.1f})")
    print(f"Writers: {writers} @ {wr_rate_per_thread:.2f} w/s each | Readers: {readers} @ {rd_rate_per_thread:.2f} r/s each")
    print(f"Write ops: {w_count}  Read ops: {r_count}")
    print(f"Write latency mean={w_mean*1000:.2f} ms  p95={w_p95*1000:.2f} ms")
    print(f"Read  latency mean={r_mean*1000:.2f} ms  p95={r_p95*1000:.2f} ms")
    print(f"Throughput (rows/sec): writes={w_tps:.1f} reads={r_tps:.1f}")
    print("Note: 'rows/sec' here equals measured ops/sec (one row per op).")

# ---- CLI ---------------------------------------------------------------------
def parse_args():
    import argparse
    p = argparse.ArgumentParser(description="IoT read/write benchmark for Postgres and Mongo")
    p.add_argument("--db", choices=["pg","mongo"], required=True)
    p.add_argument("--dsn", required=True, help="Postgres DSN or Mongo URI")
    p.add_argument("--workload", choices=list(WORKLOADS.keys()), required=True)
    p.add_argument("--duration", type=int, default=20, help="seconds")
    p.add_argument("--rate", type=float, default=200.0, help="total ops/sec (writes+reads)")
    p.add_argument("--writers", type=int, default=4)
    p.add_argument("--readers", type=int, default=4)
    p.add_argument("--sensors", type=int, default=1000)
    p.add_argument("--seed", type=int, default=42)
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    run_bench(args.db, args.dsn, args.workload, args.duration,
              args.rate, args.writers, args.readers, args.sensors, args.seed)
