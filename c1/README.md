Project Idea: “Sensor Analytics — SQL vs NoSQL Trade-offs”
1️⃣ Scenario

You’re building a tiny “IoT sensor analytics” platform (tying nicely to your real work ).
It has two data-flow parts:

Operational subsystem (OLTP) → handles real-time sensor writes (simulating an on-prem device feed).

Analytical subsystem (OLAP) → performs aggregations and trend queries.

You’ll implement both with:

PostgreSQL (SQL / ACID / relational)

MongoDB (NoSQL / flexible schema)

and compare their behavior as you vary the read/write workload.

2️⃣ Architecture
Layer	Tech	Purpose
Data generator	Python script	Simulate ~10 sensors sending readings every X seconds
OLTP sink	PostgreSQL & MongoDB	Store raw readings
Analytics queries	Python notebook / SQL engine	Run aggregate queries (hourly avg temp per device, etc.)
Visualisation	Plotly / Matplotlib	Show read/write latency & throughput comparisons
3️⃣ Workload Design

Define 3 workload mixes:

Workload	Write %	Read %	Query type
Write-heavy	90	10	Streaming ingestion only
Balanced	50	50	Mixed read/write
Read-heavy	10	90	Analytical aggregations

Measure:

1. Mean/95-percentile write latency

2. Mean/95-percentile read latency

3. Throughput (rows/sec)


## configs

### PostgreSQL 

docker run -d --name pg \
  -e POSTGRES_PASSWORD=pw \
  -p 5432:5432 \
  --cpus="2.0" \
  --memory="2g" \
  --memory-swap="2g" \
  -v pgdata:/var/lib/postgresql/data \
  postgres:16

### MongoDB 

docker run -d --name mongo \
  -e MONGO_INITDB_ROOT_USERNAME=admin \
  -e MONGO_INITDB_ROOT_PASSWORD=pw \
  -p 27018:27017 \
  --cpus="2.0" \
  --memory="2g" \
  --memory-swap="2g" \
  -v mongodata:/data/db \
  mongo:7


## mqtt

docker run -d --name mqtt \
  -p 1883:1883 -p 9001:9001 \
  -v ~/mosq/mosquitto.conf:/mosquitto/config/mosquitto.conf \
  eclipse-mosquitto:2

sudo apt install -y mosquitto-clients

mosquitto_sub -h localhost -t 'iot/sensors/#' -v

### files
created a config file for mqtt at ls -l ~/mosq/mosquitto.conf

## steps

- run psql in the postgres docker
psql -h 127.0.0.1 -p 5432 -U postgres

- check tables
\dt
exit

python3 c1/fake_data_generator.py \
  --sensors 10 --rate 2 \
  --mqtt-broker localhost:1883 \
  --pg-dsn "postgresql://postgres:pw@localhost:5432/postgres" \
  --pg-create-table \
  --pg-batch 500

### Workload 1 — Write-heavy (90/10, streaming + point reads)
python3 c1/bench_test.py \
  --db pg \
  --dsn "postgresql://postgres:pw@localhost:5432/postgres" \
  --workload write-heavy \
  --duration 30 --rate 200 --writers 4 --readers 2 --sensors 2000

python3 c1/bench_test.py \
  --db mongo \
  --dsn "mongodb://admin:pw@localhost:27018/?authSource=admin" \
  --workload write-heavy \
  --duration 30 --rate 200 --writers 4 --readers 2 --sensors 2000

=== Results === with durability, with 3 indexes 
DB: pg  Workload: write-heavy
Duration: 30.0s  Total target rate: 200.0 ops/s (writes 180.0, reads 20.0)
Writers: 4 @ 45.00 w/s each | Readers: 2 @ 10.00 r/s each
Write ops: 4204  Read ops: 578
Write latency mean=6.11 ms  p95=8.92 ms
Read  latency mean=3.75 ms  p95=9.15 ms
Throughput (rows/sec): writes=139.9 reads=19.2
Note: 'rows/sec' here equals measured ops/sec (one row per op).

=== Results === with durability
DB: mongo  Workload: write-heavy
Duration: 30.0s  Total target rate: 200.0 ops/s (writes 180.0, reads 20.0)
Writers: 4 @ 45.00 w/s each | Readers: 2 @ 10.00 r/s each
Write ops: 4231  Read ops: 592
Write latency mean=5.99 ms  p95=10.20 ms
Read  latency mean=1.36 ms  p95=2.29 ms
Throughput (rows/sec): writes=140.7 reads=19.7
Note: 'rows/sec' here equals measured ops/sec (one row per op).

=== Results === without durability
DB: mongo  Workload: write-heavy
Duration: 30.0s  Total target rate: 200.0 ops/s (writes 180.0, reads 20.0)
Writers: 4 @ 45.00 w/s each | Readers: 2 @ 10.00 r/s each
Write ops: 4768  Read ops: 586
Write latency mean=2.22 ms  p95=3.98 ms
Read  latency mean=2.19 ms  p95=4.56 ms
Throughput (rows/sec): writes=158.4 reads=19.5
Note: 'rows/sec' here equals measured ops/sec (one row per op).

=== Results === with durability
DB: mongo  Workload: write-heavy
Duration: 30.0s  Total target rate: 1000.0 ops/s (writes 900.0, reads 100.0)
Writers: 8 @ 112.50 w/s each | Readers: 2 @ 50.00 r/s each
Write ops: 14165  Read ops: 2824
Write latency mean=7.85 ms  p95=12.16 ms
Read  latency mean=1.07 ms  p95=2.34 ms
Throughput (rows/sec): writes=472.0 reads=94.1
Note: 'rows/sec' here equals measured ops/sec (one row per op).