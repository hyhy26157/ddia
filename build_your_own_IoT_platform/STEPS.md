# Fake Lakehouse (containers)
Start everything locally to mimic on-prem + cloud ingest, Avro to MongoDB, and S3-compatible lake.

## Run
docker compose up -d --build

- NATS: 4222 (console 8222)
- MinIO: http://localhost:9001  (minio / minio12345)
- Trino: http://localhost:8080
- MongoDB: localhost:27017 (db `fake_lake`)

## Simulated realism
- Device outages (~2% silent most of the time)
- Data drift (e.g., temp sensors biased high)
- Schema evolution (Oura adds `hrv_ms`, `skin_temp_c` ~20% of events)
- PII encryption (email) -> stored as `email_enc` in Avro

## Storage
MinIO bucket `raw`: `/<source>/<YYYY>/<MM>/<DD>/<HH>/<MM>/*.avro`
Mongo collections per subject.

Hook up DBT/Spark to move `raw` -> Iceberg bronze/silver/gold via the `iceberg` catalog (Nessie + MinIO already wired for Trino).