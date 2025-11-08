import os, json, time, io, asyncio
from nats.aio.client import Client as NATS
from fastavro import parse_schema, schemaless_writer
from pymongo import MongoClient
from minio import Minio
from cryptography.fernet import Fernet

NATS_URL = os.getenv("NATS_URL","nats://localhost:4222")
MONGO_URL = os.getenv("MONGO_URL","mongodb://localhost:27017")
MINIO_ENDPOINT=os.getenv("MINIO_ENDPOINT","minio:9000")
MINIO_ACCESS_KEY=os.getenv("MINIO_ACCESS_KEY","minio")
MINIO_SECRET_KEY=os.getenv("MINIO_SECRET_KEY","minio12345")
MINIO_BUCKET=os.getenv("MINIO_BUCKET","raw")
FERNET = Fernet(Fernet.generate_key())  # demo only

import pathlib
SCHEMA_DIR = pathlib.Path("/app/schemas")
def load(name):
    with open(SCHEMA_DIR/name) as f: return parse_schema(json.load(f))
CAM = load("facial_camera.avsc")
FAS = load("fas.avsc")
BMS = load("bms.avsc")
IAQ = load("iaq.avsc")
OURA_V1 = load("oura_v1.avsc")
OURA_V2 = load("oura_v2.avsc")
NET = load("netlog.avsc")

def enc(s: str)->str: return FERNET.encrypt(s.encode()).decode()
def to_avro_bytes(rec, schema):
    bio = io.BytesIO(); schemaless_writer(bio, schema, rec); return bio.getvalue()
def put_raw(minio, prefix, payload):
    from datetime import datetime
    ts = datetime.utcnow().strftime("%Y/%m/%d/%H/%M")
    name = f"{prefix}/{ts}/{int(time.time()*1000)}.avro"
    minio.put_object(MINIO_BUCKET, name, io.BytesIO(payload), len(payload))
    return name

def normalize(kind, j):
    if kind=="fr.camera":
        return to_avro_bytes({"device_id":j["device_id"],"ts":j["ts"],"email_enc":enc(j["email"]),"confidence":float(j["confidence"]),"location":j["location"]}, CAM)
    if kind=="fr.fas":
        return to_avro_bytes({"device_id":j["device_id"],"ts":j["ts"],"email_enc":enc(j["email"]),"result":j["result"],"door_id":j["door_id"]}, FAS)
    if kind.startswith("bms."):
        return to_avro_bytes({"device_id":j["device_id"],"ts":j["ts"],"metric":j["metric"],"value":float(j["value"])}, BMS)
    if kind=="iaq":
        return to_avro_bytes({"device_id":j["device_id"],"ts":j["ts"],"co2_ppm":int(j["co2_ppm"]),"pm25":float(j["pm25"]),"temp_c":float(j["temp_c"])}, IAQ)
    if kind=="oura":
        base={"user_id":j["user_id"],"ts":j["ts"],"email_enc":enc(j["email"]),"hr":int(j["hr"]),"steps":int(j["steps"])}
        if "hrv_ms" in j or "skin_temp_c" in j:
            base["hrv_ms"]=j.get("hrv_ms"); base["skin_temp_c"]=j.get("skin_temp_c")
            return to_avro_bytes(base, OURA_V2)
        return to_avro_bytes(base, OURA_V1)
    if kind=="netlog":
        return to_avro_bytes({"device_id":j["device_id"],"ts":j["ts"],"bytes_in":int(j["bytes_in"]),"bytes_out":int(j["bytes_out"]),"status":j["status"]}, NET)
    raise ValueError("unknown kind "+kind)

async def main():
    nc = NATS(); await nc.connect(servers=[NATS_URL]); js = nc.jetstream()
    mongo = MongoClient(MONGO_URL).fake_lake
    minio = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    async def handle(msg):
        subj = msg.subject
        j = json.loads(msg.data.decode())
        try:
            payload = normalize(subj, j)
            doc = j.copy(); doc.pop("email", None); doc["_raw_avro_len"]=len(payload)
            mongo[subj.replace('.','_')].insert_one(doc)
            put_raw(minio, subj.replace('.','/'), payload)
        except Exception as e:
            mongo["dead_letter"].insert_one({"subject":subj,"error":str(e),"raw":j})
        await msg.ack()
    for s in ["fr.camera","fr.fas","bms.ac","bms.mv","bms.pm","bms.light","bms.water","iaq","oura","netlog"]:
        await js.subscribe(s, durable="proc_"+s.replace('.','_'), cb=handle, manual_ack=True, ack_wait=30)
    print("processor running")
    while True: await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())