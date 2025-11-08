import asyncio, os, random, time, json
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext

NATS_URL = os.getenv("NATS_URL","nats://localhost:4222")
COUNTS = {"camera":300,"fas":150,"ac":200,"mv":30,"pm":800,"light":200,"water":150,"iaq":100,"oura":100,"net":200}

async def setup_streams(js: JetStreamContext):
    await js.add_stream(name="FR", subjects=["fr.*"])
    await js.add_stream(name="BMS", subjects=["bms.*"])
    await js.add_stream(name="CLOUD", subjects=["iaq","oura","netlog"])

def now_ms(): return int(time.time()*1000)
def rand_email(): return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=8)) + "@example.com"
def jittered(i): return i*random.uniform(0.8,1.2)

async def publish_loop(js, subj, make_event, rate_hz, population, stop_fraction=0.02, drift_fraction=0.02):
    stopped = set(random.sample(range(population), max(1,int(population*stop_fraction))))
    drifted = set(random.sample([i for i in range(population) if i not in stopped], max(1,int(population*drift_fraction))))
    base_interval = 1.0/rate_hz if rate_hz>0 else 60.0
    while True:
        idx = random.randrange(population)
        if idx in stopped and random.random()<0.98:
            await asyncio.sleep(jittered(base_interval)); continue
        payload = make_event(idx, drift=(idx in drifted))
        await js.publish(subj, payload.encode())
        await asyncio.sleep(jittered(base_interval))

def make_camera(i, drift=False):
    conf = max(0.0, min(1.0, random.gauss(0.9,0.05)))
    if drift: conf = max(0.0, min(1.0, random.gauss(0.4,0.15)))
    return json.dumps({"device_id":f"cam-{i}","ts":now_ms(),"email":rand_email(),"confidence":round(conf,3),"location":"lobby"})

def make_fas(i, drift=False):
    res = "ALLOW" if random.random()>0.1 else "DENY"
    return json.dumps({"device_id":f"fas-{i}","ts":now_ms(),"email":rand_email(),"result":res,"door_id":f"d-{i%20}"})

def metric_event(dev_id, metric, base, sigma, drift=False):
    val = random.gauss(base, sigma)
    if drift and metric=="TEMP": val = base + random.uniform(5,10)
    return json.dumps({"device_id":dev_id,"ts":now_ms(),"metric":metric,"value":round(val,2)})

def make_ac(i, drift=False): return metric_event(f"ac-{i}","TEMP",24,1.0,drift)
def make_mv(i, drift=False): return metric_event(f"mv-{i}","HUM",55,3.0,drift)
def make_pm(i, drift=False): return metric_event(f"pm-{i}","KW",2.5,0.2,drift)
def make_light(i, drift=False): return metric_event(f"light-{i}","LUX",300,20,drift)
def make_water(i, drift=False): return metric_event(f"water-{i}","WATER",0.8,0.1,drift)
def make_iaq(i, drift=False):
    co2 = max(350, int(random.gauss(700,50)))
    if drift: co2 = int(random.gauss(2000,150))
    pm25 = max(0.1, random.gauss(8,2)); temp = random.gauss(24,1)
    return json.dumps({"device_id":f"iaq-{i}","ts":now_ms(),"co2_ppm":co2,"pm25":round(pm25,1),"temp_c":round(temp,1)})

def make_oura_v1(i, drift=False):
    hr = int(max(40, random.gauss(70,5))); steps = int(max(0, random.gauss(50,30)))
    return json.dumps({"user_id":f"oura-{i}","ts":now_ms(),"email":rand_email(),"hr":hr,"steps":steps})

def make_oura_v2(i, drift=False):
    hr = int(max(40, random.gauss(70,5))); steps = int(max(0, random.gauss(50,30)))
    hrv = int(max(10, random.gauss(65,10))) if random.random()>0.2 else None
    skin = round(random.gauss(33.5,0.3),1) if random.random()>0.2 else None
    return json.dumps({"user_id":f"oura-{i}","ts":now_ms(),"email":rand_email(),"hr":hr,"steps":steps,"hrv_ms":hrv,"skin_temp_c":skin})

def make_net(i, drift=False):
    status = "OK" if random.random()>0.95 else ("WARN" if random.random()>0.5 else "ERR")
    return json.dumps({"device_id":f"net-{i}","ts":now_ms(),"bytes_in":random.randint(1000,100000),"bytes_out":random.randint(1000,100000),"status":status})

async def main():
    nc = NATS(); await nc.connect(servers=[NATS_URL]); js = nc.jetstream(); await setup_streams(js)
    tasks = []
    tasks.append(asyncio.create_task(publish_loop(js, "fr.camera", make_camera, rate_hz=(300/10), population=COUNTS["camera"])))
    tasks.append(asyncio.create_task(publish_loop(js, "fr.fas", make_fas, rate_hz=(150/20), population=COUNTS["fas"])))
    tasks.append(asyncio.create_task(publish_loop(js, "bms.ac", make_ac, rate_hz=(3*200/60), population=COUNTS["ac"])))
    tasks.append(asyncio.create_task(publish_loop(js, "bms.mv", make_mv, rate_hz=(3*30/60), population=COUNTS["mv"])))
    tasks.append(asyncio.create_task(publish_loop(js, "bms.pm", make_pm, rate_hz=(3*800/60), population=COUNTS["pm"])))
    tasks.append(asyncio.create_task(publish_loop(js, "bms.light", make_light, rate_hz=(3*200/60), population=COUNTS["light"])))
    tasks.append(asyncio.create_task(publish_loop(js, "bms.water", make_water, rate_hz=(150/3600), population=COUNTS["water"])))
    tasks.append(asyncio.create_task(publish_loop(js, "iaq", make_iaq, rate_hz=(100/300), population=COUNTS["iaq"])))
    async def oura_loop():
        i = 0
        while True:
            idx = i % COUNTS["oura"]
            maker = make_oura_v2 if (i % 5 == 0) else make_oura_v1
            await js.publish("oura", maker(idx).encode())
            i += 1
            await asyncio.sleep(jittered(60/(20*COUNTS["oura"])))
    tasks.append(asyncio.create_task(oura_loop()))
    tasks.append(asyncio.create_task(publish_loop(js, "netlog", make_net, rate_hz=(200/60), population=COUNTS["net"])))
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())