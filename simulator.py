import argparse, json, random, time
from datetime import datetime, timedelta, timezone
from math import cos, pi
from confluent_kafka import Producer

ap = argparse.ArgumentParser()
ap.add_argument("--bootstrap", default="")
ap.add_argument("--patients", type=int, default=2000)
ap.add_argument("--interval", type=int, default=60)    # simulated seconds between 2 readings
ap.add_argument("--speedup", type=float, default=1)    # 1 = real time, 0 = as fast as possible
ap.add_argument("--start", default="now")              # "now" or a date like 2026-10-01
ap.add_argument("--days", type=int, default=0)         # 0 = run forever
ap.add_argument("--shard", default="0/1")              # "i/n" to run several processes
a = ap.parse_args()

si, sn = map(int, a.shard.split("/"))
pids = list(range(si, a.patients, sn))
random.seed(si)

EPISODES_PER_DAY = 0.5                                 # ~1% of the time in crisis
p_start = EPISODES_PER_DAY * a.interval / 86400
drain = 90 / (3 * 86400 / a.interval)                  # battery 100 -> 10 in ~3 days
tick_wall = a.interval / a.speedup if a.speedup > 0 else 0

iso = lambda t: t.isoformat(timespec="seconds")
step = timedelta(seconds=a.interval)

if a.start == "now":
    t = datetime.now(timezone.utc).replace(second=0, microsecond=0)
else:
    t = datetime.fromisoformat(a.start).replace(tzinfo=timezone.utc)
end = t + timedelta(days=a.days) if a.days else None

prod = Producer({
    "bootstrap.servers": a.bootstrap,
    "linger.ms": 100, "batch.size": 524288,
    "compression.type": "lz4",
    "queue.buffering.max.messages": 1000000,
})
n_sent = 0


def send(topic, key, obj):
    global n_sent
    while True:
        try:
            prod.produce(topic, key=key, value=json.dumps(obj))
            break
        except BufferError:
            prod.poll(0.5)
    n_sent += 1
    if n_sent % 10000 == 0:
        prod.poll(0)


S = {i: dict(hr=random.gauss(72, 8), spo2=random.gauss(97, 0.8),
             temp=random.gauss(36.7, 0.2), batt=random.uniform(40, 100),
             c_start=None, c_end=None) for i in pids}

last_print = time.time()
try:
    while end is None or t < end:                      # <-- the while loop
        tick_start = time.time()
        hour = t.hour + t.minute / 60
        night = 0.5 * (1 + cos((hour - 3) / 24 * 2 * pi))

        for i in pids:
            s = S[i]
            pid = f"p-{i:06d}"

            # crisis: end / start (the crisis list is the ground truth)
            if s["c_end"] and t >= s["c_end"]:
                send("labels.truth", pid, {"patient_id": pid, "start": iso(s["c_start"]),
                                           "end": iso(s["c_end"]), "label": 1})
                s["c_start"] = s["c_end"] = None
            if not s["c_end"] and random.random() < p_start:
                s["c_start"], s["c_end"] = t, t + timedelta(minutes=random.randint(10, 60))
            in_crisis = s["c_end"] is not None

            # battery / dead device
            s["batt"] -= drain
            if s["batt"] < 5:
                if random.random() < 0.002:
                    s["batt"] = 100
                continue

            hr = s["hr"] - 8 * night + random.gauss(0, 3)
            spo2 = s["spo2"] + random.gauss(0, 0.5)
            temp = s["temp"] + random.gauss(0, 0.1)
            if in_crisis:
                hr += random.gauss(45, 8)
                spo2 -= random.gauss(8, 1.5)
                temp += random.gauss(1, 0.2)

            ev = {"device_id": f"wb-{i:06d}", "patient_id": pid, "ts": iso(t),
                  "heart_rate": round(hr), "spo2": round(min(spo2, 100)),
                  "temperature": round(temp, 1),
                  "steps": 0 if night > 0.8 else random.randint(0, 25),
                  "battery": round(s["batt"])}

            # dirty data for the ETL
            r = random.random()
            if r < 0.01:
                ev[random.choice(["heart_rate", "spo2", "temperature"])] = None
            elif r < 0.012:
                ev["heart_rate"] = random.choice([0, 250])
            elif r < 0.022:
                ev["ts"] = iso(t - timedelta(minutes=random.randint(1, 10)))

            send("wearables.vitals", pid, ev)
            if random.random() < 0.005:
                send("wearables.vitals", pid, ev)      # duplicate

        t += step

        # speed control: wait so that time runs at --speedup
        if tick_wall:
            left = tick_wall - (time.time() - tick_start)
            if left > 0:
                time.sleep(left)

        if time.time() - last_print > 10:
            print(f"[{si}/{sn}] sim time {t.isoformat()}  sent={n_sent:,}", flush=True)
            last_print = time.time()

except KeyboardInterrupt:
    print("stopping...")

prod.flush()
print(f"[{si}/{sn}] done: {n_sent:,} events", flush=True)