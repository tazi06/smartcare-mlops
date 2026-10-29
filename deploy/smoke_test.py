import json
import os
import sys
import time
import urllib.request

URL = os.environ.get("URL", "http://10.10.0.15:30000")
EXPECTED = os.environ["EXPECTED_VERSION"]

NORMAL = {"patient_id": "normal", "window_end": "2026-10-03T14:05:00Z",
          "hr_mean_5min": 74, "hr_std_5min": 3, "spo2_min_5min": 97,
          "temp_mean_5min": 36.7, "steps_5min": 60}
CRISIS = {"patient_id": "crisis", "window_end": "2026-10-03T14:05:00Z",
          "hr_mean_5min": 121, "hr_std_5min": 9, "spo2_min_5min": 88,
          "temp_mean_5min": 37.7, "steps_5min": 55}


def predict():
    body = json.dumps({"items": [NORMAL, CRISIS]}).encode()
    req = urllib.request.Request(f"{URL}/predict", data=body,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


# STEP 1: wait until the service answers
for _ in range(30):
    try:
        urllib.request.urlopen(f"{URL}/readyz", timeout=3)
        break
    except Exception:
        time.sleep(2)
else:
    sys.exit("FAILED: service is not ready")

# STEP 2: call it 10 times and check every answer
for i in range(10):
    normal, crisis = predict()
    if str(normal["model_version"]) != EXPECTED:
        sys.exit(f"FAILED: running model is v{normal['model_version']}, expected v{EXPECTED}")
    if normal["risk_score"] >= 0.5:
        sys.exit(f"FAILED: normal patient got a high score ({normal['risk_score']})")
    if crisis["risk_score"] < 0.5:
        sys.exit(f"FAILED: crisis patient got a low score ({crisis['risk_score']})")

print(f"OK: 10/10 calls, model v{EXPECTED}, "
      f"normal={normal['risk_score']}, crisis={crisis['risk_score']}")