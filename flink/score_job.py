import json
import os
import urllib.request

from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.udf import udf

KAFKA = "10.10.0.11:9092"
MODEL_URL = "http://10.10.0.15:30000/predict"
PG_URL = "jdbc:postgresql://10.10.0.14:5432/smartcare"
PG_USER = "smartcare_app"
PG_PASSWORD = os.environ["PG_PASSWORD"]
THRESHOLD = 0.8


@udf(result_type=DataTypes.DOUBLE())
def score(hr_mean, hr_std, spo2_min, temp_mean, steps):
    item = {"patient_id": "x", "window_end": "x",
            "hr_mean_5min": hr_mean, "hr_std_5min": hr_std,
            "spo2_min_5min": spo2_min, "temp_mean_5min": temp_mean,
            "steps_5min": steps}
    body = json.dumps({"items": [item]}).encode()
    req = urllib.request.Request(MODEL_URL, data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=3) as r:
            return float(json.load(r)[0]["risk_score"])
    except Exception as e:                 # model tfa: ma kayn score, w l-stream kaykml
        print("model call failed:", e)
        return None


t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
cfg = t_env.get_config()
cfg.set("parallelism.default", "2")
cfg.set("table.exec.source.idle-timeout", "10 s")
t_env.create_temporary_system_function("score", score)

t_env.execute_sql(f"""
CREATE TABLE vitals (
  device_id STRING, patient_id STRING, ts STRING,
  heart_rate INT, spo2 INT, temperature DOUBLE, steps INT, battery INT,
  event_time AS TO_TIMESTAMP(REPLACE(SUBSTR(ts, 1, 19), 'T', ' ')),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' MINUTE
) WITH (
  'connector' = 'kafka',
  'topic' = 'wearables.vitals',
  'properties.bootstrap.servers' = '{KAFKA}',
  'properties.group.id' = 'flink-scoring',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
""")

t_env.execute_sql(f"""
CREATE TABLE scores (
  patient_id STRING NOT NULL, window_start TIMESTAMP(3) NOT NULL, window_end TIMESTAMP(3),
  hr_mean_5min DOUBLE, hr_std_5min DOUBLE, spo2_min_5min DOUBLE,
  temp_mean_5min DOUBLE, steps_5min BIGINT, n_events BIGINT, risk_score DOUBLE,
  PRIMARY KEY (patient_id, window_start) NOT ENFORCED
) WITH (
  'connector' = 'jdbc', 'url' = '{PG_URL}', 'table-name' = 'scores',
  'username' = '{PG_USER}', 'password' = '{PG_PASSWORD}'
)
""")
t_env.execute_sql(f"""
CREATE TABLE alerts (
  patient_id STRING NOT NULL, window_end TIMESTAMP(3) NOT NULL, risk_score DOUBLE,
  level STRING, hr_mean_5min DOUBLE, spo2_min_5min DOUBLE,
  PRIMARY KEY (patient_id, window_end) NOT ENFORCED
) WITH (
  'connector' = 'jdbc', 'url' = '{PG_URL}', 'table-name' = 'alerts',
  'username' = '{PG_USER}', 'password' = '{PG_PASSWORD}'
)
""")

t_env.execute_sql("""
CREATE TEMPORARY VIEW features AS
SELECT
  patient_id, window_start, window_end,
  AVG(CASE WHEN heart_rate BETWEEN 30 AND 220 THEN CAST(heart_rate AS DOUBLE) END) AS hr_mean_5min,
  COALESCE(STDDEV_SAMP(CASE WHEN heart_rate BETWEEN 30 AND 220
                            THEN CAST(heart_rate AS DOUBLE) END), 0) AS hr_std_5min,
  MIN(CASE WHEN spo2 BETWEEN 70 AND 100 THEN CAST(spo2 AS DOUBLE) END) AS spo2_min_5min,
  AVG(CASE WHEN temperature BETWEEN 34 AND 42 THEN temperature END) AS temp_mean_5min,
  CAST(SUM(steps) AS BIGINT) AS steps_5min,
  COUNT(*) AS n_events
FROM TABLE(TUMBLE(TABLE vitals, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
GROUP BY patient_id, window_start, window_end
""")

t_env.execute_sql("""
CREATE TEMPORARY VIEW scored AS
SELECT *, score(hr_mean_5min, hr_std_5min, spo2_min_5min, temp_mean_5min, steps_5min) AS risk_score
FROM features
""")

stmt = t_env.create_statement_set()
stmt.add_insert_sql("INSERT INTO scores SELECT * FROM scored")
stmt.add_insert_sql(f"""
INSERT INTO alerts
SELECT patient_id, window_end, risk_score,
       CASE WHEN risk_score >= 0.95 THEN 'HIGH' ELSE 'MEDIUM' END,
       hr_mean_5min, spo2_min_5min
FROM scored WHERE risk_score >= {THRESHOLD}
""")
stmt.execute()