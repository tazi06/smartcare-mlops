import os
from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="etl_vitals",
    start_date=datetime(2026, 10, 1),
    schedule=None,                         
    catchup=False,
    params={"dates": ["2026-10-01"]},      
    tags=["smartcare"],
)
def etl_vitals():

    # ---------- Task 1:  ----------
    @task
    def get_dates(**context):
        return context["params"]["dates"]

    # ---------- Task 2: ETL  ----------
    @task(max_active_tis_per_dag=1)        
    def etl_day(date: str):
        import duckdb

        # --- Setup: DuckDB  ---
        con = duckdb.connect()
        con.execute("SET memory_limit='8GB'; SET threads=2; SET TimeZone='UTC';")

        # --- Connexion l MinIO  ---
        con.execute(f"""
            CREATE SECRET minio (TYPE S3,
              KEY_ID '{os.environ["MINIO_USER"]}',
              SECRET '{os.environ["MINIO_PASSWORD"]}',
              ENDPOINT '{os.environ["MINIO_ENDPOINT"]}',
              URL_STYLE 'path', USE_SSL false)""")

        out = f"s3://clean-data/features/date={date}/features.parquet"

        con.execute(f"""
        COPY (
          -- PHASE 1: EXTRACT + DEDUP
          WITH raw AS (
            SELECT DISTINCT *
            FROM read_json_auto('s3://raw/topics/wearables.vitals/date={date}/*.json.gz')
          ),

          -- PHASE 2: CLEAN
          clean AS (
            SELECT patient_id, CAST(ts AS TIMESTAMPTZ) AS ts,
              CASE WHEN heart_rate  BETWEEN 30 AND 220 THEN heart_rate  END AS heart_rate,
              CASE WHEN spo2        BETWEEN 70 AND 100 THEN spo2        END AS spo2,
              CASE WHEN temperature BETWEEN 34 AND 42  THEN temperature END AS temperature,
              steps
            FROM raw
          ),

          -- PHASE 3: FEATURES
          feats AS (
            SELECT patient_id,
              time_bucket(INTERVAL 5 MINUTE, ts)      AS window_start,
              avg(heart_rate)                         AS hr_mean_5min,
              coalesce(stddev_samp(heart_rate), 0)    AS hr_std_5min,
              min(spo2)                               AS spo2_min_5min,
              avg(temperature)                        AS temp_mean_5min,
              sum(steps)                              AS steps_5min,
              count(*)                                AS n_events
            FROM clean GROUP BY 1, 2
          ),

          -- PHASE 4a: LABELS
          labels AS (
            SELECT patient_id,
                   CAST(start AS TIMESTAMPTZ) AS s,
                   CAST("end" AS TIMESTAMPTZ) AS e
            FROM read_json_auto('s3://raw/topics/labels.truth/*/*.json.gz')
          )

          -- PHASE 4b: LABEL (JOIN)
          SELECT f.*,
            CASE WHEN EXISTS (
              SELECT 1 FROM labels l
              WHERE l.patient_id = f.patient_id
                AND l.s < f.window_start + INTERVAL 5 MINUTE
                AND l.e > f.window_start) THEN 1 ELSE 0 END AS label
          FROM feats f
        )
        -- PHASE 5: LOAD
        TO '{out}' (FORMAT PARQUET)""")

        # --- PHASE 6: CHECK ---
        n, crises = con.execute(
            f"SELECT count(*), sum(label) FROM read_parquet('{out}')").fetchone()
        print(f"{date}: {n:,} fenêtres, {crises:,} en crise")
        return out

    # ---------- Ordre: dates => etl_day Day ----------
    etl_day.expand(date=get_dates())

