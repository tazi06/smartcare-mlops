import os
import sys

import duckdb
import mlflow
import mlflow.xgboost
from sklearn.metrics import f1_score, precision_score, recall_score

FEATURES = ["hr_mean_5min", "hr_std_5min", "spo2_min_5min",
            "temp_mean_5min", "steps_5min"]
MODEL_NAME = "smartcare-risk"
TOLERANCE = 0.01

# STEP 1: read the data (same as train.py)
con = duckdb.connect()
con.execute(f"""
CREATE SECRET minio (TYPE S3,
  KEY_ID '{os.environ["MINIO_USER"]}',
  SECRET '{os.environ["MINIO_PASSWORD"]}',
  ENDPOINT '{os.environ["MINIO_ENDPOINT"]}',
  URL_STYLE 'path', USE_SSL false)""")
df = con.execute("""
SELECT * FROM read_parquet('s3://clean-data/features/*/features.parquet')
ORDER BY window_start""").df()

# STEP 2: one test set for both models (newest 20%)
test = df.iloc[int(len(df) * 0.8):]
X_test, y_test = test[FEATURES], test["label"]
print(f"test rows: {len(test):,}")


# STEP 3: score a model by its alias in the MLflow registry
def score(alias):
    try:
        model = mlflow.xgboost.load_model(f"models:/{MODEL_NAME}@{alias}")
    except Exception:
        return None                       # this alias does not exist yet
    pred = (model.predict_proba(X_test)[:, 1] >= 0.5).astype(int)
    return {"f1": f1_score(y_test, pred),
            "recall": recall_score(y_test, pred),
            "precision": precision_score(y_test, pred)}


candidate = score("candidate")
production = score("production")
print("candidate :", candidate)
print("production:", production)

# STEP 4: decide
if candidate is None:
    print("FAILED: no candidate model")
    sys.exit(1)

if production is None:
    print("PASSED: first model, nothing to compare with")
elif (candidate["recall"] >= production["recall"] - TOLERANCE
      and candidate["f1"] >= production["f1"] - TOLERANCE):
    print("PASSED: candidate is as good as production, or better")
else:
    print("FAILED: candidate is worse than production")
    sys.exit(1)
