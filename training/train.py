import os
import sys

import duckdb
import mlflow
import mlflow.xgboost
from sklearn.metrics import average_precision_score, f1_score, precision_score, recall_score
from xgboost import XGBClassifier

FEATURES = ["hr_mean_5min", "hr_std_5min", "spo2_min_5min",
            "temp_mean_5min", "steps_5min"]
MODEL_NAME = "smartcare-risk"
MIN_F1 = float(os.environ.get("MIN_F1") or 0.5)

# STEP 1: read the training data from MinIO (made by the Airflow ETL)
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
print(f"rows: {len(df):,}   crisis rate: {df['label'].mean():.2%}")

# STEP 2: split by TIME (old data = train, newest 20% = test)
cut = int(len(df) * 0.8)
train, test = df.iloc[:cut], df.iloc[cut:]
X_train, y_train = train[FEATURES], train["label"]
X_test, y_test = test[FEATURES], test["label"]

# STEP 3: train the model (crises are rare, so they get more weight)
params = dict(
    n_estimators=200, max_depth=5, learning_rate=0.1,
    scale_pos_weight=float((y_train == 0).sum() / max((y_train == 1).sum(), 1)),
)
model = XGBClassifier(**params).fit(X_train, y_train)

# STEP 4: measure the model on the test set
proba = model.predict_proba(X_test)[:, 1]
pred = (proba >= 0.5).astype(int)
metrics = {
    "f1": f1_score(y_test, pred),
    "recall": recall_score(y_test, pred),
    "precision": precision_score(y_test, pred),
    "pr_auc": average_precision_score(y_test, proba),
}
print(metrics)

# STEP 5: save the run and the model in MLflow
mlflow.set_experiment("smartcare-risk")
with mlflow.start_run():
    mlflow.log_params(params)
    mlflow.log_param("features", ",".join(FEATURES))
    mlflow.log_param("train_rows", len(train))
    mlflow.log_metrics(metrics)
    info = mlflow.xgboost.log_model(
        model, artifact_path="model",
        registered_model_name=MODEL_NAME,
        input_example=X_test.head(3))

# STEP 6: give the version number to the next jobs of the pipeline
version = info.registered_model_version
github_output = os.environ.get("GITHUB_OUTPUT")
if github_output:
    with open(github_output, "a") as f:
        f.write(f"version={version}\n")

# STEP 7: simple gate. If F1 is too low, the job fails
client = mlflow.MlflowClient()
if metrics["f1"] >= MIN_F1:
    client.set_registered_model_alias(MODEL_NAME, "candidate", version)
    print(f"PASSED: version {version} is now 'candidate'")
else:
    print(f"FAILED: f1 {metrics['f1']:.3f} < {MIN_F1}")
    sys.exit(1)