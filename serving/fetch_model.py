import os
from pathlib import Path

import mlflow
import mlflow.xgboost
from mlflow import MlflowClient

NAME = "smartcare-risk"

# Use MODEL_VERSION if given, otherwise the newest version
version = os.environ.get("MODEL_VERSION")
if not version:
    versions = MlflowClient().search_model_versions(f"name='{NAME}'")
    version = str(max(int(v.version) for v in versions))

model = mlflow.xgboost.load_model(f"models:/{NAME}/{version}")

out = Path(__file__).parent / "model"
out.mkdir(exist_ok=True)
model.save_model(str(out / "model.json"))
(out / "VERSION").write_text(version)
print(f"saved model version {version} in {out}")