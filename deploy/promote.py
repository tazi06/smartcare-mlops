import os

from mlflow import MlflowClient

NAME = "smartcare-risk"
version = os.environ["MODEL_VERSION"]

client = MlflowClient()
client.set_registered_model_alias(NAME, "production", version)
print(f"PROMOTED: version {version} is now 'production'")