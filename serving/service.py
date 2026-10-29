from pathlib import Path

import bentoml
import numpy as np
from pydantic import BaseModel

FEATURES = ["hr_mean_5min", "hr_std_5min", "spo2_min_5min",
            "temp_mean_5min", "steps_5min"]
MODEL_DIR = Path(__file__).parent / "model"


class Features(BaseModel):
    patient_id: str
    window_end: str
    hr_mean_5min: float | None = None
    hr_std_5min: float | None = None
    spo2_min_5min: float | None = None
    temp_mean_5min: float | None = None
    steps_5min: float | None = None


@bentoml.service(resources={"cpu": "1"}, traffic={"timeout": 10})
class RiskService:
    def __init__(self):
        from xgboost import XGBClassifier
        self.model = XGBClassifier()
        self.model.load_model(str(MODEL_DIR / "model.json"))
        self.version = (MODEL_DIR / "VERSION").read_text().strip()

    @bentoml.api
    def predict(self, items: list[Features]) -> list[dict]:
        X = np.array(
            [[np.nan if getattr(i, f) is None else getattr(i, f) for f in FEATURES]
             for i in items], dtype=float)
        scores = self.model.predict_proba(X)[:, 1]
        return [{"patient_id": i.patient_id,
                 "window_end": i.window_end,
                 "risk_score": round(float(s), 4),
                 "model_version": self.version}
                for i, s in zip(items, scores)]