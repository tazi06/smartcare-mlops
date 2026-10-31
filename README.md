# SmartCare MLOps

End-to-end healthcare MLOps pipeline: from a simulated wearable to a live risk dashboard.

> **Lab project.** All data is simulated. No real patient data is used. Model scores are not meaningful, because the simulated crises are easy to detect. The goal is the ML lifecycle, not the model.

## What it does

Patient vitals (heart rate, SpO₂, temperature, steps, battery) are streamed, turned into 5-minute features, and scored by a model. High scores become alerts on a live dashboard. A separate batch pipeline prepares training data, and a CI/CD pipeline trains, checks, deploys, and promotes new model versions.

## Architecture

```
                  ┌─────────────────────── REAL-TIME ───────────────────────┐
Simulator ──► Kafka ──┬──► Flink (5-min windows, features) ──► BentoML (XGBoost)
 (2,000 devices)      │                                              │
                      │                                              ▼
                      │                                  PostgreSQL ──► Streamlit
                      │
                      └──► Kafka Connect (S3 sink) ──► MinIO (raw events)
                                                          │
                  ┌───────────────────────── BATCH ───────┘
                  │
                  Airflow + DuckDB ──► MinIO (Parquet features + labels)
                                              │
                  ┌───────────────────────── CI/CD ─────────────────────────┐
                  │  GitHub Actions: train → eval → build → push →          │
                  │                  deploy → smoke test → promote          │
                  └──────────────────────────────────────────────────────────┘
                     MLflow (tracking, registry)      k3s (rolling update)
```

## Tools and why

| Tool | Role | Why |
|---|---|---|
| Python simulator | Generates wearable events | No real data needed. Full control of volume and dirty data |
| Apache Kafka | Event stream | One entry point. Several readers use the same data |
| Kafka Connect (S3 sink) | Copies events to MinIO | Raw copy for training, no custom code |
| Apache Flink | 5-minute event-time windows per patient | Windows need state and event time |
| MinIO | S3-compatible object storage | Raw events, training data, MLflow files |
| Apache Airflow | Orchestrates the ETL | Day-by-day runs, retries, history |
| DuckDB | Cleans and aggregates data | Fast on one small machine, no Spark needed |
| Parquet | Training dataset format | Small, typed, columnar |
| XGBoost | Risk model | Strong on tabular data, handles missing values |
| MLflow | Tracking and model registry | Run history, versions, aliases `candidate` and `production` |
| BentoML | Model REST API | Input checks, health checks, metrics |
| PostgreSQL | Scores and alerts | Simple SQL for the dashboard |
| Streamlit | Live dashboard | One Python file, auto-refresh |
| GitHub Actions | CI/CD | One workflow from training to promotion |
| Docker + Docker Hub | Versioned images | The model is inside the image. Rollback is an older tag |
| k3s (Kubernetes) | Runs the service | Rolling update, readiness checks, rollback |
| Google Cloud | Infrastructure | Private VPC, VMs without public IPs, SSH through IAP |

## The model

- **Task:** predict a crisis in a 5-minute window of a patient.
- **Features:** `hr_mean_5min`, `hr_std_5min`, `spo2_min_5min`, `temp_mean_5min`, `steps_5min`
- **Label:** `1` if the window overlaps a crisis, otherwise `0`.
- **Split:** by time. The oldest 80% train the model, the newest 20% test it.
- **Imbalance:** crises are rare, so `scale_pos_weight` is used.
- **Metrics:** F1, recall, precision, PR-AUC (not accuracy).
- **Alerts:** a score of 0.8 or more is an alert, 0.95 or more is HIGH.

## CI/CD pipeline

One manually started workflow (`.github/workflows/pipeline.yml`) with 7 jobs on a self-hosted runner inside the private network:

| # | Job | What it does |
|---|---|---|
| 1 | `train` | Trains XGBoost, logs the run, and registers a new model version in MLflow. Fails if F1 is below the minimum (input `min_f1`, default 0.5) |
| 2 | `eval` | Scores the candidate and the production model on the same test data. A worse candidate stops the pipeline |
| 3 | `build` | Downloads the model, builds a Docker image with the model inside, and tests the image |
| 4 | `push` | Pushes the image to Docker Hub, tagged with the model version |
| 5 | `deploy` | Applies the manifest to k3s and waits for the rolling update. Rolls back on failure |
| 6 | `smoke` | Calls the live API 10 times. Checks the model version and the scores. Rolls back on failure |
| 7 | `promote` | Moves the MLflow alias `production` to this version |

The `production` alias moves only after the smoke test passes, so it always points to a model that was tested live.

## Repository layout

```
.
├── .github/workflows/pipeline.yml   # the 7-job pipeline
├── training/
│   ├── train.py                     # train, measure, register, gate
│   ├── evaluate.py                  # candidate vs production
│   └── requirements.txt
├── serving/
│   ├── service.py                   # BentoML service
│   ├── fetch_model.py               # downloads the model from MLflow
│   ├── Dockerfile
│   └── requirements.txt
└── deploy/
    ├── k8s.yaml                     # Deployment (2 replicas) and Service
    ├── smoke_test.py
    └── promote.py
```

The simulator, the Flink job, the Airflow DAG, and the dashboard run on their own VMs and are described in the architecture section above.

## Setup

**1. GitHub Secrets** (Settings → Secrets and variables → Actions):

| Secret | Use |
|---|---|
| `MINIO_USER`, `MINIO_PASSWORD` | Read training data from MinIO |
| `DOCKERHUB_USERNAME`, `DOCKERHUB_TOKEN` | Push the image (use a private repository) |

**2. Self-hosted runner** with the label `vpc`, on a machine that can reach MinIO, MLflow, and the k3s API. It needs Docker, Python 3 with `venv`, and `kubectl` with access to the cluster. Use a **private** repository, because a self-hosted runner runs the repository's code.

**3. Image pull secret in k3s**, so the cluster can pull the private image:

```bash
kubectl create secret docker-registry dockerhub \
  --docker-username=<user> --docker-password=<token>
```

**4. Run:** Actions → **pipeline** → Run workflow.

## Call the service

```bash
curl -X POST http://<node-ip>:30000/predict \
  -H "Content-Type: application/json" \
  -d '{"items": [{"patient_id": "p-1", "window_end": "2026-10-03T14:05:00Z",
        "hr_mean_5min": 121, "hr_std_5min": 9, "spo2_min_5min": 88,
        "temp_mean_5min": 37.7, "steps_5min": 55}]}'
```

The answer has `risk_score` and `model_version`.

## Limitations

- The data is synthetic, and crises are easy to see, so F1 is close to 1. This shows that the pipeline works. It is not a quality result.
- The labels (the crisis list) come from the simulator. In a real system they would come from clinicians, later than the vitals.
- Monitoring and observability are not implemented yet. This is the next step (Prometheus, Grafana, drift checks).
- The pipeline is started by hand. A later step can start it when new training data is ready.
- Security scanning (image scan, signing) is not part of this version.

## Next

Monitoring and observability.
