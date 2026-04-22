# Kafka Log Anomaly Detector

Kafka Log Anomaly Detector is a streaming anomaly detection pipeline for HDFS block logs. It turns raw HDFS log lines into block-level event vectors, scores those vectors with either an autoencoder or a Random Forest model, and publishes suspicious blocks to an anomalies topic.

This version has been cleaned up around shared configuration, consistent model artifacts, Docker-friendly Kafka settings, and a clearer service layout.

## What It Does

```text
HDFS_v1/HDFS.log
      |
      v
raw producer
      |
      v
Kafka: raw_logs
      |
      v
block event aggregator
      |
      v
Kafka: aggregated_events
      |
      v
autoencoder or Random Forest inference
      |
      v
Kafka: anomalies
```

Labelled CSV batches can also be streamed to `hdfs_logs` for retraining.

## Project Layout

```text
.
├── docker/
│   └── Dockerfile
├── services/
│   ├── aggregate_consumer.py
│   ├── inference.py
│   ├── inference_randomforest.py
│   ├── labelled_producer.py
│   ├── raw_producer.py
│   ├── retrainer.py
│   └── retrainer_randomforest.py
├── utils/
│   ├── autoencoder.py
│   ├── event_extractor.py
│   ├── kafka_codecs.py
│   ├── settings.py
│   ├── train.py
│   └── train_ae.py
├── docker-compose.yml
├── first_model.py
├── pyproject.toml
├── requirements.txt
├── split_dataset.py
└── README.md
```

Expected data and artifact directories:

```text
HDFS_v1/
└── HDFS.log

preprocessed/
├── Event_occurrence_matrix.csv
├── Event_occurrence_matrix_train.csv
└── Event_occurrence_matrix_test.csv

models/
├── ae_model_v1.pt
├── ae_model_columns_v1.pkl
├── model_v1.joblib
├── model_columns_v1.pkl
├── ae_model_latest.pt
└── model_latest.joblib
```

## Services

| Service | Purpose |
| --- | --- |
| `raw_producer.py` | Streams raw HDFS log lines from `HDFS_v1/HDFS.log` into `raw_logs`. |
| `aggregate_consumer.py` | Groups raw lines by block ID and emits event-count vectors to `aggregated_events`. |
| `inference.py` | Scores aggregated rows with the autoencoder and publishes anomalies. |
| `inference_randomforest.py` | Scores aggregated rows with the Random Forest model and publishes anomalies. |
| `labelled_producer.py` | Streams labelled test rows from CSV into `hdfs_logs`. |
| `retrainer.py` | Retrains the autoencoder from labelled mini-batches. |
| `retrainer_randomforest.py` | Retrains the Random Forest model from labelled mini-batches. |

## Kafka Topics

| Topic | Description |
| --- | --- |
| `raw_logs` | Raw HDFS lines keyed by block ID. |
| `aggregated_events` | One event-count record per flushed block. |
| `anomalies` | Inference results for blocks classified as anomalous. |
| `hdfs_logs` | Labelled preprocessed rows for retraining. |

`docker-compose.yml` creates all four topics automatically.

## Setup

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Start Kafka and the pipeline services:

```bash
docker-compose up -d
```

Follow logs:

```bash
docker-compose logs -f aggregate-consumer
docker-compose logs -f inference
docker-compose logs -f raw-producer
```

## Prepare Data

If you have one labelled matrix at `preprocessed/Event_occurrence_matrix.csv`, split it into train and test files:

```bash
python split_dataset.py
```

Training expects:

- `BlockId`
- `Label` values of `Success` or `Fail`
- optional `Type`
- numeric event-count columns

Raw streaming expects `HDFS_v1/HDFS.log`.

## Train Models

Train the Random Forest baseline:

```bash
python first_model.py
```

Train the autoencoder:

```python
import pandas as pd
from utils.settings import MODEL_VERSION, TRAIN_MATRIX_PATH
from utils.train_ae import train_autoencoder

df = pd.read_csv(TRAIN_MATRIX_PATH)
train_autoencoder(df, version=MODEL_VERSION)
```

Training writes versioned artifacts and latest pointers under `models/`.

## Run Locally

With Kafka running on `localhost:9092`, run each service in a separate terminal:

```bash
python services/raw_producer.py
python services/aggregate_consumer.py
python services/inference.py
```

Use Random Forest inference instead of autoencoder inference:

```bash
python services/inference_randomforest.py
```

Stream labelled rows for retraining:

```bash
python services/labelled_producer.py
python services/retrainer.py
python services/retrainer_randomforest.py
```

## Configuration

Most runtime behavior is configured in `utils/settings.py` and can be overridden with environment variables.
Copy `.env.example` if you want a local checklist of supported settings.

| Variable | Default |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `MODEL_VERSION` | `v1` |
| `RAW_LOGS_TOPIC` | `raw_logs` |
| `AGGREGATED_EVENTS_TOPIC` | `aggregated_events` |
| `ANOMALIES_TOPIC` | `anomalies` |
| `LABELLED_LOGS_TOPIC` | `hdfs_logs` |
| `MIN_LOGS_PER_BLOCK` | `100` |
| `BLOCK_TIMEOUT_SECONDS` | `300` |
| `RF_THRESHOLD` | `0.75` |
| `RETRAIN_BATCH_SIZE` | `5000` |

Docker services set `KAFKA_BOOTSTRAP_SERVERS=kafka:29092` automatically. Local scripts keep using `localhost:9092`.

## Development

Syntax-check the project:

```bash
python -m compileall first_model.py split_dataset.py services utils
```

Check for whitespace issues:

```bash
git diff --check
```

The repo includes `pyproject.toml` with Black and Ruff settings for a repeatable formatting/linting baseline.

## Notes

- The aggregator recognizes event IDs `E1` through `E29` and emits `UNKNOWN` when a log line does not match a known template.
- The autoencoder trains on normal rows and flags rows whose reconstruction error exceeds the saved threshold.
- The Random Forest service expects `models/model_v1.joblib` and `models/model_columns_v1.pkl` by default.
- MLflow logs are written to the local `mlruns/` directory.
