import os
from pathlib import Path


PROJECT_NAME = "Kafka Log Anomaly Detector"
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1")


def env_int(name, default):
    return int(os.getenv(name, str(default)))


def env_float(name, default):
    return float(os.getenv(name, str(default)))


def env_path(name, default):
    return Path(os.getenv(name, default))


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

RAW_LOGS_TOPIC = os.getenv("RAW_LOGS_TOPIC", "raw_logs")
AGGREGATED_EVENTS_TOPIC = os.getenv("AGGREGATED_EVENTS_TOPIC", "aggregated_events")
ANOMALIES_TOPIC = os.getenv("ANOMALIES_TOPIC", "anomalies")
LABELLED_LOGS_TOPIC = os.getenv("LABELLED_LOGS_TOPIC", "hdfs_logs")

AGGREGATOR_GROUP_ID = os.getenv("AGGREGATOR_GROUP_ID", "kafka_log_anomaly_aggregator")
MIN_LOGS_PER_BLOCK = env_int("MIN_LOGS_PER_BLOCK", 100)
BLOCK_TIMEOUT_SECONDS = env_int("BLOCK_TIMEOUT_SECONDS", 300)

DATA_DIR = env_path("DATA_DIR", "preprocessed")
MODEL_DIR = env_path("MODEL_DIR", "models")
HDFS_LOG_FILE = env_path("HDFS_LOG_FILE", "HDFS_v1/HDFS.log")

EVENT_MATRIX_PATH = DATA_DIR / "Event_occurrence_matrix.csv"
TRAIN_MATRIX_PATH = DATA_DIR / "Event_occurrence_matrix_train.csv"
TEST_MATRIX_PATH = DATA_DIR / "Event_occurrence_matrix_test.csv"

RF_MODEL_PATH = env_path("RF_MODEL_PATH", str(MODEL_DIR / f"model_{MODEL_VERSION}.joblib"))
RF_LATEST_MODEL_PATH = env_path("RF_LATEST_MODEL_PATH", str(MODEL_DIR / "model_latest.joblib"))
RF_COLUMNS_PATH = env_path(
    "RF_COLUMNS_PATH",
    str(MODEL_DIR / f"model_columns_{MODEL_VERSION}.pkl"),
)
RF_THRESHOLD = env_float("RF_THRESHOLD", 0.75)

AE_MODEL_PATH = env_path("AE_MODEL_PATH", str(MODEL_DIR / f"ae_model_{MODEL_VERSION}.pt"))
AE_LATEST_MODEL_PATH = env_path("AE_LATEST_MODEL_PATH", str(MODEL_DIR / "ae_model_latest.pt"))
AE_COLUMNS_PATH = env_path(
    "AE_COLUMNS_PATH",
    str(MODEL_DIR / f"ae_model_columns_{MODEL_VERSION}.pkl"),
)

RAW_LINES_PER_SECOND = env_int("RAW_LINES_PER_SECOND", 5)
LABELLED_BATCH_SIZE = env_int("LABELLED_BATCH_SIZE", 500)
LABELLED_STREAM_DELAY_SECONDS = env_float("LABELLED_STREAM_DELAY_SECONDS", 0.01)
RETRAIN_BATCH_SIZE = env_int("RETRAIN_BATCH_SIZE", 5000)
