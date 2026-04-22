import logging

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

from utils.kafka_codecs import json_deserializer, json_serializer, utf8_deserializer
from utils.settings import (
    AGGREGATED_EVENTS_TOPIC,
    ANOMALIES_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    MODEL_VERSION,
    RF_COLUMNS_PATH,
    RF_MODEL_PATH,
    RF_THRESHOLD,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class RandomForestInferenceService:
    def __init__(self):
        self.model_type = "random_forest"
        self.total_inferences = 0
        self.anomaly_count = 0

        self.model = self.load_model()
        self.columns = joblib.load(RF_COLUMNS_PATH)

        self.consumer = KafkaConsumer(
            AGGREGATED_EVENTS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            value_deserializer=json_deserializer,
            key_deserializer=utf8_deserializer,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer,
        )

    def load_model(self):
        if not RF_MODEL_PATH.exists():
            raise FileNotFoundError(f"No Random Forest model found at {RF_MODEL_PATH}")

        logging.info("Loaded Random Forest model from %s", RF_MODEL_PATH)
        return joblib.load(RF_MODEL_PATH)

    def score(self, row):
        features = pd.DataFrame([row]).reindex(columns=self.columns, fill_value=0)
        return self.model.predict_proba(features)[0][1]

    def publish_anomaly(self, row, probability):
        result = {
            "BlockId": row.get("BlockId"),
            "prediction": 1,
            "probability": float(probability),
            "model_type": self.model_type,
            "model_version": MODEL_VERSION,
        }
        self.producer.send(ANOMALIES_TOPIC, result)
        logging.info("Anomaly #%s: %s", self.anomaly_count, result)

    def run_inference(self, row):
        self.total_inferences += 1
        probability = self.score(row)

        if probability >= RF_THRESHOLD:
            self.anomaly_count += 1
            self.publish_anomaly(row, probability)
            return

        logging.info(
            "Inference #%s normal row with probability %.6f",
            self.total_inferences,
            probability,
        )

    def run(self):
        logging.info(
            "Starting %s inference on topic %s",
            self.model_type,
            AGGREGATED_EVENTS_TOPIC,
        )
        for message in self.consumer:
            self.run_inference(message.value)


if __name__ == "__main__":
    RandomForestInferenceService().run()
