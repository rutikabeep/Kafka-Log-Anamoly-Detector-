import logging

import joblib
import pandas as pd
import torch
from kafka import KafkaConsumer, KafkaProducer

from utils.autoencoder import Autoencoder
from utils.kafka_codecs import json_deserializer, json_serializer, utf8_deserializer
from utils.settings import (
    AE_COLUMNS_PATH,
    AE_MODEL_PATH,
    AGGREGATED_EVENTS_TOPIC,
    ANOMALIES_TOPIC,
    KAFKA_BOOTSTRAP_SERVERS,
    MODEL_VERSION,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class AutoencoderInferenceService:
    def __init__(self, device="cpu"):
        self.device = device
        self.model_type = "autoencoder"
        self.total_inferences = 0
        self.anomaly_count = 0

        self.model, self.mean, self.std, self.threshold = self.load_model()
        self.columns = joblib.load(AE_COLUMNS_PATH)

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
        if not AE_MODEL_PATH.exists():
            raise FileNotFoundError(f"No autoencoder checkpoint found at {AE_MODEL_PATH}")

        checkpoint = torch.load(AE_MODEL_PATH, map_location=self.device)
        model = Autoencoder(input_dim=checkpoint["input_dim"])
        model.load_state_dict(checkpoint["model_state"])
        model.to(self.device)
        model.eval()

        logging.info("Loaded autoencoder checkpoint from %s", AE_MODEL_PATH)
        return model, checkpoint["mean"], checkpoint["std"], checkpoint["threshold"]

    def score(self, row):
        features = pd.DataFrame([row]).reindex(columns=self.columns, fill_value=0)
        x = (features.values - self.mean) / self.std
        x_tensor = torch.tensor(x, dtype=torch.float32).to(self.device)

        with torch.no_grad():
            reconstruction = self.model(x_tensor)
            return ((reconstruction - x_tensor) ** 2).mean().item()

    def publish_anomaly(self, row, score):
        result = {
            "BlockId": row.get("BlockId"),
            "prediction": 1,
            "score": float(score),
            "model_type": self.model_type,
            "model_version": MODEL_VERSION,
        }
        self.producer.send(ANOMALIES_TOPIC, result)
        logging.info("Anomaly #%s: %s", self.anomaly_count, result)

    def run_inference(self, row):
        self.total_inferences += 1
        score = self.score(row)

        if score > self.threshold:
            self.anomaly_count += 1
            self.publish_anomaly(row, score)
            return

        logging.info(
            "Inference #%s normal row with score %.6f",
            self.total_inferences,
            score,
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
    AutoencoderInferenceService().run()
