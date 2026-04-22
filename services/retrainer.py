import pandas as pd
from kafka import KafkaConsumer

from utils.kafka_codecs import json_deserializer
from utils.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    LABELLED_LOGS_TOPIC,
    RETRAIN_BATCH_SIZE,
    TRAIN_MATRIX_PATH,
)
from utils.train_ae import train_autoencoder


class StreamingRetrainerAE:
    def __init__(
        self,
        topic=LABELLED_LOGS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        batch_size=RETRAIN_BATCH_SIZE,
        device="cpu",
    ):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            value_deserializer=json_deserializer,
        )
        self.buffer = []
        self.batch_size = batch_size
        self.version_counter = 1
        self.device = device

    def process_message(self, message):
        if isinstance(message, list):
            self.buffer.extend(message)
        else:
            self.buffer.append(message)

        if len(self.buffer) >= self.batch_size:
            self.retrain()

    def retrain(self):
        print(
            f"Retraining AE model v{self.version_counter}... "
            f"Buffer size: {len(self.buffer)}"
        )

        df_batch = pd.DataFrame(self.buffer)
        self.buffer = []

        # Load old data
        if TRAIN_MATRIX_PATH.exists():
            df_old = pd.read_csv(TRAIN_MATRIX_PATH)
            df_combined = pd.concat([df_old, df_batch], ignore_index=True)
        else:
            df_combined = df_batch

        # Train
        ae, report, threshold = train_autoencoder(
            df_combined,
            version=f"v{self.version_counter}",
            device=self.device,
        )

        # Save combined dataset
        TRAIN_MATRIX_PATH.parent.mkdir(exist_ok=True)
        df_combined.to_csv(TRAIN_MATRIX_PATH, index=False)

        self.version_counter += 1
        return ae, report, threshold

    def run(self):
        print("Starting Kafka consumer for AE retraining...")
        for message in self.consumer:
            self.process_message(message.value)


if __name__ == "__main__":
    StreamingRetrainerAE().run()
