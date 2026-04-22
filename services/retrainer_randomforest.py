import pandas as pd
from kafka import KafkaConsumer

from utils.kafka_codecs import json_deserializer
from utils.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    LABELLED_LOGS_TOPIC,
    RETRAIN_BATCH_SIZE,
    TRAIN_MATRIX_PATH,
)
from utils.train import train_model


class StreamingRetrainer:
    def __init__(
        self,
        topic=LABELLED_LOGS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        batch_size=RETRAIN_BATCH_SIZE,
    ):
        # Kafka consumer for incoming data
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset="earliest",
            value_deserializer=json_deserializer,
        )

        self.buffer = []
        self.batch_size = batch_size
        self.version_counter = 1

    def process_message(self, message):
        # Add incoming row(s) to buffer
        if isinstance(message, list):
            self.buffer.extend(message)
        else:
            self.buffer.append(message)

        # Retrain once batch is full
        if len(self.buffer) >= self.batch_size:
            self.retrain()

    def retrain(self):
        print(
            f"Retraining on batch {self.version_counter}... "
            f"Buffer size: {len(self.buffer)}"
        )

        df_batch = pd.DataFrame(self.buffer)
        self.buffer = []  # clear buffer

        # Load previous dataset
        df_old = pd.read_csv(TRAIN_MATRIX_PATH)

        # Combine with new batch
        df_combined = pd.concat([df_old, df_batch], ignore_index=True)

        # Retrain on combined dataset
        clf, report = train_model(
            df_combined,
            version=f"v{self.version_counter}",
        )

        # Save updated combined dataset
        TRAIN_MATRIX_PATH.parent.mkdir(exist_ok=True)
        df_combined.to_csv(TRAIN_MATRIX_PATH, index=False)

        # Increment version counter
        self.version_counter += 1

        return clf, report

    def run(self):
        print("Starting Kafka consumer for retraining...")
        for message in self.consumer:
            self.process_message(message.value)


if __name__ == "__main__":
    StreamingRetrainer().run()
