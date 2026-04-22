import logging
import time

import pandas as pd
from kafka import KafkaProducer

from utils.kafka_codecs import json_serializer
from utils.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    LABELLED_BATCH_SIZE,
    LABELLED_LOGS_TOPIC,
    LABELLED_STREAM_DELAY_SECONDS,
    TEST_MATRIX_PATH,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def stream_labelled_batches():
    if not TEST_MATRIX_PATH.exists():
        raise FileNotFoundError(f"No test matrix found at {TEST_MATRIX_PATH}")

    data = pd.read_csv(TEST_MATRIX_PATH)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=json_serializer,
    )

    total_batches = (len(data) + LABELLED_BATCH_SIZE - 1) // LABELLED_BATCH_SIZE
    for batch_index in range(total_batches):
        start = batch_index * LABELLED_BATCH_SIZE
        end = min(start + LABELLED_BATCH_SIZE, len(data))
        batch = data.iloc[start:end].to_dict(orient="records")

        producer.send(LABELLED_LOGS_TOPIC, batch)
        logging.info(
            "Sent labelled batch %s/%s with %s rows",
            batch_index + 1,
            total_batches,
            len(batch),
        )
        time.sleep(LABELLED_STREAM_DELAY_SECONDS)

    producer.flush()
    logging.info("Finished streaming labelled batches")


if __name__ == "__main__":
    stream_labelled_batches()
