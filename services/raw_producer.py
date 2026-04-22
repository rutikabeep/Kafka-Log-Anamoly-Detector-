import logging
import re
import time

from kafka import KafkaProducer

from utils.kafka_codecs import utf8_serializer
from utils.settings import (
    HDFS_LOG_FILE,
    KAFKA_BOOTSTRAP_SERVERS,
    RAW_LINES_PER_SECOND,
    RAW_LOGS_TOPIC,
)


BLOCK_ID_PATTERN = re.compile(r"blk_[0-9\-]+")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def extract_block_id(log_line):
    match = BLOCK_ID_PATTERN.search(log_line)
    return match.group(0) if match else "unknown"


def stream_logs():
    if not HDFS_LOG_FILE.exists():
        raise FileNotFoundError(f"No raw HDFS log file found at {HDFS_LOG_FILE}")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=utf8_serializer,
        value_serializer=utf8_serializer,
    )

    sent_since_pause = 0
    with HDFS_LOG_FILE.open("r") as log_file:
        for raw_line in log_file:
            line = raw_line.strip()
            if not line:
                continue

            block_id = extract_block_id(line)
            metadata = producer.send(RAW_LOGS_TOPIC, key=block_id, value=line).get(
                timeout=10
            )
            logging.info(
                "Sent block=%s to partition=%s offset=%s",
                block_id,
                metadata.partition,
                metadata.offset,
            )

            sent_since_pause += 1
            if sent_since_pause >= RAW_LINES_PER_SECOND:
                time.sleep(1)
                sent_since_pause = 0

    producer.flush()
    logging.info("Finished streaming raw logs")


if __name__ == "__main__":
    stream_logs()
