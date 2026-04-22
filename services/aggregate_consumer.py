import logging
import threading
import time
from collections import Counter, defaultdict

from kafka import KafkaConsumer, KafkaProducer

from utils.event_extractor import EventExtractor
from utils.kafka_codecs import json_serializer, utf8_deserializer
from utils.settings import (
    AGGREGATED_EVENTS_TOPIC,
    AGGREGATOR_GROUP_ID,
    BLOCK_TIMEOUT_SECONDS,
    KAFKA_BOOTSTRAP_SERVERS,
    MIN_LOGS_PER_BLOCK,
    RAW_LOGS_TOPIC,
)


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class BlockEventAggregator:
    def __init__(self):
        self.extractor = EventExtractor()
        self.buffers = defaultdict(self.empty_buffer)
        self.buffer_lock = threading.Lock()
        self.processed_count = 0

        self.consumer = KafkaConsumer(
            RAW_LOGS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=AGGREGATOR_GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=utf8_deserializer,
            key_deserializer=utf8_deserializer,
        )
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=json_serializer,
        )

    @staticmethod
    def empty_buffer():
        return {"events": Counter(), "count": 0, "start_time": time.time()}

    @staticmethod
    def create_aggregated_row(block_id, events_counter, total_count):
        row = {"BlockId": block_id, "total_logs": total_count}
        row.update({f"E{i}": 0 for i in range(1, 30)})
        row["UNKNOWN"] = 0

        classified_count = 0
        unknown_count = 0
        for event_id, count in events_counter.items():
            if event_id in row:
                row[event_id] = count
                if event_id == "UNKNOWN":
                    unknown_count += count
                else:
                    classified_count += count
            else:
                logging.warning(
                    "Unexpected event type '%s' for block %s: %s occurrences",
                    event_id,
                    block_id,
                    count,
                )

        if classified_count + unknown_count != total_count:
            logging.error(
                "Block %s total mismatch: expected=%s actual=%s",
                block_id,
                total_count,
                classified_count + unknown_count,
            )

        return row

    def flush_block(self, block_id, reason):
        buffer = self.buffers.pop(block_id)
        row = self.create_aggregated_row(block_id, buffer["events"], buffer["count"])
        self.producer.send(AGGREGATED_EVENTS_TOPIC, row).get(timeout=10)

        age = time.time() - buffer["start_time"]
        non_zero_events = {key: value for key, value in row.items() if value}
        logging.info(
            "%s flush for block=%s logs=%s age=%.1fs events=%s",
            reason,
            block_id,
            buffer["count"],
            age,
            non_zero_events,
        )

    def flush_expired_blocks(self):
        while True:
            now = time.time()
            with self.buffer_lock:
                expired_blocks = [
                    block_id
                    for block_id, buffer in self.buffers.items()
                    if now - buffer["start_time"] >= BLOCK_TIMEOUT_SECONDS
                ]
                for block_id in expired_blocks:
                    self.flush_block(block_id, "timeout")

            time.sleep(1)

    def process_message(self, message):
        log_line = message.value
        block_id = message.key or "unknown"
        event_id = self.extractor.extract_event_id(log_line)

        self.processed_count += 1
        if self.processed_count <= 10:
            logging.info(
                "Sample extraction #%s block=%s event=%s log=%s...",
                self.processed_count,
                block_id,
                event_id,
                log_line[:100],
            )

        with self.buffer_lock:
            buffer = self.buffers[block_id]
            buffer["events"][event_id] += 1
            buffer["count"] += 1

            if buffer["count"] >= MIN_LOGS_PER_BLOCK:
                self.flush_block(block_id, "threshold")

    def run(self):
        threading.Thread(target=self.flush_expired_blocks, daemon=True).start()
        logging.info("Aggregator listening on topic %s", RAW_LOGS_TOPIC)

        for message in self.consumer:
            self.process_message(message)


if __name__ == "__main__":
    BlockEventAggregator().run()
