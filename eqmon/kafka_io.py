"""Kafka publishing via confluent-kafka (imported lazily so the rest of the package needs no broker)."""
from __future__ import annotations

import json
import logging
import time
from typing import Iterable, Protocol

log = logging.getLogger(__name__)


class Publisher(Protocol):
    def publish(self, messages: Iterable[tuple[str, dict]]) -> set[str]:
        """Send (key, value) messages; return the set of keys that were NOT delivered."""

    def close(self) -> None: ...


class KafkaPublisher:
    def __init__(self, bootstrap: str, topic: str, flush_timeout_s: float = 30.0):
        from confluent_kafka import Producer  # lazy import

        self.topic = topic
        self.flush_timeout_s = flush_timeout_s
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "client.id": "earthquake-producer",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "gzip",
                "linger.ms": 50,
                "message.timeout.ms": 30_000,
                "socket.timeout.ms": 10_000,
            }
        )

    def wait_until_ready(self, attempts: int = 10, delay_s: float = 5.0) -> None:
        """Block until the broker answers a metadata request (raises after ``attempts``)."""
        from confluent_kafka import KafkaException

        last: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                self._producer.list_topics(timeout=10)
                return
            except KafkaException as exc:
                last = exc
                log.warning("Kafka not reachable (attempt %d/%d): %s", attempt, attempts, exc)
                time.sleep(delay_s)
        raise ConnectionError(f"Kafka broker unreachable after {attempts} attempts: {last}")

    def publish(self, messages: Iterable[tuple[str, dict]]) -> set[str]:
        pending = {key: value for key, value in messages}
        delivered: set[str] = set()

        def on_delivery(err, msg):
            if err is not None:
                log.error("Delivery failed for key %s: %s", msg.key(), err)
            else:
                delivered.add(msg.key().decode("utf-8"))

        for key, value in pending.items():
            payload = json.dumps(value, separators=(",", ":")).encode("utf-8")
            while True:
                try:
                    self._producer.produce(self.topic, key=key.encode("utf-8"), value=payload, on_delivery=on_delivery)
                    break
                except BufferError:  # local queue full: let delivery reports drain
                    self._producer.poll(1.0)
            self._producer.poll(0)
        self._producer.flush(self.flush_timeout_s)
        return set(pending) - delivered

    def close(self) -> None:
        self._producer.flush(self.flush_timeout_s)
