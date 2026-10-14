import json
import logging
import time

from confluent_kafka import Consumer, Producer

from bamaq.application import COMMANDS
from bamaq.domain import InvalidEvent

log = logging.getLogger("consumer")


class KafkaPublisher:
    def __init__(self, servers: str):
        self.producer = Producer(
            {
                "bootstrap.servers": servers,
                "enable.idempotence": True,
                "acks": "all",
                "delivery.timeout.ms": 5000,
                "request.timeout.ms": 3000,
                "client.id": "bamaq-outbox",
                "logger": logging.getLogger("kafka.producer"),
            }
        )

    def publish(self, event):
        delivery = []

        def acknowledged(error, message):
            delivery.append(error)

        self.producer.produce(
            event.topic,
            key=event.transaction_id or event.event_id,
            value=json.dumps(event.payload, ensure_ascii=True, separators=(",", ":")).encode(),
            on_delivery=acknowledged,
        )
        deadline = time.monotonic() + 6
        while not delivery and time.monotonic() < deadline:
            self.producer.poll(0.1)
        if not delivery:
            raise TimeoutError("Kafka delivery acknowledgement deadline exceeded")
        if delivery[0] is not None:
            raise ConnectionError("Kafka delivery failed")

    def close(self):
        self.producer.flush(6)


class MessageHandler:
    def __init__(self, service):
        self.service = service

    def handle(self, raw, topic, partition, offset):
        raw = raw or b""
        try:
            payload = json.loads(raw)
            received = self.service.receive(payload)
            log.info(
                "job_persisted" if received else "duplicate_or_stale_ignored",
                extra={
                    "transaction_id": payload["transaction_id"],
                    "event_id": payload["event_id"],
                    "generation": payload["generation"],
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "correlation_id": payload["correlation_id"],
                },
            )
        except (InvalidEvent, json.JSONDecodeError, UnicodeDecodeError) as exc:
            reason = str(exc) if isinstance(exc, InvalidEvent) else "Invalid JSON encoding"
            failure_id = self.service.quarantine(raw, reason, topic, partition, offset)
            log.warning(
                "message_quarantined",
                extra={
                    "failure_id": failure_id,
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "error_type": type(exc).__name__,
                },
            )


def make_consumer(servers: str, group: str):
    return Consumer(
        {
            "bootstrap.servers": servers,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "max.poll.interval.ms": 300000,
            "client.id": "bamaq-ingestion",
            "logger": logging.getLogger("kafka.consumer"),
        }
    )


def consume(settings, handler, stop, heartbeat):
    consumer = make_consumer(settings.kafka_bootstrap_servers, settings.kafka_group_id)
    consumer.subscribe([COMMANDS])
    try:
        while not stop.is_set():
            heartbeat()
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError("Kafka poll failed")
            # Keep the current record until durable handoff succeeds. Never poll a later
            # offset after persistence failure. DB outage may force group rejoin/restart.
            while not stop.is_set():
                try:
                    handler.handle(
                        message.value(),
                        message.topic(),
                        message.partition(),
                        message.offset(),
                    )
                    break
                except Exception as exc:
                    log.error(
                        "handoff_failed_retrying_current_record",
                        extra={
                            "error_type": type(exc).__name__,
                            "topic": message.topic(),
                            "partition": message.partition(),
                            "offset": message.offset(),
                        },
                    )
                    heartbeat()
                    stop.wait(1)
            if stop.is_set():
                break
            # A commit failure exits the process. Restart/redelivery uses durable dedup.
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
