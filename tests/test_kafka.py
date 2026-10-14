import json
import os
import time
from decimal import Decimal
from uuid import uuid4

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import func, select

from bamaq.adapters.database import jobs, outbox
from bamaq.adapters.kafka import KafkaPublisher, MessageHandler, make_consumer
from bamaq.application import COMMANDS, DLQ, Relay
from bamaq.domain import OutboxEvent, Status

pytestmark = [pytest.mark.integration, pytest.mark.kafka]


@pytest.fixture
def broker_topic():
    servers = os.getenv("BAMAQ_TEST_KAFKA_BOOTSTRAP_SERVERS")
    if not servers:
        pytest.skip("Set BAMAQ_TEST_KAFKA_BOOTSTRAP_SERVERS to enable real Kafka tests")
    topic = f"bamaq-test-{uuid4()}"
    admin = AdminClient({"bootstrap.servers": servers})
    for future in admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)]
    ).values():
        future.result(timeout=20)
    yield servers, topic
    for future in admin.delete_topics([topic]).values():
        future.result(timeout=20)


def poll_record(consumer, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        record = consumer.poll(0.2)
        if record is not None and not record.error():
            return record
    raise AssertionError("Kafka record was not received within deadline")


def test_crash_after_database_commit_before_offset_commit(
    sql_service,
    sql_factory,
    broker_topic,
):
    servers, topic = broker_topic
    transaction = sql_service.create("123", Decimal("1500"))
    with sql_factory() as uow:
        event = uow.session.scalar(select(OutboxEvent).where(outbox.c.topic == COMMANDS))
    event.topic = topic
    producer = KafkaPublisher(servers)
    producer.publish(event)
    group = f"bamaq-test-group-{uuid4()}"
    first = make_consumer(servers, group)
    first.subscribe([topic])
    handler = MessageHandler(sql_service)
    try:
        record = poll_record(first)
        handler.handle(record.value(), topic, record.partition(), record.offset())
        original_offset = record.offset()
        # Deliberately close before commit, simulating the crash boundary.
    finally:
        first.close()
    restarted = make_consumer(servers, group)
    restarted.subscribe([topic])
    try:
        redelivery = poll_record(restarted)
        assert redelivery.offset() == original_offset
        handler.handle(redelivery.value(), topic, redelivery.partition(), redelivery.offset())
        restarted.commit(message=redelivery, asynchronous=False)
    finally:
        restarted.close()
        producer.close()
    with sql_factory() as uow:
        assert uow.session.scalar(select(func.count()).select_from(jobs)) == 1
    claim = sql_service.claim()
    sql_service.complete(claim, result=Status.APPROVED)
    assert sql_service.get(transaction.id).status == Status.APPROVED


def test_broker_unavailable_then_real_relay_recovery(
    sql_service,
    sql_factory,
    clock,
    broker_topic,
):
    servers, topic = broker_topic
    sql_service.create("123", Decimal("1"))
    with sql_factory() as uow:
        for event in uow.session.scalars(select(OutboxEvent)):
            event.topic = topic
        uow.commit()
    offline = KafkaPublisher("127.0.0.1:1")
    try:
        with pytest.raises((ConnectionError, TimeoutError)):
            Relay(sql_factory, offline, clock).once()
    finally:
        offline.close()
    with sql_factory() as uow:
        assert uow.outbox.oldest_pending() is not None
    producer = KafkaPublisher(servers)
    relay = Relay(sql_factory, producer, clock)
    try:
        assert relay.once()
        assert relay.once()
        assert not relay.once()
        consumer = make_consumer(servers, f"bamaq-recovery-{uuid4()}")
        consumer.subscribe([topic])
        try:
            first = json.loads(poll_record(consumer).value())
            second = json.loads(poll_record(consumer).value())
            assert first["event_type"] == "transaction.processing_requested"
            assert second["event_type"] == "transaction.status_changed"
        finally:
            consumer.close()
    finally:
        producer.close()


def test_invalid_message_becomes_durable_dlq(
    sql_service,
    sql_factory,
    clock,
    broker_topic,
):
    servers, topic = broker_topic
    producer = KafkaPublisher(servers)
    producer.publish(
        OutboxEvent(
            event_id=str(uuid4()),
            transaction_id=None,
            topic=topic,
            payload={"schema_version": 99},
            created_at=clock.now(),
        )
    )
    consumer = make_consumer(servers, f"bamaq-dlq-{uuid4()}")
    consumer.subscribe([topic])
    try:
        record = poll_record(consumer)
        MessageHandler(sql_service).handle(record.value(), topic, 0, record.offset())
        consumer.commit(message=record, asynchronous=False)
        with sql_factory() as uow:
            event = uow.session.scalar(select(OutboxEvent).where(outbox.c.topic == DLQ))
            assert event is not None
            event.topic = topic  # Isolated test topic; production topics remain untouched.
            uow.commit()
        Relay(sql_factory, producer, clock).once()
        dlq = json.loads(poll_record(consumer).value())
        assert dlq["event_type"] == "message.quarantined"
        assert dlq["data"]["reason"] == "Unsupported schema_version"
    finally:
        consumer.close()
        producer.close()
