import pytest
from kafka import KafkaConsumer, KafkaProducer

from streamguard_serializers import JSONDeserializer, JSONSerializer

BROKER = "localhost:19092"
TOPIC = "transactions-test"


@pytest.fixture(scope="module")
def producer():
    p = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=JSONSerializer(),
        acks="all",
    )
    yield p
    p.close()


def test_redpanda_accepts_messages(producer):
    msg = {"step": 1, "type": "PAYMENT", "amount": 100.0, "isFraud": 0}
    producer.send(TOPIC, value=msg)
    producer.flush()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        auto_offset_reset="earliest",
        value_deserializer=JSONDeserializer(),
        consumer_timeout_ms=10000,
    )
    try:
        got = [next(consumer).value for _ in range(1)]
    except StopIteration:
        got = []
    finally:
        consumer.close()

    assert got, "expected at least one message back from Redpanda"
    assert got[0] == msg
