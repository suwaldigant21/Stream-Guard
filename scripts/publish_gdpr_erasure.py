"""Publish a GDPR Article 17 erasure request to gdpr-deletion-requests.

The Phase 8 streaming consumer purges the account from its fan-in window the
moment this record lands on the topic.

Usage:
    uv run python scripts/publish_gdpr_erasure.py C123456789 [request_id]
"""

import sys
import uuid

from kafka import KafkaProducer

from streamguard_serializers import JSONSerializer

BROKER = "localhost:19092"
GDPR_TOPIC = "gdpr-deletion-requests"


def main() -> None:
    if len(sys.argv) < 2:
        print("usage: python scripts/publish_gdpr_erasure.py <account_id> [request_id]")
        sys.exit(1)

    account_id = sys.argv[1]
    request_id = sys.argv[2] if len(sys.argv) > 2 else f"req-{uuid.uuid4().hex[:8]}"
    payload = {"account_id": account_id, "request_id": request_id}

    producer = KafkaProducer(
        bootstrap_servers=[BROKER],
        value_serializer=JSONSerializer(),
        acks="all",
    )
    try:
        producer.send(GDPR_TOPIC, value=payload)
        producer.flush()
        print(f"Published GDPR erasure: {payload}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
