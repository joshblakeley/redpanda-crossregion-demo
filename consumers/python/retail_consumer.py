#!/usr/bin/env python3
"""
Retail orders consumer — standard Kafka client (confluent-kafka).

Connects to Redpanda over TLS using the standard Kafka protocol.
No Redpanda-specific SDK required — any Kafka-compatible client works.

Usage (local):
    pip install -r requirements.txt
    export KAFKA_BROKERS=<broker-host>:9092
    export KAFKA_CA_CERT=/path/to/ca.crt
    python retail_consumer.py

Usage (in-cluster):
    kubectl apply -f ../../clusters/region-a/python-consumer.yaml
    kubectl --context rp-demo-eu-west-1 -n redpanda logs -f deploy/python-retail-consumer
"""
import json
import os
import signal
import sys
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException

BROKER  = os.environ.get("KAFKA_BROKERS", "redpanda-0.redpanda.redpanda.svc.cluster.local:9092")
TOPIC   = os.environ.get("KAFKA_TOPIC",   "retail-orders")
GROUP   = os.environ.get("KAFKA_GROUP",   "python-retail-consumer")
CA_CERT = os.environ.get("KAFKA_CA_CERT", "/etc/redpanda-certs/ca.crt")

conf = {
    "bootstrap.servers":  BROKER,
    "group.id":           GROUP,
    "auto.offset.reset":  "earliest",
    "security.protocol":  "SSL",
    "ssl.ca.location":    CA_CERT,
    "enable.auto.commit": True,
}

running = True


def shutdown(signum, frame):
    global running
    print("\n[consumer] Shutting down...", flush=True)
    running = False


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


def main():
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    print(f"[consumer] broker     : {BROKER}", flush=True)
    print(f"[consumer] topic      : {TOPIC}", flush=True)
    print(f"[consumer] group      : {GROUP}", flush=True)
    print(f"[consumer] protocol   : Kafka/TLS (confluent-kafka)", flush=True)
    print(f"[consumer] Waiting for messages...\n", flush=True)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
            key = msg.key().decode() if msg.key() else "-"
            try:
                value = json.loads(msg.value().decode("utf-8"))
                print(
                    f"[{ts}] p={msg.partition()} offset={msg.offset()} key={key}",
                    flush=True,
                )
                print(f"         {json.dumps(value)}", flush=True)
            except Exception:
                print(f"[{ts}] raw: {msg.value()}", flush=True)
    finally:
        consumer.close()
        print("[consumer] Closed.", flush=True)


if __name__ == "__main__":
    main()
