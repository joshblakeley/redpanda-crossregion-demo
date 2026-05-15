#!/usr/bin/env python3
"""
IoT events AMQP consumer — pika (AMQP 0.9.1).

Consumes from RabbitMQ queue fed by the Redpanda Connect AMQP bridge.
Messages originated as MQTT in eu-west-1, bridged to Kafka, replicated
by ShadowLink to eu-central-1, then forwarded to RabbitMQ via AMQP.

Full chain:
  MQTT (eu-west-1) → Redpanda Connect → iot-events topic
    → ShadowLink → iot-events shadow (eu-central-1)
    → Redpanda Connect → RabbitMQ → this consumer

Usage (local):
    pip install -r requirements.txt
    export AMQP_URL=amqp://guest:guest@<rabbitmq-host>:5672/
    python amqp_consumer.py

Usage (in-cluster):
    kubectl apply -f ../../clusters/region-b/amqp-bridge/amqp-consumer.yaml
    kubectl --context rp-demo-eu-central-1 -n redpanda logs -f deploy/amqp-iot-consumer
"""
import json
import os
import signal
import sys
from datetime import datetime, timezone

import pika

AMQP_URL  = os.environ.get("AMQP_URL",   "amqp://guest:guest@rabbitmq.redpanda.svc.cluster.local:5672/")
EXCHANGE  = os.environ.get("AMQP_EXCHANGE", "iot-events")
QUEUE     = os.environ.get("AMQP_QUEUE",    "iot-events-consumer")
ROUTING_KEY = os.environ.get("AMQP_ROUTING_KEY", "iot-events")


def shutdown(signum, frame):
    print("\n[amqp-consumer] Shutting down...", flush=True)
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)


def on_message(channel, method, properties, body):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    key = method.routing_key
    try:
        value = json.loads(body.decode("utf-8"))
        print(f"[{ts}] routing_key={key}", flush=True)
        print(f"         {json.dumps(value)}", flush=True)
    except Exception:
        print(f"[{ts}] raw: {body}", flush=True)
    channel.basic_ack(delivery_tag=method.delivery_tag)


def main():
    params = pika.URLParameters(AMQP_URL)
    params.heartbeat = 60

    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # Declare exchange and queue (idempotent — safe to re-run)
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="direct", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)
    channel.basic_qos(prefetch_count=10)
    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)

    print(f"[amqp-consumer] broker    : {AMQP_URL}", flush=True)
    print(f"[amqp-consumer] exchange  : {EXCHANGE} (direct)", flush=True)
    print(f"[amqp-consumer] queue     : {QUEUE}", flush=True)
    print(f"[amqp-consumer] protocol  : AMQP 0.9.1 (pika)", flush=True)
    print(f"[amqp-consumer] Waiting for messages...\n", flush=True)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    finally:
        connection.close()
        print("[amqp-consumer] Closed.", flush=True)


if __name__ == "__main__":
    main()
