import json
import os
import signal
import time
from collections import Counter

import boto3
from confluent_kafka import KafkaException

from fda import schemas
from fda.live import TABLE_NAME, put_live
from fda.raw import objects, should_flush
from fda.topics import BOOTSTRAP, create_topics

TOPICS = tuple(schemas.TOPICS)
GROUP_ID = "fda-consumer"
HEARTBEAT_SECONDS = 60
METRIC_NAMESPACE = "FlightDisruption"
POLL_SECONDS = 1.0
HEARTBEAT = "Heartbeat"
WRITTEN = "EventsWritten"
STALE = "EventsStale"
REJECTED = "EventsRejected"
TOPIC_DIMENSION = "Topic"


def record(msg):
    return {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp_ms": msg.timestamp()[1],
        "value": msg.value(),
    }


def handle(table, msg, buffer, counts):
    value = msg.value()
    if value is None or b"\n" in value or b"\r" in value:
        counts[(msg.topic(), REJECTED)] += 1
        return
    try:
        written = put_live(table, msg.topic(), json.loads(value))
        counts[(msg.topic(), WRITTEN if written else STALE)] += 1
    except (ValueError, KeyError, TypeError, ArithmeticError):
        counts[(msg.topic(), REJECTED)] += 1
    buffer.append(record(msg))


def flush(s3, bucket, consumer, buffer):
    if not buffer:
        return
    for key, body in objects(buffer):
        s3.put_object(Bucket=bucket, Key=key, Body=body)
    consumer.commit(asynchronous=False)
    buffer.clear()


def heartbeat(cloudwatch, counts):
    data = [{"MetricName": HEARTBEAT, "Value": 1}]
    for (topic, metric), value in counts.items():
        dimensions = [{"Name": TOPIC_DIMENSION, "Value": topic}]
        data.append({"MetricName": metric, "Dimensions": dimensions, "Value": value})
    cloudwatch.put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=data)
    for key in counts:
        counts[key] = 0


def run(consumer, table, s3, cloudwatch, bucket, stop, clock=time.monotonic):
    consumer.subscribe(list(TOPICS))
    buffer = []
    counts = Counter(
        {(topic, metric): 0 for topic in TOPICS for metric in (WRITTEN, STALE, REJECTED)}
    )
    last_flush = last_beat = clock()
    while not stop():
        msg = consumer.poll(POLL_SECONDS)
        if msg is not None:
            if msg.error() is not None:
                raise KafkaException(msg.error())
            handle(table, msg, buffer, counts)
        if should_flush(len(buffer), clock() - last_flush):
            flush(s3, bucket, consumer, buffer)
            last_flush = clock()
        if clock() - last_beat >= HEARTBEAT_SECONDS:
            heartbeat(cloudwatch, counts)
            last_beat = clock()
    flush(s3, bucket, consumer, buffer)
    consumer.close()


def main():
    from confluent_kafka import Consumer

    bucket = os.environ["RAW_BUCKET"]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", BOOTSTRAP)
    create_topics(bootstrap)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    table = boto3.resource("dynamodb").Table(TABLE_NAME)
    s3 = boto3.client("s3")
    cloudwatch = boto3.client("cloudwatch")
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(True))
    run(consumer, table, s3, cloudwatch, bucket, lambda: bool(stopping))


if __name__ == "__main__":
    main()
