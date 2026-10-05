import json
import os
import signal
import time
from collections import Counter

import boto3
from confluent_kafka import KafkaException

from fda.live import TABLE_NAME, put_live
from fda.raw import objects, should_flush

TOPICS = ("flights", "bookings", "tickets")
GROUP_ID = "fda-consumer"
HEARTBEAT_SECONDS = 60
METRIC_NAMESPACE = "FlightDisruption"
POLL_SECONDS = 1.0


def record(msg):
    return {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp_ms": msg.timestamp()[1],
        "value": msg.value(),
    }


def handle(table, msg, buffer, counts):
    event = json.loads(msg.value())
    written = put_live(table, msg.topic(), event)
    counts[(msg.topic(), "EventsWritten" if written else "EventsStale")] += 1
    buffer.append(record(msg))


def flush(s3, bucket, consumer, buffer):
    if not buffer:
        return
    for key, body in objects(buffer):
        s3.put_object(Bucket=bucket, Key=key, Body=body)
    consumer.commit(asynchronous=False)
    buffer.clear()


def heartbeat(cloudwatch, counts):
    data = [{"MetricName": "Heartbeat", "Value": 1}]
    for (topic, metric), value in counts.items():
        dimensions = [{"Name": "Topic", "Value": topic}]
        data.append({"MetricName": metric, "Dimensions": dimensions, "Value": value})
    cloudwatch.put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=data)
    for key in counts:
        counts[key] = 0


def run(consumer, table, s3, cloudwatch, bucket, stop, clock=time.monotonic):
    consumer.subscribe(TOPICS)
    buffer = []
    counts = Counter()
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
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
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
