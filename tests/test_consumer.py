import json
from types import SimpleNamespace

import boto3
import pytest
from botocore.exceptions import ClientError
from confluent_kafka import KafkaException
from moto import mock_aws

from fda.consumer import HEARTBEAT_SECONDS, METRIC_NAMESPACE, flush, record, run
from fda.live import TABLE_NAME
from fda.raw import FLUSH_SECONDS, objects

REGION = "eu-central-1"
BUCKET = "fda-raw-test"
TIMESTAMP_MS = 1_791_158_400_000


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    with mock_aws():
        dynamodb = boto3.resource("dynamodb", region_name=REGION)
        table = dynamodb.create_table(
            TableName=TABLE_NAME,
            KeySchema=[
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        s3 = boto3.client("s3", region_name=REGION)
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
        yield SimpleNamespace(dynamodb=dynamodb, table=table, s3=s3)


def flight(flight_id, sequence=1):
    return {"flight_id": flight_id, "event_type": "scheduled", "sequence": sequence}


def booking(booking_id, sequence=1):
    return {"booking_id": booking_id, "passenger_id": "P1", "sequence": sequence}


def message(topic, offset, event, partition=0, error=None):
    return SimpleNamespace(
        topic=lambda: topic,
        partition=lambda: partition,
        offset=lambda: offset,
        timestamp=lambda: (1, TIMESTAMP_MS),
        value=lambda: json.dumps(event).encode(),
        error=lambda: error,
    )


def bucket_keys(s3):
    response = s3.list_objects_v2(Bucket=BUCKET)
    return sorted(entry["Key"] for entry in response.get("Contents", []))


def fake_consumer(messages, s3=None, tick=lambda: None):
    consumer = SimpleNamespace(messages=list(messages), commits=[], subscribed=[], closed=[])

    def poll(timeout):
        tick()
        return consumer.messages.pop(0) if consumer.messages else None

    def commit(asynchronous):
        consumer.commits.append(bucket_keys(s3) if s3 else [])

    consumer.poll = poll
    consumer.commit = commit
    consumer.subscribe = consumer.subscribed.append
    consumer.close = lambda: consumer.closed.append(True)
    return consumer


def fake_cloudwatch():
    cloudwatch = SimpleNamespace(calls=[])
    cloudwatch.put_metric_data = lambda **kwargs: cloudwatch.calls.append(kwargs)
    return cloudwatch


def fake_clock(step):
    now = [0.0]

    def tick():
        now[0] += step

    return (lambda: now[0]), tick


def until_drained(consumer):
    return lambda: not consumer.messages


def metric_values(call):
    values = {}
    for datum in call["MetricData"]:
        topic = datum["Dimensions"][0]["Value"] if datum.get("Dimensions") else None
        values[(topic, datum["MetricName"])] = datum["Value"]
    return values


def test_commit_only_after_s3_objects_exist(aws):
    messages = [
        message("flights", 0, flight("F1")),
        message("flights", 1, flight("F2")),
        message("bookings", 0, booking("B1")),
    ]
    expected = sorted(key for key, _ in objects([record(m) for m in messages]))
    clock, tick = fake_clock(FLUSH_SECONDS / 3)
    consumer = fake_consumer(messages, aws.s3, tick)
    run(consumer, aws.table, aws.s3, fake_cloudwatch(), BUCKET, until_drained(consumer), clock)
    assert consumer.commits == [expected]
    assert len(expected) == 2


def test_dynamodb_failure_means_no_commit(aws):
    missing = aws.dynamodb.Table("does-not-exist")
    consumer = fake_consumer([message("flights", 0, flight("F1"))], aws.s3)
    with pytest.raises(ClientError):
        run(consumer, missing, aws.s3, fake_cloudwatch(), BUCKET, until_drained(consumer))
    assert consumer.commits == []
    assert bucket_keys(aws.s3) == []


def test_stop_flushes_and_commits(aws):
    messages = [message("flights", 0, flight("F1")), message("flights", 1, flight("F2"))]
    clock, _ = fake_clock(0)
    consumer = fake_consumer(messages, aws.s3)
    run(consumer, aws.table, aws.s3, fake_cloudwatch(), BUCKET, until_drained(consumer), clock)
    assert len(consumer.commits) == 1
    assert len(bucket_keys(aws.s3)) == 1
    assert consumer.closed == [True]
    assert consumer.subscribed == [("flights", "bookings", "tickets")]


def test_clock_drives_flush_and_heartbeat(aws):
    messages = [message("flights", 0, flight("F1")), message("bookings", 0, booking("B1"))]
    clock, tick = fake_clock(FLUSH_SECONDS)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    assert [len(keys) for keys in consumer.commits] == [1, 2]
    assert FLUSH_SECONDS >= HEARTBEAT_SECONDS
    assert len(cloudwatch.calls) == 2
    assert cloudwatch.calls[0]["Namespace"] == METRIC_NAMESPACE
    assert metric_values(cloudwatch.calls[0]) == {
        (None, "Heartbeat"): 1,
        ("flights", "EventsWritten"): 1,
    }
    assert metric_values(cloudwatch.calls[1]) == {
        (None, "Heartbeat"): 1,
        ("flights", "EventsWritten"): 0,
        ("bookings", "EventsWritten"): 1,
    }


def test_duplicate_counts_as_stale(aws):
    event = flight("F1")
    messages = [message("flights", 0, event), message("flights", 1, event)]
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    assert metric_values(cloudwatch.calls[0]) == {
        (None, "Heartbeat"): 1,
        ("flights", "EventsWritten"): 1,
        ("flights", "EventsStale"): 1,
    }


def test_empty_flush_is_a_no_op():
    puts = []
    s3 = SimpleNamespace(put_object=lambda **kwargs: puts.append(kwargs))
    consumer = fake_consumer([])
    flush(s3, BUCKET, consumer, [])
    assert puts == []
    assert consumer.commits == []


def test_message_error_raises(aws):
    consumer = fake_consumer([message("flights", 0, flight("F1"), error="broker down")])
    with pytest.raises(KafkaException):
        run(consumer, aws.table, aws.s3, fake_cloudwatch(), BUCKET, until_drained(consumer))
