import gzip
import json
from types import SimpleNamespace

import boto3
import pytest
from botocore.exceptions import ClientError
from confluent_kafka import KafkaException

from fda.consumer import (
    HEARTBEAT,
    HEARTBEAT_SECONDS,
    METRIC_NAMESPACE,
    REJECTED,
    STALE,
    TOPICS,
    WRITTEN,
    flush,
    run,
)
from fda.raw import FLUSH_SECONDS

REGION = "eu-central-1"
BUCKET = "fda-raw-test"
TIMESTAMP_MS = 1_791_158_400_000


@pytest.fixture
def aws(live_table):
    s3 = boto3.client("s3", region_name=REGION)
    s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": REGION})
    return SimpleNamespace(table=live_table, s3=s3)


def flight(flight_id, sequence=1):
    return {
        "event_id": f"{flight_id}-{sequence}",
        "flight_id": flight_id,
        "event_type": "scheduled",
        "sequence": sequence,
    }


def booking(booking_id, sequence=1):
    return {
        "event_id": f"{booking_id}-{sequence}",
        "booking_id": booking_id,
        "passenger_id": "P1",
        "sequence": sequence,
    }


def message(topic, offset, event, partition=0, error=None, raw=None):
    value = raw if raw is not None else json.dumps(event).encode()
    return SimpleNamespace(
        topic=lambda: topic,
        partition=lambda: partition,
        offset=lambda: offset,
        timestamp=lambda: (1, TIMESTAMP_MS),
        value=lambda: value,
        error=lambda: error,
    )


def bucket_keys(s3):
    response = s3.list_objects_v2(Bucket=BUCKET)
    return sorted(entry["Key"] for entry in response.get("Contents", []))


def fake_consumer(messages, s3=None, tick=lambda: None):
    consumer = SimpleNamespace(
        messages=list(messages), polls=0, commits=[], subscribed=[], closed=[]
    )

    def poll(timeout):
        consumer.polls += 1
        tick()
        return consumer.messages.pop(0) if consumer.messages else None

    def commit(asynchronous):
        assert asynchronous is False
        consumer.commits.append(bucket_keys(s3) if s3 else [])

    def subscribe(topics):
        assert isinstance(topics, list)
        consumer.subscribed.append(topics)

    consumer.poll = poll
    consumer.commit = commit
    consumer.subscribe = subscribe
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


def after_polls(consumer, count):
    return lambda: consumer.polls >= count


def failing_on_second_put(table):
    calls = []

    def put_item(**kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            error = {"Error": {"Code": "InternalServerError", "Message": "boom"}}
            raise ClientError(error, "PutItem")
        return table.put_item(**kwargs)

    return SimpleNamespace(put_item=put_item, meta=table.meta)


def metric_values(call):
    values = {}
    for datum in call["MetricData"]:
        topic = datum["Dimensions"][0]["Value"] if datum.get("Dimensions") else None
        values[(topic, datum["MetricName"])] = datum["Value"]
    return values


def beat(counts):
    values = {(None, HEARTBEAT): 1}
    for topic in TOPICS:
        values[(topic, WRITTEN)] = 0
        values[(topic, STALE)] = 0
        values[(topic, REJECTED)] = 0
    return values | counts


def test_commit_only_after_s3_objects_exist(aws):
    messages = [
        message("flights", 0, flight("F1")),
        message("flights", 1, flight("F2")),
        message("bookings", 0, booking("B1")),
    ]
    expected = [
        "raw/topic=bookings/dt=2026-10-05/0-000000000000-000000000000.jsonl.gz",
        "raw/topic=flights/dt=2026-10-05/0-000000000000-000000000001.jsonl.gz",
    ]
    clock, tick = fake_clock(FLUSH_SECONDS / 3)
    consumer = fake_consumer(messages, aws.s3, tick)
    run(consumer, aws.table, aws.s3, fake_cloudwatch(), BUCKET, until_drained(consumer), clock)
    assert consumer.commits == [expected]


def test_flush_when_buffered_bytes_reach_the_limit(aws, monkeypatch):
    messages = [message("flights", offset, flight(f"F{offset}")) for offset in range(4)]
    size = len(messages[0].value())
    monkeypatch.setattr("fda.raw.FLUSH_BYTES", 2 * size)
    clock, tick = fake_clock(0)
    consumer = fake_consumer(messages, aws.s3, tick)
    run(consumer, aws.table, aws.s3, fake_cloudwatch(), BUCKET, after_polls(consumer, 4), clock)
    assert [len(keys) for keys in consumer.commits] == [1, 2, 2]


def test_dynamodb_failure_means_no_commit(aws):
    table = failing_on_second_put(aws.table)
    messages = [message("flights", 0, flight("F1")), message("flights", 1, flight("F2"))]
    consumer = fake_consumer(messages, aws.s3)
    with pytest.raises(ClientError):
        run(consumer, table, aws.s3, fake_cloudwatch(), BUCKET, until_drained(consumer))
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
    assert consumer.subscribed == [["flights", "bookings", "tickets"]]


def test_flush_waits_a_full_interval_again(aws):
    messages = [message("flights", offset, flight(f"F{offset}")) for offset in range(4)]
    clock, tick = fake_clock(FLUSH_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    run(consumer, aws.table, aws.s3, fake_cloudwatch(), BUCKET, after_polls(consumer, 4), clock)
    assert len(consumer.commits) == 2


def test_heartbeat_waits_a_full_interval_again(aws):
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer([], aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, after_polls(consumer, 4), clock)
    assert len(cloudwatch.calls) == 2


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
    assert {
        "MetricName": "EventsWritten",
        "Dimensions": [{"Name": "Topic", "Value": "flights"}],
        "Value": 1,
    } in cloudwatch.calls[0]["MetricData"]
    assert len(cloudwatch.calls[0]["MetricData"]) == 10
    assert metric_values(cloudwatch.calls[0]) == beat({("flights", WRITTEN): 1})
    assert metric_values(cloudwatch.calls[1]) == beat({("bookings", WRITTEN): 1})


def test_duplicate_counts_as_stale(aws):
    event = flight("F1")
    messages = [message("flights", 0, event), message("flights", 1, event)]
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    expected = beat({("flights", WRITTEN): 1, ("flights", STALE): 1})
    assert metric_values(cloudwatch.calls[0]) == expected


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


def raw_lines(s3):
    lines = []
    for key in bucket_keys(s3):
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        lines += gzip.decompress(body).splitlines()
    return lines


def test_malformed_event_is_counted_kept_raw_and_skipped(aws):
    messages = [
        message("flights", 0, None, raw=b"{not json"),
        message("flights", 1, None, raw=json.dumps({"sequence": 1}).encode()),
        message("flights", 2, flight("F1")),
    ]
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 3)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    expected = beat({("flights", REJECTED): 2, ("flights", WRITTEN): 1})
    assert metric_values(cloudwatch.calls[0]) == expected
    assert len(consumer.commits) == 1
    assert raw_lines(aws.s3)[:2] == [b"{not json", b'{"sequence": 1}']


@pytest.mark.parametrize("raw", [b'{"a": 1}\n{"b": 2}', b'{"a": 1}\r{"b": 2}', None])
def test_value_with_a_line_break_or_no_value_is_rejected_and_not_kept(aws, raw):
    messages = [message("flights", 0, None), message("flights", 1, flight("F1"))]
    messages[0].value = lambda: raw
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    expected = beat({("flights", REJECTED): 1, ("flights", WRITTEN): 1})
    assert metric_values(cloudwatch.calls[0]) == expected
    assert raw_lines(aws.s3) == [json.dumps(flight("F1")).encode()]


def test_number_too_big_for_dynamodb_is_rejected_and_kept_raw(aws):
    messages = [
        message("flights", 0, flight("F1", sequence=10**40)),
        message("flights", 1, flight("F2")),
    ]
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    expected = beat({("flights", REJECTED): 1, ("flights", WRITTEN): 1})
    assert metric_values(cloudwatch.calls[0]) == expected
    assert len(raw_lines(aws.s3)) == 2


@pytest.mark.parametrize(
    "raw",
    [
        b'{"flight_id": "F9", "sequence": "9"}',
        b'{"flight_id": "F9", "sequence": null}',
        b'{"flight_id": "F9", "sequence": true}',
        pytest.param(b"[" * 5000 + b"]" * 5000, id="deep"),
    ],
)
def test_bad_sequence_or_too_deep_json_is_rejected_and_kept_raw(aws, raw):
    messages = [message("flights", 0, None, raw=raw), message("flights", 1, flight("F1"))]
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    expected = beat({("flights", REJECTED): 1, ("flights", WRITTEN): 1})
    assert metric_values(cloudwatch.calls[0]) == expected
    assert raw_lines(aws.s3) == [raw, json.dumps(flight("F1")).encode()]
    assert len(consumer.commits) == 1


def test_validation_exception_from_dynamodb_is_rejected_and_kept_raw(aws):
    def put_item(**kwargs):
        error = {"Error": {"Code": "ValidationException", "Message": "bad operand"}}
        raise ClientError(error, "PutItem")

    table = SimpleNamespace(put_item=put_item, meta=aws.table.meta)
    messages = [message("flights", 0, flight("F1")), message("flights", 1, flight("F2"))]
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    assert metric_values(cloudwatch.calls[0]) == beat({("flights", REJECTED): 2})
    assert len(raw_lines(aws.s3)) == 2
    assert len(consumer.commits) == 1


def test_event_without_an_event_id_is_rejected_and_kept_raw(aws):
    unnamed = flight("F9")
    del unnamed["event_id"]
    messages = [message("flights", 0, unnamed), message("flights", 1, flight("F1"))]
    clock, tick = fake_clock(HEARTBEAT_SECONDS / 2)
    consumer = fake_consumer(messages, aws.s3, tick)
    cloudwatch = fake_cloudwatch()
    run(consumer, aws.table, aws.s3, cloudwatch, BUCKET, until_drained(consumer), clock)
    expected = beat({("flights", REJECTED): 1, ("flights", WRITTEN): 1})
    assert metric_values(cloudwatch.calls[0]) == expected
    assert len(raw_lines(aws.s3)) == 2
