import gzip
import json
import time
from datetime import UTC, datetime, timedelta, timezone

import pytest

from fda.raw import body, dt, key, objects, should_flush

DAY_ONE = datetime(2026, 10, 5, 12, tzinfo=UTC)
DAY_TWO = datetime(2026, 10, 6, 12, tzinfo=UTC)


def ms(moment):
    return int(moment.timestamp() * 1000)


def record(topic, partition, offset, moment):
    value = json.dumps({"topic": topic, "partition": partition, "offset": offset})
    return {
        "topic": topic,
        "partition": partition,
        "offset": offset,
        "timestamp_ms": ms(moment),
        "value": value.encode(),
    }


@pytest.fixture
def tokyo(monkeypatch):
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def read_lines(data):
    return gzip.decompress(data).decode().split("\n")


def test_dt_changes_at_utc_midnight(tokyo):
    midnight = datetime(2026, 10, 6, tzinfo=UTC)
    assert dt(ms(midnight) - 1) == "2026-10-05"
    assert dt(ms(midnight)) == "2026-10-06"


def test_dt_uses_utc_not_local_time(tokyo):
    plus_two = timezone(timedelta(hours=2))
    assert dt(ms(datetime(2026, 10, 5, 23, 59, tzinfo=plus_two))) == "2026-10-05"
    assert dt(ms(datetime(2026, 10, 6, 0, 30, tzinfo=plus_two))) == "2026-10-05"


def test_key_layout_with_padded_offsets():
    assert key("flights", "2026-10-05", 0, 10, 42) == (
        "raw/topic=flights/dt=2026-10-05/0-000000000010-000000000042.jsonl.gz"
    )


def test_body_is_the_values_as_is_one_per_line():
    records = [record("flights", 0, offset, DAY_ONE) for offset in (1, 2, 3)]
    expected = b"".join(item["value"] + b"\n" for item in records)
    assert gzip.decompress(body(records)) == expected


def test_objects_groups_by_topic_date_and_partition():
    records = [
        record("flights", 0, 42, DAY_ONE),
        record("flights", 0, 10, DAY_ONE),
        record("flights", 0, 17, DAY_ONE),
        record("flights", 1, 15, DAY_ONE),
        record("flights", 0, 50, DAY_TWO),
        record("bookings", 0, 20, DAY_ONE),
        record("bookings", 0, 12, DAY_ONE),
    ]
    expected = {
        "raw/topic=flights/dt=2026-10-05/0-000000000010-000000000042.jsonl.gz": [10, 17, 42],
        "raw/topic=flights/dt=2026-10-05/1-000000000015-000000000015.jsonl.gz": [15],
        "raw/topic=flights/dt=2026-10-06/0-000000000050-000000000050.jsonl.gz": [50],
        "raw/topic=bookings/dt=2026-10-05/0-000000000012-000000000020.jsonl.gz": [12, 20],
    }
    written = objects(records)
    assert len(written) == len(expected)
    assert {path for path, _ in written} == set(expected)
    for path, data in written:
        offsets = [json.loads(line)["offset"] for line in read_lines(data)[:-1]]
        assert offsets == expected[path]


def test_objects_of_nothing_is_empty():
    assert objects([]) == []


def test_should_flush():
    assert should_flush(0, 1000) is False
    assert should_flush(4999, 10) is False
    assert should_flush(5000, 0) is True
    assert should_flush(1, 300) is True
    assert should_flush(1, 299) is False
