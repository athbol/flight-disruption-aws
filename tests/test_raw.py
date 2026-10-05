import gzip
import json
from datetime import UTC, datetime, timedelta, timezone

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


def read_lines(data):
    return gzip.decompress(data).decode().split("\n")


def test_dt_changes_at_utc_midnight():
    midnight = datetime(2026, 10, 6, tzinfo=UTC)
    assert dt(ms(midnight) - 1) == "2026-10-05"
    assert dt(ms(midnight)) == "2026-10-06"


def test_dt_uses_utc_not_local_time():
    plus_two = timezone(timedelta(hours=2))
    assert dt(ms(datetime(2026, 10, 5, 23, 59, tzinfo=plus_two))) == "2026-10-05"
    assert dt(ms(datetime(2026, 10, 6, 0, 30, tzinfo=plus_two))) == "2026-10-05"


def test_key_layout_with_padded_offsets():
    assert key("flights", "2026-10-05", 0, 10, 42) == (
        "raw/topic=flights/dt=2026-10-05/0-000000000010-000000000042.jsonl.gz"
    )


def test_body_is_gzip_jsonl_of_the_values():
    records = [record("flights", 0, offset, DAY_ONE) for offset in (1, 2, 3)]
    text = gzip.decompress(body(records)).decode()
    assert text.endswith("\n")
    lines = text.split("\n")[:-1]
    assert "" not in lines
    assert [json.loads(line) for line in lines] == [json.loads(item["value"]) for item in records]


def test_objects_groups_by_topic_date_and_partition():
    records = [
        record("flights", 0, 42, DAY_ONE),
        record("flights", 0, 10, DAY_ONE),
        record("flights", 0, 17, DAY_ONE),
        record("flights", 1, 5, DAY_ONE),
        record("flights", 0, 50, DAY_TWO),
        record("bookings", 0, 4, DAY_ONE),
        record("bookings", 0, 3, DAY_ONE),
    ]
    expected = {
        "raw/topic=flights/dt=2026-10-05/0-000000000010-000000000042.jsonl.gz": [10, 17, 42],
        "raw/topic=flights/dt=2026-10-05/1-000000000005-000000000005.jsonl.gz": [5],
        "raw/topic=flights/dt=2026-10-06/0-000000000050-000000000050.jsonl.gz": [50],
        "raw/topic=bookings/dt=2026-10-05/0-000000000003-000000000004.jsonl.gz": [3, 4],
    }
    written = objects(records)
    assert len(written) == len(expected)
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
