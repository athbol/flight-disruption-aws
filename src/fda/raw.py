import gzip
from datetime import UTC, datetime
from itertools import groupby

RAW_PREFIX = "raw"
OFFSET_DIGITS = 12
FLUSH_RECORDS = 5000
FLUSH_SECONDS = 300


def dt(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).strftime("%Y-%m-%d")


def key(topic: str, day: str, partition: int, first_offset: int, last_offset: int) -> str:
    first = str(first_offset).zfill(OFFSET_DIGITS)
    last = str(last_offset).zfill(OFFSET_DIGITS)
    return f"{RAW_PREFIX}/topic={topic}/dt={day}/{partition}-{first}-{last}.jsonl.gz"


def body(records: list[dict]) -> bytes:
    data = b"".join(record["value"] + b"\n" for record in records)
    return gzip.compress(data, mtime=0)


def group(record: dict) -> tuple[str, str, int]:
    return record["topic"], dt(record["timestamp_ms"]), record["partition"]


def objects(records: list[dict]) -> list[tuple[str, bytes]]:
    ordered = sorted(records, key=lambda record: (group(record), record["offset"]))
    pairs = []
    for (topic, day, partition), members in groupby(ordered, key=group):
        members = list(members)
        first, last = members[0]["offset"], members[-1]["offset"]
        pairs.append((key(topic, day, partition, first, last), body(members)))
    return pairs


def should_flush(count: int, seconds_since_flush: float) -> bool:
    if count == 0:
        return False
    return count >= FLUSH_RECORDS or seconds_since_flush >= FLUSH_SECONDS
