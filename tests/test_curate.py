import json
from collections import defaultdict
from datetime import date, timedelta

import pytest
from pyspark.sql import SparkSession

from fda import curate
from fda.generator import plan_day
from fda.raw import RAW_PREFIX, body, key
from fda.schemas import FLIGHTS, JOURNEYS

RUN_DATE = date(2026, 10, 6)


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def day(offset):
    return (RUN_DATE + timedelta(days=offset)).isoformat()


def flight(flight_date, status="departed", sequence=1, delay=0, number=1):
    return {
        "event_id": f"F{number}-{flight_date}-{sequence}",
        "event_type": status,
        "event_time": f"{flight_date}T06:00:00+00:00",
        "sequence": sequence,
        "flight_id": f"F{number}-{flight_date}",
        "flight_no": f"F{number}",
        "flight_date": flight_date,
        "origin": "NQA",
        "destination": "KVB",
        "scheduled_departure": f"{flight_date}T08:00:00+00:00",
        "delay_minutes": delay,
    }


def booking(booking_id, flight_id, status="created", sequence=1, original=None):
    return {
        "event_id": f"{booking_id}-{sequence}",
        "event_type": status,
        "event_time": "2026-10-01T05:00:00+00:00",
        "sequence": sequence,
        "booking_id": booking_id,
        "passenger_id": f"P{booking_id}",
        "flight_id": flight_id,
        "original_flight_id": original,
    }


def ticket(booking_id, status="issued", sequence=1, amount=10_000):
    return {
        "event_id": f"T{booking_id}-{sequence}",
        "event_type": status,
        "event_time": "2026-10-01T05:00:00+00:00",
        "sequence": sequence,
        "ticket_id": f"T{booking_id}",
        "booking_id": booking_id,
        "passenger_id": f"P{booking_id}",
        "amount_cents": amount,
    }


def journey_events(flight_date):
    flight_event = flight(flight_date)
    booking_id = f"B{flight_date}"
    return [
        ("flights", flight_date, flight_event),
        ("bookings", flight_date, booking(booking_id, flight_event["flight_id"])),
        ("tickets", flight_date, ticket(booking_id)),
    ]


def write_raw(root, events):
    files = defaultdict(list)
    for topic, dt, event in events:
        files[(topic, dt)].append({"value": json.dumps(event).encode()})
    for (topic, dt), records in files.items():
        path = root / key(topic, dt, 0, 0, len(records) - 1)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body(records))
    return str(root / RAW_PREFIX)


def frame(spark, topic, events):
    return spark.createDataFrame(events, curate.schema(topic))


def by_booking(rows):
    return {row["booking_id"]: row for row in rows}


def partitions(curated_root, name):
    return sorted(path.name for path in (curated_root / name).glob("flight_date=*"))


def test_latest_keeps_highest_sequence_once_per_entity(spark):
    events = [
        flight(day(0), "scheduled", sequence=1),
        flight(day(0), "departed", sequence=3),
        flight(day(0), "delayed", sequence=2),
        flight(day(0), "departed", sequence=3),
        flight(day(0), "scheduled", sequence=1, number=2),
    ]

    rows = curate.latest(frame(spark, "flights", events), "flight_id").collect()

    assert sorted((row["flight_id"], row["sequence"]) for row in rows) == [
        (f"F1-{day(0)}", 3),
        (f"F2-{day(0)}", 1),
    ]


def test_journeys_labels_each_disruption(spark):
    on_time = flight(day(0), "departed", number=1)
    late = flight(day(0), "departed", delay=30, number=2)
    still_delayed = flight(day(0), "delayed", delay=45, number=3)
    cancelled = flight(day(0), "cancelled", number=4)
    replacement = flight(day(0), "departed", number=5)
    flights = [on_time, late, still_delayed, cancelled, replacement]
    bookings = [
        booking("B1", on_time["flight_id"]),
        booking("B2", late["flight_id"]),
        booking("B3", still_delayed["flight_id"]),
        booking("B4", replacement["flight_id"], "rebooked", 2, cancelled["flight_id"]),
        booking("B5", cancelled["flight_id"], "cancelled", 2),
        booking("B6", still_delayed["flight_id"], "rebooked", 2, cancelled["flight_id"]),
    ]
    tickets = [
        ticket("B1"),
        ticket("B2"),
        ticket("B3"),
        ticket("B4", "exchanged", 2),
        ticket("B5", "refunded", 2, amount=25_000),
        ticket("B6", "exchanged", 2),
    ]

    result = curate.journeys(
        frame(spark, "bookings", bookings),
        frame(spark, "flights", flights),
        frame(spark, "tickets", tickets),
    )
    rows = by_booking(result.collect())

    assert result.columns == list(JOURNEYS)
    assert {booking_id: row["disruption"] for booking_id, row in rows.items()} == {
        "B1": "none",
        "B2": "delayed",
        "B3": "delayed",
        "B4": "cancelled_rebooked",
        "B5": "cancelled_refunded",
        "B6": "cancelled_rebooked",
    }
    assert rows["B4"]["flight_status"] == "departed"
    assert rows["B4"]["original_flight_status"] == "cancelled"
    assert rows["B1"]["original_flight_status"] is None
    assert rows["B5"]["original_flight_status"] is None
    assert (rows["B5"]["ticket_id"], rows["B5"]["ticket_status"]) == ("TB5", "refunded")
    assert rows["B5"]["amount_cents"] == 25_000


def test_window_keeps_lookback_dates_and_reads_late_events(spark, tmp_path):
    events = [event for offset in range(-5, 1) for event in journey_events(day(offset))]
    late_flight = f"F1-{day(-2)}"
    events += [
        ("bookings", day(-2), booking("BLATE", late_flight)),
        ("bookings", day(0), booking("BLATE", late_flight, "cancelled", 2)),
    ]
    raw_root = write_raw(tmp_path, events)
    curated_root = tmp_path / "curated"

    curate.run(spark, raw_root, str(curated_root), RUN_DATE)

    expected = [f"flight_date={day(offset)}" for offset in range(-3, 1)]
    assert partitions(curated_root, "journeys") == expected
    assert partitions(curated_root, "flights") == expected
    rows = by_booking(spark.read.parquet(str(curated_root / "journeys")).collect())
    assert str(rows["BLATE"]["flight_date"]) == day(-2)
    assert rows["BLATE"]["disruption"] == "cancelled_refunded"


def test_window_drops_flights_after_run_date(spark, tmp_path):
    tomorrow = flight(day(1))
    events = journey_events(day(0)) + [
        ("flights", day(0), tomorrow),
        ("bookings", day(0), booking("BTOMORROW", tomorrow["flight_id"])),
        ("tickets", day(0), ticket("BTOMORROW")),
    ]
    raw_root = write_raw(tmp_path, events)
    curated_root = tmp_path / "curated"

    curate.run(spark, raw_root, str(curated_root), RUN_DATE)

    assert partitions(curated_root, "journeys") == [f"flight_date={day(0)}"]
    assert partitions(curated_root, "flights") == [f"flight_date={day(0)}"]


def test_run_end_to_end_on_generated_days(spark, tmp_path):
    first = RUN_DATE - timedelta(days=1)
    emissions = plan_day("s", first) + plan_day("s", RUN_DATE)
    events = [(e["topic"], e["due"].date().isoformat(), e["event"]) for e in emissions]
    raw_root = write_raw(tmp_path, events)
    curated_root = tmp_path / "curated"
    run_date = RUN_DATE + timedelta(days=curate.LATE_DAYS)

    curate.run(spark, raw_root, str(curated_root), run_date)

    journey_rows = spark.read.parquet(str(curated_root / "journeys")).collect()
    flight_rows = spark.read.parquet(str(curated_root / "flights")).collect()
    booking_ids = {event["booking_id"] for topic, _, event in events if topic == "bookings"}
    flight_ids = {event["flight_id"] for topic, _, event in events if topic == "flights"}
    assert len(journey_rows) == len(booking_ids)
    assert sorted(row["flight_id"] for row in flight_rows) == sorted(flight_ids)
    hit = [
        row
        for row in journey_rows
        if "cancelled" in (row["flight_status"], row["original_flight_status"])
    ]
    assert hit
    assert {row["disruption"] for row in hit} <= {"cancelled_rebooked", "cancelled_refunded"}
    expected = [f"flight_date={first}", f"flight_date={RUN_DATE}"]
    assert partitions(curated_root, "journeys") == expected
    assert partitions(curated_root, "flights") == expected
    one_day = str(curated_root / "journeys" / expected[0])
    assert spark.read.parquet(one_day).columns == [c for c in JOURNEYS if c != "flight_date"]
    one_day = str(curated_root / "flights" / expected[0])
    assert spark.read.parquet(one_day).columns == [c for c in FLIGHTS if c != "flight_date"]


def test_second_run_keeps_partitions_outside_its_window(spark, tmp_path):
    events = [event for offset in range(-3, 5) for event in journey_events(day(offset))]
    raw_root = write_raw(tmp_path, events)
    curated_root = tmp_path / "curated"

    curate.run(spark, raw_root, str(curated_root), RUN_DATE)
    curate.run(spark, raw_root, str(curated_root), RUN_DATE + timedelta(days=4))

    expected = [f"flight_date={day(offset)}" for offset in range(-3, 5)]
    assert partitions(curated_root, "journeys") == expected
    assert partitions(curated_root, "flights") == expected


def test_generator_lateness_fits_late_days():
    planned_days = [RUN_DATE + timedelta(days=offset) for offset in range(3)]
    days_late = [
        (emission["due"].date() - planned_day).days
        for planned_day in planned_days
        for emission in plan_day("s", planned_day)
    ]

    assert 0 <= min(days_late)
    assert max(days_late) <= curate.LATE_DAYS <= curate.LOOKBACK_DAYS


def test_run_succeeds_with_missing_raw_days(spark, tmp_path):
    events = journey_events(day(-1)) + journey_events(day(0))
    raw_root = write_raw(tmp_path, events)
    curated_root = tmp_path / "curated"

    curate.run(spark, raw_root, str(curated_root), RUN_DATE)

    expected = [f"flight_date={day(-1)}", f"flight_date={day(0)}"]
    assert partitions(curated_root, "journeys") == expected
