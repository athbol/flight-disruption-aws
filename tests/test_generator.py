from collections import Counter, defaultdict
from datetime import UTC, date, datetime, timedelta
from itertools import takewhile
from types import SimpleNamespace

import pytest

from fda import generator
from fda.generator import CARRY_OVER_DAYS, STOP_CHECK_SECONDS, midnight, plan_day, run, schedule
from fda.schemas import TOPICS

DAY = date(2026, 10, 5)
FIXTURE_DAYS = 10
MANY_DAYS = 400

EVENT_TYPES = {
    "flights": {"scheduled", "delayed", "cancelled", "departed"},
    "bookings": {"created", "rebooked", "cancelled"},
    "tickets": {"issued", "exchanged", "refunded"},
}


@pytest.fixture(scope="module")
def days():
    return [plan_day("s", DAY + timedelta(days=offset)) for offset in range(MANY_DAYS)]


@pytest.fixture(scope="module")
def plan(days):
    return [emission for day in days[:FIXTURE_DAYS] for emission in day]


def distinct_events(emissions):
    seen = {}
    for emission in emissions:
        seen[emission["event"]["event_id"]] = emission
    return list(seen.values())


def final_states(emissions, topic, id_field):
    latest = {}
    for emission in emissions:
        event = emission["event"]
        if emission["topic"] != topic:
            continue
        current = latest.get(event[id_field])
        if current is None or event["sequence"] > current["sequence"]:
            latest[event[id_field]] = event
    return latest


def test_same_seed_and_date_gives_the_same_plan(days):
    assert plan_day("s", DAY) == days[0]
    assert plan_day("other", DAY) != days[0]


def test_fixture_covers_every_event_type_of_every_topic(plan):
    seen = defaultdict(set)
    for emission in plan:
        seen[emission["topic"]].add(emission["event"]["event_type"])
    assert seen == EVENT_TYPES


def test_every_event_has_exactly_the_fields_of_its_topic(plan):
    for emission in plan:
        assert set(emission["event"]) == set(TOPICS[emission["topic"]])


def test_sequence_counts_up_from_one_and_event_time_never_goes_back(plan):
    by_entity = defaultdict(list)
    for emission in distinct_events(plan):
        by_entity[(emission["topic"], emission["key"])].append(emission["event"])
    for events in by_entity.values():
        events.sort(key=lambda event: event["sequence"])
        assert [event["sequence"] for event in events] == list(range(1, len(events) + 1))
        times = [datetime.fromisoformat(event["event_time"]) for event in events]
        assert times == sorted(times)


def test_duplicates_repeat_the_same_event(plan):
    counts = Counter(emission["event"]["event_id"] for emission in plan)
    assert max(counts.values()) == 2
    first = {}
    for emission in plan:
        event_id = emission["event"]["event_id"]
        if event_id in first:
            assert emission["event"] == first[event_id]["event"]
            assert emission["event"] is not first[event_id]["event"]
            assert emission["key"] == first[event_id]["key"]
        first[event_id] = emission


def test_about_two_percent_of_events_are_duplicated_over_many_days(days):
    counts = Counter(emission["event"]["event_id"] for day in days for emission in day)
    duplicated = sum(count == 2 for count in counts.values())
    assert 0.015 < duplicated / len(counts) < 0.025


def test_outcome_shares_over_many_days(days):
    flight_types = defaultdict(set)
    booking_outcomes = Counter()
    for emissions in days:
        for emission in emissions:
            if emission["topic"] == "flights":
                flight_types[emission["key"]].add(emission["event"]["event_type"])
        for booking in final_states(emissions, "bookings", "booking_id").values():
            booking_outcomes[booking["event_type"]] += 1
    flights = len(flight_types)
    cancelled = sum("cancelled" in types for types in flight_types.values())
    delayed = sum("delayed" in types for types in flight_types.values())
    assert cancelled / flights == pytest.approx(0.08, abs=0.015)
    assert delayed / flights == pytest.approx(0.12, abs=0.015)
    disrupted = booking_outcomes["rebooked"] + booking_outcomes["cancelled"]
    assert booking_outcomes["rebooked"] / disrupted == pytest.approx(0.70, abs=0.05)


def test_rebookings_move_to_a_later_flying_flight_on_the_same_route_and_tickets_follow(plan):
    flights = final_states(plan, "flights", "flight_id")
    bookings = final_states(plan, "bookings", "booking_id")
    tickets = {t["booking_id"]: t for t in final_states(plan, "tickets", "ticket_id").values()}
    rebooked = [b for b in bookings.values() if b["event_type"] == "rebooked"]
    cancelled = [b for b in bookings.values() if b["event_type"] == "cancelled"]
    assert rebooked and cancelled
    for booking in rebooked:
        original = flights[booking["original_flight_id"]]
        new = flights[booking["flight_id"]]
        assert original["event_type"] == "cancelled"
        assert new["event_type"] != "cancelled"
        assert (new["origin"], new["destination"]) == (original["origin"], original["destination"])
        assert new["flight_date"] == original["flight_date"]
        departure = datetime.fromisoformat(new["scheduled_departure"])
        assert departure > datetime.fromisoformat(booking["event_time"])
        assert tickets[booking["booking_id"]]["event_type"] == "exchanged"
    for booking in cancelled:
        assert tickets[booking["booking_id"]]["event_type"] == "refunded"
    for booking in bookings.values():
        if booking["event_type"] != "rebooked":
            assert booking["original_flight_id"] is None


def test_emissions_are_sorted_and_never_more_than_two_days_late(days):
    for emissions in days[:FIXTURE_DAYS]:
        dues = [emission["due"] for emission in emissions]
        assert dues == sorted(dues)
    plan = [emission for emissions in days[:FIXTURE_DAYS] for emission in emissions]
    lateness = [e["due"] - datetime.fromisoformat(e["event"]["event_time"]) for e in plan]
    assert min(lateness) >= timedelta(0)
    assert max(lateness) < timedelta(days=2)


def test_every_emission_is_due_within_the_carry_over_window(days):
    for offset, emissions in enumerate(days):
        last_midnight = midnight(DAY + timedelta(days=offset + CARRY_OVER_DAYS + 1))
        assert max(emission["due"] for emission in emissions) < last_midnight


def test_about_thirty_percent_of_flight_cancellations_are_held_back(days):
    held = [
        e["due"] - datetime.fromisoformat(e["event"]["event_time"]) >= timedelta(hours=1)
        for emissions in days
        for e in emissions
        if e["topic"] == "flights" and e["event"]["event_type"] == "cancelled"
    ]
    assert 0.25 < sum(held) / len(held) < 0.40


def fake_producer():
    producer = SimpleNamespace(calls=[])
    producer.produce = lambda topic, key, value, on_delivery: producer.calls.append(topic)
    producer.poll = lambda timeout: None
    producer.flush = lambda: producer.calls.append("flush")
    return producer


def emission(due):
    return {"due": due, "topic": "flights", "key": "F0400", "event": {}}


def test_stop_ends_the_loop_and_flushes():
    producer = fake_producer()
    past = datetime.now(UTC) - timedelta(minutes=1)
    run(producer, [emission(past), emission(past)], lambda: "flights" in producer.calls)
    assert producer.calls == ["flights", "flush"]


def test_stop_during_a_wait_ends_the_loop_without_producing(monkeypatch):
    producer = fake_producer()
    sleeps = []
    monkeypatch.setattr(generator.time, "sleep", sleeps.append)
    future = datetime.now(UTC) + timedelta(hours=1)
    run(producer, [emission(future)], lambda: bool(sleeps))
    assert sleeps == [STOP_CHECK_SECONDS]
    assert producer.calls == ["flush", "flush"]


def test_schedule_is_in_due_order_across_midnight():
    now = datetime.now(UTC)
    horizon = midnight(now.date() + timedelta(days=2))
    dues = [e["due"] for e in takewhile(lambda e: e["due"] < horizon, schedule("fda", now))]
    assert dues == sorted(dues)
    assert dues[0] >= now
    assert dues[-1] >= midnight(now.date() + timedelta(days=1))
