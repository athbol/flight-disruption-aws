from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

import pytest

from fda.generator import plan_day
from fda.schemas import TOPICS

DAY = date(2026, 10, 5)


@pytest.fixture(scope="module")
def plan():
    return plan_day("s", DAY)


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


def test_same_seed_and_date_gives_the_same_plan(plan):
    assert plan_day("s", DAY) == plan
    assert plan_day("other", DAY) != plan


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


def test_duplicates_repeat_the_same_event_about_two_percent_of_the_time(plan):
    counts = Counter(emission["event"]["event_id"] for emission in plan)
    duplicated = [event_id for event_id, count in counts.items() if count == 2]
    assert max(counts.values()) == 2
    assert 0.01 < len(duplicated) / len(counts) < 0.03
    first = {}
    for emission in plan:
        event_id = emission["event"]["event_id"]
        if event_id in first:
            assert emission["event"] == first[event_id]["event"]
            assert emission["key"] == first[event_id]["key"]
        first[event_id] = emission


def test_outcome_shares_over_many_days():
    flight_types = defaultdict(set)
    booking_outcomes = Counter()
    for offset in range(400):
        emissions = plan_day("s", DAY + timedelta(days=offset))
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


def test_rebookings_move_to_a_flying_flight_on_the_same_route_and_tickets_follow():
    emissions = [e for offset in range(10) for e in plan_day("s", DAY + timedelta(days=offset))]
    flights = final_states(emissions, "flights", "flight_id")
    bookings = final_states(emissions, "bookings", "booking_id")
    tickets = {t["booking_id"]: t for t in final_states(emissions, "tickets", "ticket_id").values()}
    rebooked = [b for b in bookings.values() if b["event_type"] == "rebooked"]
    cancelled = [b for b in bookings.values() if b["event_type"] == "cancelled"]
    assert rebooked and cancelled
    for booking in rebooked:
        original = flights[booking["original_flight_id"]]
        new = flights[booking["flight_id"]]
        assert original["event_type"] == "cancelled"
        assert new["event_type"] != "cancelled"
        assert (new["origin"], new["destination"]) == (original["origin"], original["destination"])
        assert tickets[booking["booking_id"]]["event_type"] == "exchanged"
    for booking in cancelled:
        assert tickets[booking["booking_id"]]["event_type"] == "refunded"


def test_emissions_are_sorted_and_never_more_than_two_days_late(plan):
    dues = [emission["due"] for emission in plan]
    assert dues == sorted(dues)
    lateness = [e["due"] - datetime.fromisoformat(e["event"]["event_time"]) for e in plan]
    assert min(lateness) >= timedelta(0)
    assert max(lateness) < timedelta(days=2)


def test_some_cancellations_arrive_after_their_rebookings():
    emissions = [e for offset in range(10) for e in plan_day("s", DAY + timedelta(days=offset))]
    cancelled_due = {
        e["key"]: e["due"]
        for e in emissions
        if e["topic"] == "flights" and e["event"]["event_type"] == "cancelled"
    }
    early_rebookings = [
        e
        for e in emissions
        if e["event"]["event_type"] == "rebooked"
        and e["due"] < cancelled_due[e["event"]["original_flight_id"]]
    ]
    assert early_rebookings
