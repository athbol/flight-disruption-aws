import json
import os
import random
import signal
import time
import uuid
from datetime import UTC, date, datetime, timedelta

from fda.schemas import TOPICS
from fda.topics import BOOTSTRAP, create_topics

EVENT_NAMESPACE = uuid.UUID("6f1c2a9e-4b7d-4e2a-9c51-0d3f8a6b2e47")

ROUTES = (
    ("NQA", "KVB"),
    ("NQA", "RKX"),
    ("SFQ", "KVB"),
    ("NQA", "JVQ"),
    ("NQA", "SFQ"),
    ("KZQ", "NQA"),
)
FLIGHTS_PER_DAY = 24
PASSENGERS_PER_FLIGHT = 30
FIRST_FLIGHT_NO = 400

FIRST_DEPARTURE = timedelta(hours=6)
DEPARTURE_GAP = timedelta(minutes=40)
SALES_WINDOW_SECONDS = 4 * 3600
CANCELLATION_NOTICE = timedelta(hours=5)
REBOOKING_WINDOW_SECONDS = 30 * 60
DELAY_NOTICE = timedelta(hours=1)

CANCEL_RATE = 0.08
DELAY_RATE = 0.12
MIN_DELAY_MINUTES = 15
MAX_DELAY_MINUTES = 240
REBOOK_RATE = 0.70
MIN_FARE_CENTS = 5_000
MAX_FARE_CENTS = 90_000

LAG_RANGES_SECONDS = ((0, 5), (5, 30 * 60), (30 * 60, 2 * 3600), (2 * 3600, 36 * 3600))
LAG_WEIGHTS = (85, 10, 2, 3)
DUPLICATE_RATE = 0.02
HOLD_RATE = 0.30
MIN_HOLD_SECONDS = 3600
MAX_HOLD_SECONDS = 3 * 3600

CARRY_OVER_DAYS = 2
STOP_CHECK_SECONDS = 1


def midnight(day):
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def plan_day(seed: str, day: date) -> list[dict]:
    rng = random.Random(f"{seed}:{day}")
    start = midnight(day)
    cancelled_at = start + CANCELLATION_NOTICE
    flights = [(new_flight(day, number), draw_outcome(rng)) for number in range(FLIGHTS_PER_DAY)]
    cancelled = {flight["flight_id"] for flight, (outcome, _) in flights if outcome == "cancelled"}
    emissions = []
    for number, (flight, (outcome, delay)) in enumerate(flights):
        hold = draw_hold(rng) if outcome == "cancelled" else timedelta(0)
        changes = flight_changes(start, cancelled_at, flight, outcome, delay)
        emissions += entity_emissions(rng, seed, "flights", flight["flight_id"], changes, hold)
        alternatives = [
            other["flight_id"]
            for other, _ in flights
            if (other["origin"], other["destination"]) == (flight["origin"], flight["destination"])
            and other["flight_id"] not in cancelled
        ]
        moved_from = cancelled_at if outcome == "cancelled" else None
        for seat in range(PASSENGERS_PER_FLIGHT):
            passenger = number * PASSENGERS_PER_FLIGHT + seat
            emissions += passenger_emissions(
                rng, seed, day, flight, passenger, moved_from, alternatives
            )
    return sorted(emissions, key=lambda emission: emission["due"])


def new_flight(day, number):
    origin, destination = ROUTES[number % len(ROUTES)]
    flight_no = f"F{FIRST_FLIGHT_NO + number:04d}"
    departure = midnight(day) + FIRST_DEPARTURE + number * DEPARTURE_GAP
    return {
        "flight_id": f"{flight_no}-{day}",
        "flight_no": flight_no,
        "flight_date": day.isoformat(),
        "origin": origin,
        "destination": destination,
        "scheduled_departure": departure.isoformat(),
        "delay_minutes": 0,
    }


def draw_outcome(rng):
    roll = rng.random()
    if roll < CANCEL_RATE:
        return "cancelled", 0
    if roll < CANCEL_RATE + DELAY_RATE:
        return "delayed", rng.randint(MIN_DELAY_MINUTES, MAX_DELAY_MINUTES)
    return "on_time", 0


def draw_hold(rng):
    if rng.random() < HOLD_RATE:
        return timedelta(seconds=rng.randint(MIN_HOLD_SECONDS, MAX_HOLD_SECONDS))
    return timedelta(0)


def draw_lag(rng):
    low, high = rng.choices(LAG_RANGES_SECONDS, weights=LAG_WEIGHTS)[0]
    return timedelta(seconds=rng.randint(low, high))


def flight_changes(start, cancelled_at, flight, outcome, delay):
    departure = datetime.fromisoformat(flight["scheduled_departure"])
    changes = [(start, "scheduled", flight)]
    if outcome == "cancelled":
        return changes + [(cancelled_at, "cancelled", flight)]
    if outcome == "delayed":
        flight = flight | {"delay_minutes": delay}
        changes.append((departure - DELAY_NOTICE, "delayed", flight))
    return changes + [(departure + timedelta(minutes=delay), "departed", flight)]


def passenger_emissions(rng, seed, day, flight, passenger, moved_from, alternatives):
    start = midnight(day)
    suffix = f"{day:%Y%m%d}-{passenger:03d}"
    booked_at = start + timedelta(seconds=rng.randint(0, SALES_WINDOW_SECONDS))
    booking = {
        "booking_id": f"B{suffix}",
        "passenger_id": f"P{suffix}",
        "flight_id": flight["flight_id"],
        "original_flight_id": None,
    }
    ticket = {
        "ticket_id": f"T{suffix}",
        "booking_id": booking["booking_id"],
        "passenger_id": booking["passenger_id"],
        "amount_cents": rng.randint(MIN_FARE_CENTS, MAX_FARE_CENTS),
    }
    booking_changes = [(booked_at, "created", booking)]
    ticket_changes = [(booked_at, "issued", ticket)]
    if moved_from:
        moved_at = moved_from + timedelta(seconds=rng.randint(1, REBOOKING_WINDOW_SECONDS))
        if alternatives and rng.random() < REBOOK_RATE:
            rebooked = booking | {
                "flight_id": rng.choice(alternatives),
                "original_flight_id": flight["flight_id"],
            }
            booking_changes.append((moved_at, "rebooked", rebooked))
            ticket_changes.append((moved_at, "exchanged", ticket))
        else:
            booking_changes.append((moved_at, "cancelled", booking))
            ticket_changes.append((moved_at, "refunded", ticket))
    bookings = entity_emissions(rng, seed, "bookings", booking["booking_id"], booking_changes)
    tickets = entity_emissions(rng, seed, "tickets", ticket["ticket_id"], ticket_changes)
    return bookings + tickets


def entity_emissions(rng, seed, topic, entity_id, changes, hold_last=timedelta(0)):
    emissions = []
    for sequence, (event_time, event_type, state) in enumerate(changes, start=1):
        values = state | {
            "event_id": str(uuid.uuid5(EVENT_NAMESPACE, f"{seed}:{entity_id}:{sequence}")),
            "event_type": event_type,
            "event_time": event_time.isoformat(),
            "sequence": sequence,
        }
        event = {field: values[field] for field in TOPICS[topic]}
        hold = hold_last if sequence == len(changes) else timedelta(0)
        copies = 2 if rng.random() < DUPLICATE_RATE else 1
        for _ in range(copies):
            due = event_time + hold + draw_lag(rng)
            emissions.append({"topic": topic, "key": entity_id, "due": due, "event": dict(event)})
    return emissions


def check_delivery(err, _message):
    if err is not None:
        raise RuntimeError(f"delivery failed: {err}")


def wait_until(producer, moment, stopping):
    if moment > datetime.now(UTC):
        producer.flush()
    while not stopping():
        seconds = (moment - datetime.now(UTC)).total_seconds()
        if seconds <= 0:
            return
        time.sleep(min(seconds, STOP_CHECK_SECONDS))


def schedule(seed, now):
    day = now.date()
    queue = [
        emission
        for back in range(CARRY_OVER_DAYS + 1)
        for emission in plan_day(seed, day - timedelta(days=back))
        if emission["due"] >= now
    ]
    while True:
        next_midnight = midnight(day + timedelta(days=1))
        queue.sort(key=lambda emission: emission["due"])
        yield from [e for e in queue if e["due"] < next_midnight]
        queue = [e for e in queue if e["due"] >= next_midnight]
        day += timedelta(days=1)
        queue += plan_day(seed, day)


def run(producer, emissions, stopping):
    for emission in emissions:
        wait_until(producer, emission["due"], stopping)
        if stopping():
            break
        value = json.dumps(emission["event"])
        producer.produce(
            emission["topic"], key=emission["key"], value=value, on_delivery=check_delivery
        )
        producer.poll(0)
    producer.flush()


def main():
    from confluent_kafka import Producer

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", BOOTSTRAP)
    seed = os.environ.get("SEED", "fda")
    create_topics(bootstrap)
    producer = Producer({"bootstrap.servers": bootstrap})
    stopping = []
    signal.signal(signal.SIGTERM, lambda *_: stopping.append(True))
    signal.signal(signal.SIGINT, lambda *_: stopping.append(True))
    run(producer, schedule(seed, datetime.now(UTC)), lambda: bool(stopping))


if __name__ == "__main__":
    main()
