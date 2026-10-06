LONG_FIELDS = ("sequence", "delay_minutes", "amount_cents")
PARTITION_KEY = "flight_date"

ENVELOPE = ("event_id", "event_type", "event_time", "sequence")

FLIGHT = ENVELOPE + (
    "flight_id",
    "flight_no",
    "flight_date",
    "origin",
    "destination",
    "scheduled_departure",
    "delay_minutes",
)
BOOKING = ENVELOPE + ("booking_id", "passenger_id", "flight_id", "original_flight_id")
TICKET = ENVELOPE + ("ticket_id", "booking_id", "passenger_id", "amount_cents")

TOPICS = {"flights": FLIGHT, "bookings": BOOKING, "tickets": TICKET}

JOURNEYS = (
    "passenger_id",
    "booking_id",
    "flight_id",
    "flight_no",
    "flight_date",
    "origin",
    "destination",
    "scheduled_departure",
    "delay_minutes",
    "flight_status",
    "booking_status",
    "original_flight_id",
    "original_flight_status",
    "ticket_id",
    "ticket_status",
    "amount_cents",
    "disruption",
)
FLIGHTS = FLIGHT[len(ENVELOPE) :] + ("flight_status",)

CURATED = {"journeys": JOURNEYS, "flights": FLIGHTS}
