import random
from types import SimpleNamespace

import boto3
import pytest
from botocore.exceptions import ClientError

from fda.live import TABLE_NAME, item, passenger_status, put_live

REGION = "eu-central-1"


def flight(sequence, delay_minutes=None):
    return {
        "event_id": f"e-{sequence}",
        "event_type": "delayed" if delay_minutes else "scheduled",
        "event_time": f"2026-10-05T0{sequence}:00:00+00:00",
        "sequence": sequence,
        "flight_id": "GL400-2026-10-05",
        "flight_no": "GL400",
        "flight_date": "2026-10-05",
        "origin": "NQA",
        "destination": "KVB",
        "scheduled_departure": "2026-10-05T06:00:00+00:00",
        "delay_minutes": delay_minutes,
    }


def booking(passenger_id, booking_id, sequence=1):
    return {
        "event_id": f"b-{booking_id}-{sequence}",
        "event_type": "created",
        "event_time": "2026-10-05T01:00:00+00:00",
        "sequence": sequence,
        "booking_id": booking_id,
        "passenger_id": passenger_id,
        "flight_id": "GL400-2026-10-05",
        "original_flight_id": None,
    }


def ticket(passenger_id, ticket_id, booking_id, sequence=1):
    return {
        "event_id": f"t-{ticket_id}-{sequence}",
        "event_type": "issued",
        "event_time": "2026-10-05T01:00:05+00:00",
        "sequence": sequence,
        "ticket_id": ticket_id,
        "booking_id": booking_id,
        "passenger_id": passenger_id,
        "amount_cents": 12_000,
    }


def stored(table, pk, sk):
    return table.get_item(Key={"pk": pk, "sk": sk})["Item"]


def test_item_keys_flight():
    result = item("flights", flight(1))
    assert (result["pk"], result["sk"]) == ("FLIGHT#GL400-2026-10-05", "STATE")


def test_item_keys_booking():
    result = item("bookings", booking("P1", "B1"))
    assert (result["pk"], result["sk"]) == ("PAX#P1", "BOOKING#B1")


def test_item_keys_ticket():
    result = item("tickets", ticket("P1", "T1", "B1"))
    assert (result["pk"], result["sk"]) == ("PAX#P1", "TICKET#T1")


def test_item_keeps_every_field_and_drops_none():
    event = booking("P1", "B1")
    result = item("bookings", event)
    assert "original_flight_id" not in result
    assert {k: v for k, v in event.items() if v is not None}.items() <= result.items()


def test_payload_cannot_override_the_keys():
    event = flight(1) | {"pk": "PAX#P9", "sk": "BOOKING#B9"}
    result = item("flights", event)
    assert (result["pk"], result["sk"]) == ("FLIGHT#GL400-2026-10-05", "STATE")


def test_item_drops_fields_outside_the_schema():
    result = item("tickets", ticket("P1", "T1", "B1") | {"note": "x" * 1000})
    assert "note" not in result


def test_sequence_stored_as_number(live_table):
    put_live(live_table, "flights", flight(3))
    assert stored(live_table, "FLIGHT#GL400-2026-10-05", "STATE")["sequence"] == 3
    raw = boto3.client("dynamodb", region_name=REGION).get_item(
        TableName=TABLE_NAME, Key={"pk": {"S": "FLIGHT#GL400-2026-10-05"}, "sk": {"S": "STATE"}}
    )
    assert raw["Item"]["sequence"] == {"N": "3"}


def test_newer_overwrites(live_table):
    assert put_live(live_table, "flights", flight(1))
    assert put_live(live_table, "flights", flight(2, delay_minutes=45))
    result = stored(live_table, "FLIGHT#GL400-2026-10-05", "STATE")
    assert (result["sequence"], result["event_type"], result["delay_minutes"]) == (2, "delayed", 45)


def test_older_rejected(live_table):
    put_live(live_table, "flights", flight(2, delay_minutes=45))
    assert put_live(live_table, "flights", flight(1)) is False
    assert stored(live_table, "FLIGHT#GL400-2026-10-05", "STATE")["sequence"] == 2


def test_duplicate_rejected(live_table):
    assert put_live(live_table, "flights", flight(1))
    assert put_live(live_table, "flights", flight(1)) is False


def test_shuffled_order_converges(live_table):
    events = [flight(sequence, delay_minutes=sequence * 10) for sequence in range(1, 6)]
    random.Random(0).shuffle(events)
    written = [put_live(live_table, "flights", event) for event in events]
    new_maxima = sum(
        event["sequence"] > max([0] + [e["sequence"] for e in events[:position]])
        for position, event in enumerate(events)
    )
    result = stored(live_table, "FLIGHT#GL400-2026-10-05", "STATE")
    assert (result["sequence"], result["delay_minutes"]) == (5, 50)
    assert sum(written) == new_maxima


def test_passenger_status_returns_only_that_passenger(live_table):
    put_live(live_table, "bookings", booking("P1", "B1"))
    put_live(live_table, "tickets", ticket("P1", "T1", "B1"))
    put_live(live_table, "bookings", booking("P2", "B2"))
    put_live(live_table, "tickets", ticket("P2", "T2", "B2"))
    put_live(live_table, "flights", flight(1))
    keys = {(entry["pk"], entry["sk"]) for entry in passenger_status(live_table, "P1")}
    assert keys == {("PAX#P1", "BOOKING#B1"), ("PAX#P1", "TICKET#T1")}


def test_other_errors_propagate(dynamodb):
    missing = dynamodb.Table("does-not-exist")
    with pytest.raises(ClientError) as error:
        put_live(missing, "flights", flight(1))
    assert error.value.response["Error"]["Code"] == "ResourceNotFoundException"


@pytest.mark.parametrize("sequence", ["9", None, True, 1.5, [1], {"n": 1}])
def test_sequence_that_is_not_an_int_is_a_type_error(live_table, sequence):
    with pytest.raises(TypeError):
        put_live(live_table, "flights", flight(sequence))
    assert live_table.scan()["Items"] == []


def test_validation_error_from_dynamodb_is_a_value_error(live_table):
    def put_item(**kwargs):
        error = {"Error": {"Code": "ValidationException", "Message": "bad operand"}}
        raise ClientError(error, "PutItem")

    table = SimpleNamespace(put_item=put_item, meta=live_table.meta)
    with pytest.raises(ValueError):
        put_live(table, "flights", flight(1))


@pytest.mark.parametrize("change", [{"event_id": None}, {"event_id": ""}])
def test_event_without_an_event_id_is_a_value_error(live_table, change):
    with pytest.raises(ValueError):
        put_live(live_table, "flights", flight(1) | change)
    assert live_table.scan()["Items"] == []


def test_event_missing_the_event_id_field_is_a_value_error(live_table):
    event = flight(1)
    del event["event_id"]
    with pytest.raises(ValueError):
        put_live(live_table, "flights", event)
    assert live_table.scan()["Items"] == []
