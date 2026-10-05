import pytest

from fda.schemas import CURATED, ENVELOPE, TOPICS

ALL_TUPLES = {f"topic_{name}": fields for name, fields in TOPICS.items()} | {
    f"curated_{name}": fields for name, fields in CURATED.items()
}


def test_topics_are_flights_bookings_tickets():
    assert set(TOPICS) == {"flights", "bookings", "tickets"}


def test_every_topic_starts_with_envelope():
    for fields in TOPICS.values():
        assert fields[: len(ENVELOPE)] == ENVELOPE


def test_curated_tables_are_journeys_and_flights():
    assert set(CURATED) == {"journeys", "flights"}


@pytest.mark.parametrize("fields", ALL_TUPLES.values(), ids=ALL_TUPLES)
def test_no_duplicate_fields(fields):
    assert len(fields) == len(set(fields))
