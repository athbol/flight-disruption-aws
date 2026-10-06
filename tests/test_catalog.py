import catalog
import pytest

from fda.schemas import CURATED, LONG_FIELDS


@pytest.mark.parametrize("table", ["journeys", "flights"])
def test_columns_are_curated_fields_without_partition_key(table):
    names = [column["name"] for column in catalog.columns(table)]

    assert names == [field for field in CURATED[table] if field != "flight_date"]


@pytest.mark.parametrize("table", ["journeys", "flights"])
def test_counts_are_bigint_and_the_rest_string(table):
    for column in catalog.columns(table):
        assert column["type"] == ("bigint" if column["name"] in LONG_FIELDS else "string")


def test_partition_key_is_flight_date_string():
    assert catalog.partition_keys() == [{"name": "flight_date", "type": "string"}]
