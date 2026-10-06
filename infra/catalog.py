from fda.schemas import CURATED

PARTITION_KEY = "flight_date"
BIGINT_FIELDS = ("delay_minutes", "amount_cents")


def columns(table):
    return [
        {"name": field, "type": "bigint" if field in BIGINT_FIELDS else "string"}
        for field in CURATED[table]
        if field != PARTITION_KEY
    ]


def partition_keys():
    return [{"name": PARTITION_KEY, "type": "string"}]
