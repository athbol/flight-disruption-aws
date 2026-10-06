from fda.schemas import CURATED, LONG_FIELDS, PARTITION_KEY


def columns(table):
    return [
        {"name": field, "type": "bigint" if field in LONG_FIELDS else "string"}
        for field in CURATED[table]
        if field != PARTITION_KEY
    ]


def partition_keys():
    return [{"name": PARTITION_KEY, "type": "string"}]
