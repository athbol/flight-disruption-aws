from datetime import date, timedelta

from pyspark.sql.functions import col
from pyspark.sql.types import LongType, StringType, StructField, StructType

from fda.schemas import TOPICS

LOOKBACK_DAYS = 3
LATE_DAYS = 2
LONG_FIELDS = ("sequence", "delay_minutes", "amount_cents")


def schema(topic: str) -> StructType:
    return StructType(
        [
            StructField(field, LongType() if field in LONG_FIELDS else StringType())
            for field in TOPICS[topic]
        ]
    )


def days_back(run_date: date, count: int) -> list[str]:
    return [(run_date - timedelta(days=back)).isoformat() for back in range(count, -1, -1)]


def read_topic(spark, raw_root: str, topic: str, days: list[str]):
    return (
        spark.read.schema(schema(topic))
        .option("basePath", raw_root)
        .json(f"{raw_root}/topic={topic}")
        .filter(col("dt").isin(days))
    )
