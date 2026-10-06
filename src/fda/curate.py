from datetime import UTC, date, datetime, timedelta

from pyspark.errors import AnalysisException
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, when
from pyspark.sql.types import LongType, StringType, StructField, StructType

from fda.schemas import FLIGHTS, JOURNEYS, LONG_FIELDS, PARTITION_KEY, TOPICS

LOOKBACK_DAYS = 3
LATE_DAYS = 2


def parse_run_date(value: str) -> date:
    return datetime.now(UTC).date() if value == "today" else date.fromisoformat(value)


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
    fields = schema(topic)
    try:
        return (
            spark.read.schema(fields)
            .option("basePath", raw_root)
            .json(f"{raw_root}/topic={topic}/dt={{{','.join(days)}}}")
        )
    except AnalysisException as error:
        if error.getErrorClass() != "PATH_NOT_FOUND":
            raise
        return spark.createDataFrame([], fields)


def latest(df, key: str):
    newest_first = Window.partitionBy(key).orderBy(col("sequence").desc())
    return (
        df.dropna(subset=[key, "sequence"])
        .dropDuplicates(["event_id"])
        .withColumn("rank", row_number().over(newest_first))
        .filter(col("rank") == 1)
        .drop("rank")
    )


def flights_table(flights):
    return flights.withColumnRenamed("event_type", "flight_status").select(*FLIGHTS)


def disruption():
    delayed = (col("flight_status") == "delayed") | (
        (col("flight_status") == "departed") & (col("delay_minutes") > 0)
    )
    return (
        when(col("booking_status") == "rebooked", "cancelled_rebooked")
        .when(col("booking_status") == "cancelled", "cancelled_refunded")
        .when(delayed, "delayed")
        .otherwise("none")
    )


def journeys(bookings, flights, tickets):
    flight = flights_table(flights)
    original = flight.select(
        col("flight_id").alias("original_flight_id"),
        col("flight_status").alias("original_flight_status"),
    )
    ticket = tickets.select(
        "booking_id", "ticket_id", col("event_type").alias("ticket_status"), "amount_cents"
    )
    return (
        bookings.withColumnRenamed("event_type", "booking_status")
        .join(flight, "flight_id", "left")
        .join(original, "original_flight_id", "left")
        .join(ticket, "booking_id", "left")
        .withColumn("disruption", disruption())
        .select(*JOURNEYS)
    )


def in_window(df, days: list[str]):
    return df.filter(col(PARTITION_KEY).between(days[0], days[-1]))


def write(df, curated_root: str, name: str):
    (
        df.write.mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(PARTITION_KEY)
        .parquet(f"{curated_root}/{name}")
    )


def run(spark, raw_root: str, curated_root: str, run_date: date):
    days = days_back(run_date, LOOKBACK_DAYS)
    flights = latest(read_topic(spark, raw_root, "flights", days), "flight_id")
    bookings = latest(read_topic(spark, raw_root, "bookings", days), "booking_id")
    tickets = latest(read_topic(spark, raw_root, "tickets", days), "ticket_id")
    tables = {"journeys": journeys(bookings, flights, tickets), "flights": flights_table(flights)}
    for name, table in tables.items():
        write(in_window(table, days), curated_root, name)
