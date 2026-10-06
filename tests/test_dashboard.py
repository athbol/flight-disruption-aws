import dashboard

from fda.consumer import HEARTBEAT, METRIC_NAMESPACE, STALE, TOPIC_DIMENSION, TOPICS, WRITTEN

REGION = "eu-central-1"
TABLE = "fda-live"


def widgets(kind):
    return [w for w in dashboard.dashboard(REGION, TABLE)["widgets"] if w["type"] == kind]


def metrics(widget):
    return [tuple(metric) for metric in widget["properties"]["metrics"]]


def test_four_metric_widgets_and_one_text_widget():
    assert len(widgets("metric")) == 4
    assert len(widgets("text")) == 1
    assert len(dashboard.dashboard(REGION, TABLE)["widgets"]) == 5


def test_widgets_sit_in_two_columns():
    for widget in dashboard.dashboard(REGION, TABLE)["widgets"]:
        assert widget["x"] in (0, 12)
        assert widget["width"] == 12


def test_every_metric_widget_names_the_region():
    for widget in widgets("metric"):
        assert widget["properties"]["region"] == REGION


def test_consumer_widgets_use_the_consumer_metric_names():
    heartbeat, written, stale, _ = widgets("metric")
    assert metrics(heartbeat) == [(METRIC_NAMESPACE, HEARTBEAT)]
    assert metrics(written) == [(METRIC_NAMESPACE, WRITTEN, TOPIC_DIMENSION, t) for t in TOPICS]
    assert metrics(stale) == [(METRIC_NAMESPACE, STALE, TOPIC_DIMENSION, t) for t in TOPICS]


def test_heartbeat_is_per_minute_and_events_per_five_minutes_with_written_stacked():
    heartbeat, written, stale, _ = widgets("metric")
    assert heartbeat["properties"]["period"] == 60
    assert written["properties"]["period"] == stale["properties"]["period"] == 300
    assert written["properties"]["stacked"] is True


def test_dynamodb_widget_shows_writes_and_throttles_for_the_table():
    *_, dynamodb = widgets("metric")
    assert metrics(dynamodb) == [
        ("AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", TABLE),
        ("AWS/DynamoDB", "WriteThrottleEvents", "TableName", TABLE),
        ("AWS/DynamoDB", "ReadThrottleEvents", "TableName", TABLE),
    ]


def test_text_widget_links_the_alarms_and_the_glue_job_in_the_region():
    [text] = widgets("text")
    markdown = text["properties"]["markdown"]
    for name in (
        "fda-heartbeat-missing",
        "fda-dynamodb-throttles",
        "fda-glue-failed",
        "fda-curate",
    ):
        assert name in markdown
    assert f"region={REGION}" in markdown
