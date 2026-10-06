from fda.consumer import (
    HEARTBEAT,
    METRIC_NAMESPACE,
    REJECTED,
    STALE,
    TOPIC_DIMENSION,
    TOPICS,
    WRITTEN,
)

HEARTBEAT_ALARM = "fda-heartbeat-missing"
THROTTLE_ALARM = "fda-dynamodb-throttles"
GLUE_RULE = "fda-glue-failed"
CURATE_JOB = "fda-curate"
DYNAMODB_METRICS = ("ConsumedWriteCapacityUnits", "WriteThrottleEvents", "ReadThrottleEvents")


def metric_widget(x, y, title, region, metrics, period, stacked=False):
    return {
        "type": "metric",
        "x": x,
        "y": y,
        "width": 12,
        "height": 6,
        "properties": {
            "title": title,
            "region": region,
            "metrics": metrics,
            "stat": "Sum",
            "period": period,
            "stacked": stacked,
            "view": "timeSeries",
        },
    }


def per_topic(metric):
    return [[METRIC_NAMESPACE, metric, TOPIC_DIMENSION, topic] for topic in TOPICS]


def home(region, service):
    return f"https://{region}.console.aws.amazon.com/{service}/home?region={region}"


def links(region):
    alarms = f"{home(region, 'cloudwatch')}#alarmsV2:alarm"
    return "\n".join(
        [
            "## Alerts (email via fda-alerts)",
            f"* [{HEARTBEAT_ALARM}]({alarms}/{HEARTBEAT_ALARM}): "
            "consumer stopped sending its heartbeat",
            f"* [{THROTTLE_ALARM}]({alarms}/{THROTTLE_ALARM}): writes to the live table throttled",
            f"* [{GLUE_RULE}]({home(region, 'events')}#/eventbus/default/rules/{GLUE_RULE}): "
            "Glue job failed or timed out",
            f"* [{CURATE_JOB}]({home(region, 'gluestudio')}#/editor/job/{CURATE_JOB}/runs): "
            "daily curate job runs",
        ]
    )


def dashboard(region, table):
    dynamodb = [["AWS/DynamoDB", metric, "TableName", table] for metric in DYNAMODB_METRICS]
    return {
        "widgets": [
            metric_widget(0, 0, "Consumer heartbeat", region, [[METRIC_NAMESPACE, HEARTBEAT]], 60),
            metric_widget(12, 0, "Events written", region, per_topic(WRITTEN), 300, stacked=True),
            metric_widget(
                0,
                6,
                "Stale and rejected events",
                region,
                per_topic(STALE) + per_topic(REJECTED),
                300,
            ),
            metric_widget(12, 6, f"DynamoDB {table}", region, dynamodb, 300),
            {
                "type": "text",
                "x": 0,
                "y": 12,
                "width": 12,
                "height": 6,
                "properties": {"markdown": links(region)},
            },
        ]
    }
