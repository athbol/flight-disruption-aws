from fda.consumer import HEARTBEAT, METRIC_NAMESPACE, STALE, TOPIC_DIMENSION, TOPICS, WRITTEN


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


def links(region):
    console = f"https://{region}.console.aws.amazon.com"
    return "\n".join(
        [
            "## Alerts (email via fda-alerts)",
            f"* [fda-heartbeat-missing]({console}/cloudwatch/home?region={region}"
            "#alarmsV2:alarm/fda-heartbeat-missing): consumer stopped sending its heartbeat",
            f"* [fda-dynamodb-throttles]({console}/cloudwatch/home?region={region}"
            "#alarmsV2:alarm/fda-dynamodb-throttles): writes to the live table throttled",
            f"* [fda-glue-failed]({console}/events/home?region={region}"
            "#/eventbus/default/rules/fda-glue-failed): Glue job failed or timed out",
            f"* [fda-curate]({console}/gluestudio/home?region={region}"
            "#/editor/job/fda-curate/runs): daily curate job runs",
        ]
    )


def dashboard(region, table):
    dynamodb = [
        ["AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", table],
        ["AWS/DynamoDB", "ThrottledRequests", "TableName", table, "Operation", "PutItem"],
    ]
    return {
        "widgets": [
            metric_widget(0, 0, "Consumer heartbeat", region, [[METRIC_NAMESPACE, HEARTBEAT]], 60),
            metric_widget(12, 0, "Events written", region, per_topic(WRITTEN), 300, stacked=True),
            metric_widget(0, 6, "Stale events skipped", region, per_topic(STALE), 300),
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
