from boto3.dynamodb.conditions import Key

TABLE_NAME = "fda-live"


def keys(topic, event):
    if topic == "flights":
        return f"FLIGHT#{event['flight_id']}", "STATE"
    if topic == "bookings":
        return f"PAX#{event['passenger_id']}", f"BOOKING#{event['booking_id']}"
    if topic == "tickets":
        return f"PAX#{event['passenger_id']}", f"TICKET#{event['ticket_id']}"
    raise ValueError(f"unknown topic: {topic}")


def item(topic, event):
    pk, sk = keys(topic, event)
    fields = {name: value for name, value in event.items() if value is not None}
    return {"pk": pk, "sk": sk} | fields


def put_live(table, topic, event):
    try:
        table.put_item(
            Item=item(topic, event),
            ConditionExpression="attribute_not_exists(pk) OR #seq < :seq",
            ExpressionAttributeNames={"#seq": "sequence"},
            ExpressionAttributeValues={":seq": event["sequence"]},
        )
    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return False
    return True


def passenger_status(table, passenger_id):
    response = table.query(KeyConditionExpression=Key("pk").eq(f"PAX#{passenger_id}"))
    return response["Items"]
