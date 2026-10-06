from boto3.dynamodb.conditions import Key

from fda import schemas

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
    fields = {name: event[name] for name in schemas.TOPICS[topic] if event.get(name) is not None}
    return fields | {"pk": pk, "sk": sk}


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
