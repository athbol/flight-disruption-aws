from fda.schemas import TOPICS

BOOTSTRAP = "kafka:9092"


def create_topics(bootstrap):
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    futures = admin.create_topics([NewTopic(topic, num_partitions=1) for topic in TOPICS])
    for future in futures.values():
        try:
            future.result()
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
