from kafka import KafkaConsumer

def get_kafka_consumer(topic_name, broker_url, group_id=None):
    try:
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=broker_url,
            auto_offset_reset='earliest',
            group_id=group_id
        )
        return consumer
    except Exception as e:
        print(f"Failed to create Kafka consumer: {e}")
        raise  # Re-raise the exception to handle it further up the call stack