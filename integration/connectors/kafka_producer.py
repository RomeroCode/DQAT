from kafka import KafkaProducer

def get_kafka_producer(broker_url):
    try:
        producer = KafkaProducer(bootstrap_servers=broker_url)
        return producer
    except Exception as e:
        print(f"Failed to create Kafka producer: {e}")
        raise  # Re-raise the exception to handle it further up the call stack