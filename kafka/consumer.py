import time
from confluent_kafka import Consumer, KafkaException

# Configure the consumer
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "python-consumer",
    "auto.offset.reset": "earliest",
}

consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe(["test-topic"])

print("Consuming messages from 'test-topic'...")


def process_message(message):
    # Simulate message processing (e.g., parsing, database operations)
    print(f"Processing message: {message.value()}")
    time.sleep(1)
    print(f"Message processed: {message.value()}")
    # data = json.loads(message.value().decode("utf-8"))
    # Add your processing logic here
    # Simulate a processing delay if needed
    # time.sleep(1)


try:
    while True:
        msg = consumer.poll(1.0)  # Timeout after 1 second
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            process_message(msg)
            # Message received successfully
            # message = json.loads(msg.value().decode("utf-8"))
            # print(f"Received message: {message}")
except KeyboardInterrupt:
    pass
finally:
    # Close down consumer to commit final offsets.
    consumer.close()
