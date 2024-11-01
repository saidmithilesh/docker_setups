import json
from confluent_kafka import Producer

# Configure the producer
conf = {"bootstrap.servers": "localhost:9092", "client.id": "python-producer"}

producer = Producer(conf)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result."""
    if err is not None:
        print(f"Delivery failed for Message {msg.key()}: {err}")
    else:
        print(
            f"Message produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}"
        )


for i in range(20):
    # Produce a JSON message
    message = {"key": "value", "number": i}

    # Convert the message to a JSON string and encode it to bytes
    message_bytes = json.dumps(message).encode("utf-8")

    # Send the message
    producer.produce("test-topic", value=message_bytes, callback=delivery_report)
    print(f"Produced message: {message}")

# Flush any remaining messages in the queue
producer.flush()
