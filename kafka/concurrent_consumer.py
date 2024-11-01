import time
from confluent_kafka import Consumer, KafkaException
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_message(message):
    # Simulate message processing (e.g., parsing, database operations)
    print(f"Processing message: {message.value()}")
    time.sleep(1)
    print(f"Message processed: {message.value()}")
    # data = json.loads(message.value().decode("utf-8"))
    # Add your processing logic here
    # Simulate a processing delay if needed
    # time.sleep(1)


# Configure the consumer
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "python-concurrent-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # We'll commit manually after processing
}

consumer = Consumer(conf)
consumer.subscribe(["test-topic"])

# Initialize ThreadPoolExecutor with max_workers=10
executor = ThreadPoolExecutor(max_workers=10)

print("Consuming messages from 'test-topic' with concurrency...")

try:
    futures = []
    while True:
        msg = consumer.poll(1.0)  # Timeout after 1 second
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        else:
            # Submit the message to the executor for processing
            future = executor.submit(process_message, msg)
            futures.append(future)

            # Control concurrency
            if len(futures) >= 10:
                # Wait for the futures to complete
                for completed_future in as_completed(futures):
                    try:
                        completed_future.result()
                    except Exception as e:
                        print(f"Error processing message: {e}")
                # Commit offsets after processing
                consumer.commit()
                # Clear the list of futures
                futures = []
except KeyboardInterrupt:
    pass
finally:
    # Wait for any remaining futures to complete
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"Error processing message: {e}")
    # Commit any remaining offsets
    consumer.commit()
    # Close the executor and consumer
    executor.shutdown(wait=True)
    consumer.close()
