import json

from confluent_kafka import Consumer

consumer_config = {
    "bootstrap.servers":"localhost:9092",
    # identifies a group of consumers
    "group.id":"movie-tracker",
    "auto.offset.reset":"earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["movie-events"])

print("Consumer is running and subscribed to the movie-events topic")

def process_event(event):
    print(f"Received movie #{event['movie_id']}: \"{event['movie_title']}\"."
          f"\n Genre: {event['genre']}, Rating: {event['rating']}."
          f"\n Event Type: {event['event_type']}, Timestamp: {event['timestamp']}.")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error: ", msg.error())
            continue

        # success, read the msg + process
        value = msg.value().decode("utf-8")
        # transform bytes -> string -> json/dictionary
        event_raw = json.loads(value)
        process_event(event_raw)
except KeyboardInterrupt:
    print("\nStopping consumer")
finally:
    consumer.close()