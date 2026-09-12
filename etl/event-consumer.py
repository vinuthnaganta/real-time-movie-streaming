import json
from datetime import datetime, timedelta
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

MOVIES_FILE = Path(__file__).parent / "movies.json"
with open(MOVIES_FILE, "r", encoding="utf-8") as file:
    MOVIES = json.load(file)

movie_ids = {movie["movie_id"] for movie in MOVIES}

consumer_config = {
    "bootstrap.servers":"localhost:9092",
    # identifies a group of consumers
    "group.id":"movie-tracker",
    "auto.offset.reset":"earliest"
}

consumer = Consumer(consumer_config)
consumer.subscribe(["movie-events"])
print("Consumer is running and subscribed to the movie-events topic.")

def transform_event(eevent):
    # normalize the fields for analytics
    timestamp = datetime.fromisoformat(eevent["timestamp"])
    genre = eevent["genre"].strip().lower()
    return {
        "movie_id": eevent["movie_id"],
        "movie_title": eevent["movie_title"],
        "genre": genre,
        "rating": eevent["rating"],
        "timestamp": timestamp,
        "event_min": timestamp - timedelta(seconds=timestamp.second, minutes=timestamp.minute),
        "event_date": timestamp.date(),
        "event_type": eevent["event_type"],
    }

def process_event(raw_event):
    try:
        # transform json/string -> dict
        new_event = json.loads(raw_event)
        # validate events
        if new_event['movie_id'] not in movie_ids:
            raise ValueError(f"Movie ID {new_event['movie_id']} could not be found.")
        if new_event['event_type'] not in ['view']:
            raise ValueError(f"Event Type {new_event['event_type']} is not supported.")
        try:
            timestamp = datetime.fromisoformat(new_event["timestamp"])
        except (TypeError, ValueError):
            raise ValueError(f"Timestamp {new_event['timestamp']} is not valid.")
        if not isinstance(new_event['rating'], (int, float)) or not 0 <= new_event['rating'] <= 10:
            raise ValueError(f"Rating {new_event['rating']} is not valid.")

        print(f"Received movie #{new_event['movie_id']}: \"{new_event['movie_title']}\"."
          f"\n Genre: {new_event['genre']}, Rating: {new_event['rating']}."
          f"\n Event Type: {new_event['event_type']}, Timestamp: {new_event['timestamp']}.")
        return new_event
    except UnicodeDecodeError:
        print("Skipped message with invalid UTF-8 characters.")
    except json.JSONDecodeError:
        print("Skipped malformed JSON message")
    except KeyError as e:
        print(f"Skipping message due to missing field {e}")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print(f"Reached end of {msg.topic()}'s partition {msg.partition()} at offset {msg.offset()}.")
                continue
            print(f"Kafka Error: {msg.error()}")
            continue
        # transform bytes -> string
        event = msg.value().decode("utf-8")
        valid_event = process_event(event)

        if valid_event is not None:
            transformed_event = transform_event(valid_event)
            print(transformed_event)
except KeyboardInterrupt:
    print("\nStopping consumer")
finally:
    consumer.close()