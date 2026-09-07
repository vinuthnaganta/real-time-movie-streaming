import json
import time
import uuid
from datetime import datetime
import random
from pathlib import Path

import requests

API_URL = "http://127.0.0.1:8000/events"
# EVENT_TYPE = ["view", "rate"]

# load movies.json into memory
MOVIES_FILE = Path(__file__).parent / "movies.json"
with open(MOVIES_FILE, "r", encoding="utf-8") as file:
    MOVIES = json.load(file)

def get_events_per_sec(spike=False):
    if spike:
        return random.randint(5, 10)
    return random.randint(1, 3)

def is_spike():
    # trigger a spike 10% of the time
    return random.random() < 0.10

def choose_movie(spike=False):
    # during a spike, 70% of the events are the shining
    if spike and random.random() < 0.70:
        shining_movies = [
            movie for movie in MOVIES
            if movie["Title"] == "The Shining"
        ]
        return random.choice(shining_movies)
    return random.choice(MOVIES)

def generate_event(spike=False):
    movie = choose_movie(spike)
    new_event = {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "movie_id": movie["IMDB Id"],
        "movie_title": movie["Title"],
        "genre": movie["Genre"],
        "rating": movie["Rating"],
        "event_type": "view",
        "timestamp": str(datetime.now()),
    }
    return new_event

def simulate_spike():
    spike_time = 30
    start_time = time.time()
    print("\n----Spike STARTED----\n")
    while (time.time() - start_time) < spike_time:
        events_per_sec = get_events_per_sec(spike=True)
        headers = {"Content-Type": "application/json"}
        for _ in range(events_per_sec):
            event = generate_event(spike=True)
            try:
                response = requests.post(API_URL, json=event, headers=headers)
                if response.status_code == 200:
                    print("Event sent successfully")
                    print(f"{event['movie_title']}")
                    print(response.json())
                else:
                    print("Event not sent successfully")
                    print(response.status_code)
            except requests.exceptions.ConnectionError:
                print("Connection error")
            time.sleep(1)
    print("\n----Spike ENDED----\n")

if __name__ == "__main__":
    while True:
        # start a spike occasionally
        if is_spike():
            simulate_spike()
            continue
        # otherwise, generate normal events
        events_per_sec = get_events_per_sec(spike=False)
        for _ in range(events_per_sec):
            event = generate_event(spike=False)
            headers = {"Content-Type": "application/json"}
            try:
                response = requests.post(API_URL, json=event, headers=headers)
                if response.status_code == 200:
                    print("Event sent successfully")
                    print(f"{event['movie_title']}")
                    print(response.json())
                else:
                    print("Event not sent successfully")
                    print(response.status_code)
            except requests.exceptions.ConnectionError:
                print("Connection error")
            time.sleep(1)