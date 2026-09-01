import json
import threading
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

# control traffic level
def get_events_per_sec(spike=False):
    if spike:
        return random.randint(5, 10)
    return random.randint(1, 3)

def is_spike():
    return random.random() < 0.10 # 10% chance of entering spike movde

def simulate_spike():
    spike_time = 30
    start = time.time()
    print("Simulating spike")

def generate_event():
    movie = random.choice(MOVIES)
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "movie_id": movie["IMDB Id"],
        "movie_title": movie["Title"],
        "genre": movie["Genre"],
        "rating": movie["Rating"],
        "event_type": "view",
        "timestamp": str(datetime.now()),
    }
    return event

def simulate_event():
    x = 0
    while x < 6:
        fake_event = generate_event()
        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(API_URL, json=fake_event, headers=headers)
            if response.status_code == 200:
                print("Event sent successfully")
                print(response.json())
            else:
                print("Event not sent successfully")
                print(response.status_code)
        except requests.exceptions.ConnectionError:
            print("Connection error")
        time.sleep(random.randint(1, 4))
        x = x + 1

if __name__ == "__main__":
    # for i in range(10):
    #     threading.Thread(target=simulate_event).start()
    for _ in range(get_events_per_sec()):
        eevent = generate_event()
        print(json.dumps(eevent, indent=2))