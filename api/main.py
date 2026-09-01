import datetime

from confluent_kafka import Producer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI()

producer_config = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_config)

class MovieEvent(BaseModel):
    event_id: str
    user_id: str
    movie_id: str
    movie_title: str
    genre: str
    rating: float | None = Field(default=None, ge=0.0, le=10.0)
    event_type: str
    timestamp: datetime.datetime

@app.get("/health")
async def health():
    return {"status": "ok"}

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Delivered: {msg.value().decode('utf-8')}")
        print(f"Message delivered to {msg.topic()} at partition: [{msg.partition()}]")

@app.post("/events")
async def receive_event(event: MovieEvent):
    try:
        event_bytes = event.model_dump_json().encode("utf-8")
        producer.produce("movie-events", key=str(event.movie_id),
                         value=event_bytes, callback=delivery_report)
        producer.poll(0)
        # producer.flush()
        return {
            "message": "Event received",
            "event": event
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))