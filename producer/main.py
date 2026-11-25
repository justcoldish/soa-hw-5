import logging
import os
import random
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Literal, get_args
from uuid import UUID, uuid4

import uvicorn
from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger()
logging.getLogger("httpx").setLevel(logging.WARNING)
TOPIC = "movie-events"

registry = SchemaRegistryClient({"url": os.environ["SCHEMA_REGISTRY_URL"]})
schema = registry.register_schema_full_response(f"{TOPIC}-value", Schema(open("movie_event.avsc").read(), "AVRO"))
log.info("schema %s version %d id %d", schema.subject, schema.version, schema.schema_id)
serialize = AvroSerializer(registry, schema.schema.schema_str, conf={"auto.register.schemas": False})
producer = Producer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP"],
    "acks": "all",
    "enable.idempotence": True,
    # librdkafka удваивает паузу между ретраями от 100ms до 5s, пока не выйдут 30s
    "retry.backoff.ms": 100,
    "retry.backoff.max.ms": 5000,
    "message.timeout.ms": 30000,
})
app = FastAPI(title="Movie Service")


EventType = Literal["VIEW_STARTED", "VIEW_FINISHED", "VIEW_PAUSED", "VIEW_RESUMED", "LIKED", "SEARCHED"]
DeviceType = Literal["MOBILE", "DESKTOP", "TV", "TABLET"]


class Event(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(min_length=1)
    movie_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    device_type: DeviceType
    session_id: str = Field(min_length=1)
    progress_seconds: int = Field(0, ge=0, le=2**31 - 1)


def publish(event: Event):
    result = []
    # ключ user_id: события одного пользователя в одной партиции и по порядку
    producer.produce(TOPIC, key=event.user_id, value=serialize(event.model_dump(), SerializationContext(TOPIC, MessageField.VALUE)),
                     on_delivery=lambda err, msg: result.append(err))
    producer.flush()
    if result[0]:
        raise KafkaException(result[0])
    log.info("published %s %s %s", event.event_id, event.event_type, event.timestamp.isoformat())


@app.post("/events")
def post_event(event: Event):
    try:
        publish(event)
    except KafkaException as e:
        raise HTTPException(503, str(e))
    return {"event_id": event.event_id}


def generate():
    # первый визит за последнюю неделю, шанс вернуться падает с каждым днем
    users = {}
    for i in range(500):
        first = random.randint(0, 7)
        users[f"user-{i}"] = [first] + [first - k for k in range(1, first + 1) if random.random() < 0.5 * 0.8 ** k]
    while True:
        user, days = random.choice(list(users.items()))
        now = datetime.now(UTC)
        day = datetime.combine(now.date() - timedelta(days=random.choice(days)), datetime.min.time(), UTC)
        start = day + (min(now, day + timedelta(days=1)) - day) * random.random()
        common = dict(user_id=user, movie_id=f"movie-{int(random.paretovariate(1.2)) % 50}",
                      device_type=random.choice(get_args(DeviceType)), session_id=str(uuid4()))
        steps = [("SEARCHED", 0)] if random.random() < 0.3 else []
        steps.append(("VIEW_STARTED", 0))
        progress = 0
        while random.random() < 0.4:
            progress += random.randint(60, 1200)
            steps += [("VIEW_PAUSED", progress), ("VIEW_RESUMED", progress)]
        if random.random() < 0.7:
            progress += random.randint(600, 5400)
            steps.append(("VIEW_FINISHED", progress))
            if random.random() < 0.3:
                steps.append(("LIKED", progress))
        try:
            for i, (event_type, progress) in enumerate(steps):
                ts = min(start + timedelta(seconds=progress + 30 * i), now)
                publish(Event(event_type=event_type, timestamp=ts, progress_seconds=progress, **common))
        except KafkaException as e:
            log.error("generator: %s", e)
        time.sleep(0.2)


if os.environ.get("GENERATOR_ENABLED") == "true":
    threading.Thread(target=generate, daemon=True).start()
uvicorn.run(app, host="0.0.0.0", port=8000, access_log=False)
