import asyncio
import json
import time
from typing import Callable, Optional

from aiokafka import AIOKafkaConsumer

from ingestion_fast import bulk_insert_vitals, bulk_insert_labs
from models import SessionLocal


def _default_transform(message: dict) -> dict:
    return message


async def consume_kafka(
    topic: str,
    kind: str = "vitals",
    bootstrap_servers: str = "localhost:9092",
    group_id: str = "clinical-ingest",
    batch_size: int = 500,
    flush_seconds: int = 5,
    transform: Optional[Callable[[dict], dict]] = None,
):
    if transform is None:
        transform = _default_transform

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )

    await consumer.start()
    batch = []
    last_flush = time.time()
    try:
        async for msg in consumer:
            payload = transform(msg.value)
            batch.append(payload)
            now = time.time()
            if len(batch) >= batch_size or (now - last_flush) >= flush_seconds:
                session = SessionLocal()
                try:
                    if kind == "vitals":
                        bulk_insert_vitals(session, batch)
                    else:
                        bulk_insert_labs(session, batch)
                finally:
                    session.close()
                batch.clear()
                last_flush = now
    finally:
        await consumer.stop()


if __name__ == "__main__":
    # Ejemplo:
    # python3 kafka_ingest.py vitals_topic
    import sys

    topic = sys.argv[1] if len(sys.argv) > 1 else "vitals"
    asyncio.run(consume_kafka(topic))
