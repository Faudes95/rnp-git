import asyncio
import json
import time
from typing import Callable, Optional

from asyncio_mqtt import Client

from ingestion_fast import bulk_insert_vitals, bulk_insert_labs
from models import SessionLocal


def _default_transform(message: dict) -> dict:
    return message


async def consume_mqtt(
    topic: str,
    kind: str = "vitals",
    host: str = "localhost",
    port: int = 1883,
    username: Optional[str] = None,
    password: Optional[str] = None,
    batch_size: int = 500,
    flush_seconds: int = 5,
    transform: Optional[Callable[[dict], dict]] = None,
):
    if transform is None:
        transform = _default_transform

    batch = []
    last_flush = time.time()

    async with Client(hostname=host, port=port, username=username, password=password) as client:
        await client.subscribe(topic)
        async with client.messages() as messages:
            async for message in messages:
                payload = json.loads(message.payload.decode("utf-8"))
                batch.append(transform(payload))
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


if __name__ == "__main__":
    # Ejemplo:
    # python3 mqtt_ingest.py vitals/topic
    import sys

    topic = sys.argv[1] if len(sys.argv) > 1 else "vitals"
    asyncio.run(consume_mqtt(topic))
