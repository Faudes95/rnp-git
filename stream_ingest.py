import asyncio
from ingestion_fast import bulk_insert_vitals
from models import SessionLocal


async def stream_listener(queue):
    while True:
        batch = await queue.get()
        session = SessionLocal()
        try:
            bulk_insert_vitals(session, batch)
        finally:
            session.close()
        queue.task_done()
