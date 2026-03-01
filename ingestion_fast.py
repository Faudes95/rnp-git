from sqlalchemy.orm import Session
from models import VitalORM, LabORM


def bulk_insert_vitals(session: Session, rows: list[dict]):
    session.bulk_insert_mappings(VitalORM, rows)
    session.commit()


def bulk_insert_labs(session: Session, rows: list[dict]):
    session.bulk_insert_mappings(LabORM, rows)
    session.commit()
