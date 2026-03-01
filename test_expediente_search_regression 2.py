from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import Session, declarative_base

from app.services.expediente_flow import _name_compat_expr, _nss_compat_expr


Base = declarative_base()


class _PacienteDemo(Base):
    __tablename__ = "pacientes_demo_busqueda"
    id = Column(Integer, primary_key=True, autoincrement=True)
    nss = Column(String(32), nullable=True)
    nombre = Column(String(255), nullable=True)


def _build_engine():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as db:
        db.add_all(
            [
                _PacienteDemo(nss="12345678901", nombre="Juan Perez"),
                _PacienteDemo(nss="0987-6543-21", nombre="maria gomez"),
            ]
        )
        db.commit()
    return engine


def test_nss_compat_search_matches_legacy_and_10_digits():
    engine = _build_engine()
    with Session(engine) as db:
        rows = (
            db.query(_PacienteDemo)
            .filter(_nss_compat_expr(_PacienteDemo.nss, "1234567890"))
            .order_by(_PacienteDemo.id.asc())
            .all()
        )
        assert [r.id for r in rows] == [1]


def test_name_compat_search_is_case_insensitive():
    engine = _build_engine()
    with Session(engine) as db:
        rows = (
            db.query(_PacienteDemo)
            .filter(_name_compat_expr(_PacienteDemo.nombre, "MARIA"))
            .order_by(_PacienteDemo.id.asc())
            .all()
        )
        assert [r.id for r in rows] == [2]

