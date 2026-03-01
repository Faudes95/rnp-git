"""phase2 additive surgical guardrails

Revision ID: 20260218_phase2_surgical_additive
Revises:
Create Date: 2026-02-18
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect


revision = "20260218_phase2_surgical_additive"
down_revision = None
branch_labels = None
depends_on = None


def _insp():
    return inspect(op.get_bind())


def _table_exists(table_name: str) -> bool:
    return _insp().has_table(table_name)


def _column_exists(table_name: str, column_name: str) -> bool:
    try:
        cols = [c.get("name") for c in _insp().get_columns(table_name)]
        return column_name in cols
    except Exception:
        return False


def _add_col_if_missing(table_name: str, column: sa.Column) -> None:
    if _table_exists(table_name) and not _column_exists(table_name, column.name):
        op.add_column(table_name, column)


def upgrade():
    # Programación quirúrgica: campos para diferimientos/hemoderivados (aditivo).
    _add_col_if_missing("surgical_programaciones", sa.Column("cancelacion_codigo", sa.String(40), nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("cancelacion_categoria", sa.String(120), nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("cancelacion_concepto", sa.String(255), nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("cancelacion_detalle", sa.Text, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("cancelacion_fecha", sa.DateTime, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("cancelacion_usuario", sa.String(120), nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("solicita_hemoderivados", sa.String(20), nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("hemoderivados_pg_solicitados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("hemoderivados_pfc_solicitados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("hemoderivados_cp_solicitados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("uso_hemoderivados", sa.String(20), nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("hemoderivados_pg_utilizados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("hemoderivados_pfc_utilizados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("hemoderivados_cp_utilizados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("cateter_jj_colocado", sa.String(20), nullable=True))
    _add_col_if_missing("surgical_programaciones", sa.Column("fecha_colocacion_jj", sa.Date, nullable=True))

    # Postquirúrgica: reforzar tracking de sangrado/transfusión aditivo.
    _add_col_if_missing("surgical_postquirurgicas", sa.Column("uso_hemoderivados", sa.String(20), nullable=True))
    _add_col_if_missing("surgical_postquirurgicas", sa.Column("hemoderivados_pg_utilizados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_postquirurgicas", sa.Column("hemoderivados_pfc_utilizados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_postquirurgicas", sa.Column("hemoderivados_cp_utilizados", sa.Integer, nullable=True))
    _add_col_if_missing("surgical_postquirurgicas", sa.Column("cateter_jj_colocado", sa.String(20), nullable=True))
    _add_col_if_missing("surgical_postquirurgicas", sa.Column("fecha_colocacion_jj", sa.Date, nullable=True))


def downgrade():
    # Downgrade conservador para no perder datos clínicos.
    pass

