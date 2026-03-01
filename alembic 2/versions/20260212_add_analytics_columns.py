"""add analytics columns and high volume tables

Revision ID: 20260212_add_analytics
Revises: 
Create Date: 2026-02-12
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "20260212_add_analytics"
down_revision = None
branch_labels = None
depends_on = None


def _json_type():
    bind = op.get_bind()
    if bind.dialect.name == "postgresql":
        return postgresql.JSONB()
    return sa.JSON()


def column_exists(table_name: str, column_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    columns = [col["name"] for col in inspector.get_columns(table_name)]
    return column_name in columns


def add_column_if_missing(table_name: str, column: sa.Column):
    if not column_exists(table_name, column.name):
        op.add_column(table_name, column)


def index_exists(table_name: str, index_name: str) -> bool:
    bind = op.get_bind()
    inspector = inspect(bind)
    indexes = inspector.get_indexes(table_name)
    return any(idx.get("name") == index_name for idx in indexes)


def upgrade():
    bind = op.get_bind()
    inspector = inspect(bind)

    # Create high-volume tables if missing
    if not inspector.has_table("vitals"):
        op.create_table(
            "vitals",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("consulta_id", sa.Integer, sa.ForeignKey("consultas.id", ondelete="CASCADE"), nullable=True, index=True),
            sa.Column("patient_id", sa.String(50), index=True),
            sa.Column("timestamp", sa.DateTime, index=True),
            sa.Column("hr", sa.Float),
            sa.Column("sbp", sa.Float),
            sa.Column("dbp", sa.Float),
            sa.Column("temp", sa.Float),
            sa.Column("peso", sa.Float),
            sa.Column("talla", sa.Float),
            sa.Column("imc", sa.Float),
            sa.Column("source", sa.String(100)),
        )

    if not inspector.has_table("labs"):
        op.create_table(
            "labs",
            sa.Column("id", sa.Integer, primary_key=True, index=True),
            sa.Column("consulta_id", sa.Integer, sa.ForeignKey("consultas.id", ondelete="CASCADE"), nullable=True, index=True),
            sa.Column("patient_id", sa.String(50), index=True),
            sa.Column("timestamp", sa.DateTime, index=True),
            sa.Column("test_code", sa.String(100)),
            sa.Column("test_name", sa.String(200)),
            sa.Column("value", sa.String(200)),
            sa.Column("unit", sa.String(50)),
            sa.Column("source", sa.String(100)),
        )

    # Add new columns to consultas
    consulta_columns = [
        sa.Column("ahf_status", sa.String(20)),
        sa.Column("app_complicaciones", sa.String(20)),
        sa.Column("hosp_previas", sa.String(20)),
        sa.Column("tabaquismo_status", sa.String(20)),
        sa.Column("cigarros_dia", sa.Integer),
        sa.Column("anios_fumando", sa.Integer),
        sa.Column("transfusiones_status", sa.String(20)),
        sa.Column("aqx_complicaciones_status", sa.String(20)),
        sa.Column("vejiga_coagulos_tipo", sa.String(50)),
        sa.Column("evento_clinico", sa.String(100)),
        sa.Column("fecha_evento", sa.Date),
        sa.Column("embedding_diagnostico", _json_type()),
        sa.Column("nota_soap_auto", sa.Text),
        sa.Column("inconsistencias", sa.Text),

        # Protocolos riñón
        sa.Column("rinon_tiempo", sa.String(100)),
        sa.Column("rinon_tnm", sa.String(100)),
        sa.Column("rinon_etapa", sa.String(100)),
        sa.Column("rinon_ecog", sa.String(50)),
        sa.Column("rinon_charlson", sa.String(100)),
        sa.Column("rinon_nefrectomia", sa.String(100)),
        sa.Column("rinon_rhp", sa.String(100)),
        sa.Column("rinon_sistemico", sa.String(200)),

        # Protocolos UTUC
        sa.Column("utuc_tiempo", sa.String(100)),
        sa.Column("utuc_tnm", sa.String(100)),
        sa.Column("utuc_tx_quirurgico", sa.String(100)),
        sa.Column("utuc_rhp", sa.String(100)),
        sa.Column("utuc_sistemico", sa.String(200)),

        # Protocolos vejiga
        sa.Column("vejiga_tnm", sa.String(100)),
        sa.Column("vejiga_ecog", sa.String(50)),
        sa.Column("vejiga_hematuria_tipo", sa.String(50)),
        sa.Column("vejiga_hematuria_coagulos", sa.String(50)),
        sa.Column("vejiga_hematuria_transfusion", sa.String(50)),
        sa.Column("vejiga_procedimiento_qx", sa.String(200)),
        sa.Column("vejiga_via", sa.String(50)),
        sa.Column("vejiga_rhp", sa.String(100)),
        sa.Column("vejiga_cistoscopias_previas", sa.Text),
        sa.Column("vejiga_quimio_intravesical", sa.String(100)),
        sa.Column("vejiga_esquema", sa.String(200)),
        sa.Column("vejiga_sistemico", sa.String(200)),

        # Protocolos próstata
        sa.Column("pros_ape_pre", sa.Float),
        sa.Column("pros_ape_act", sa.Float),
        sa.Column("pros_ecog", sa.String(50)),
        sa.Column("pros_rmn", sa.String(100)),
        sa.Column("pros_historial_ape", sa.Text),
        sa.Column("pros_tr", sa.String(100)),
        sa.Column("pros_briganti", sa.String(100)),
        sa.Column("pros_gleason", sa.String(100)),
        sa.Column("pros_tnm", sa.String(100)),
        sa.Column("pros_riesgo", sa.String(100)),
        sa.Column("pros_adt_previo", sa.String(200)),
        sa.Column("pros_prostatectomia", sa.String(100)),
        sa.Column("pros_rhp", sa.String(100)),
        sa.Column("pros_radioterapia", sa.String(200)),
        sa.Column("pros_continencia", sa.String(100)),
        sa.Column("pros_ereccion", sa.String(100)),

        # Protocolos pene
        sa.Column("pene_tiempo_ecog", sa.String(100)),
        sa.Column("pene_tnm", sa.String(100)),
        sa.Column("pene_tx_quirurgico", sa.String(100)),
        sa.Column("pene_rhp", sa.String(100)),
        sa.Column("pene_sistemico", sa.String(200)),

        # Protocolos testículo
        sa.Column("testiculo_tiempo_ecog", sa.String(100)),
        sa.Column("testiculo_tnm", sa.String(100)),
        sa.Column("testiculo_orquiectomia_fecha", sa.Date),
        sa.Column("testiculo_marcadores_pre", sa.String(200)),
        sa.Column("testiculo_marcadores_post", sa.String(200)),
        sa.Column("testiculo_rhp", sa.String(100)),
        sa.Column("testiculo_historial_marcadores", sa.Text),

        # Protocolos suprarrenal
        sa.Column("suprarrenal_ecog_metanefrinas", sa.String(200)),
        sa.Column("suprarrenal_aldosterona_cortisol", sa.String(200)),
        sa.Column("suprarrenal_tnm", sa.String(100)),
        sa.Column("suprarrenal_tamano", sa.String(100)),
        sa.Column("suprarrenal_cirugia", sa.String(100)),
        sa.Column("suprarrenal_rhp", sa.String(100)),

        # Tumor incierto
        sa.Column("incierto_ape_densidad", sa.String(200)),
        sa.Column("incierto_tr", sa.String(100)),
        sa.Column("incierto_rmn", sa.String(100)),
        sa.Column("incierto_velocidad_ape", sa.String(100)),
        sa.Column("incierto_necesidad_btr", sa.String(100)),

        # Litiasis
        sa.Column("lit_tamano", sa.Float),
        sa.Column("lit_localizacion", sa.String(100)),
        sa.Column("lit_densidad_uh", sa.Float),
        sa.Column("lit_estatus_postop", sa.String(100)),
        sa.Column("lit_unidad_metabolica", sa.String(100)),
        sa.Column("lit_guys_score", sa.String(50)),
        sa.Column("lit_croes_score", sa.String(50)),

        # HPB
        sa.Column("hpb_tamano_prostata", sa.String(100)),
        sa.Column("hpb_ape", sa.String(100)),
        sa.Column("hpb_ipss", sa.String(100)),
        sa.Column("hpb_tamsulosina", sa.String(200)),
        sa.Column("hpb_finasteride", sa.String(200)),

        # Otros / subsecuente
        sa.Column("otro_detalles", sa.Text),
        sa.Column("subsecuente_subjetivo", sa.Text),
        sa.Column("subsecuente_objetivo", sa.Text),
        sa.Column("subsecuente_analisis", sa.Text),
        sa.Column("subsecuente_plan", sa.Text),
        sa.Column("subsecuente_rhp_actualizar", sa.String(200)),
    ]

    for column in consulta_columns:
        add_column_if_missing("consultas", column)

    if bind.dialect.name == "postgresql":
        if not index_exists("consultas", "ix_consultas_protocolo_detalles"):
            op.create_index(
                "ix_consultas_protocolo_detalles",
                "consultas",
                ["protocolo_detalles"],
                postgresql_using="gin",
            )


def downgrade():
    # Downgrade intentionally minimal to avoid data loss in clinical records.
    pass
