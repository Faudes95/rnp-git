from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Tuple


REPO_ROOT = Path(__file__).resolve().parents[3]


@dataclass(frozen=True)
class SqlAssetSpec:
    asset_id: str
    filename: str
    description: str
    domain: str
    database_scope: str
    target_modes: Tuple[str, ...]
    auto_apply: bool = True
    manual_review_required: bool = False
    required_relations: Tuple[str, ...] = ()
    required_columns: Tuple[Tuple[str, str], ...] = ()
    verify_relations: Tuple[str, ...] = ()
    verify_columns: Tuple[Tuple[str, str], ...] = ()
    verify_indexes: Tuple[str, ...] = ()
    verify_extensions: Tuple[str, ...] = ()
    notes: Tuple[str, ...] = ()

    @property
    def path(self) -> Path:
        return REPO_ROOT / self.filename

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["path"] = str(self.path)
        return payload


SQL_ASSET_REGISTRY: Tuple[SqlAssetSpec, ...] = (
    SqlAssetSpec(
        asset_id="materialized_views",
        filename="materialized_views.sql",
        description="Materialized views de soporte para analytics clínico y agregados longitudinales.",
        domain="analytics",
        database_scope="clinical",
        target_modes=("postgres",),
        auto_apply=True,
        required_relations=("vitals",),
        required_columns=(("vitals", "patient_id"), ("vitals", "timestamp"), ("vitals", "hr")),
        verify_relations=("vitals_daily_avg",),
        verify_indexes=("idx_vitals_daily",),
        notes=(
            "Debe ejecutarse sobre PostgreSQL con tabla vitals disponible.",
            "El REFRESH puede orquestarse fuera de este script.",
        ),
    ),
    SqlAssetSpec(
        asset_id="partition_vitals",
        filename="partition_vitals.sql",
        description="Particionado de vitals para staging/producción PostgreSQL.",
        domain="hospitalizacion",
        database_scope="clinical",
        target_modes=("postgres",),
        auto_apply=False,
        manual_review_required=True,
        required_relations=("vitals",),
        required_columns=(("vitals", "timestamp"),),
        verify_relations=("vitals_2026",),
        notes=(
            "Requiere revisión manual porque puede implicar recrear o convertir la tabla base.",
            "No se autoaplica en CI; se valida en review y se habilita explícitamente en staging.",
        ),
    ),
    SqlAssetSpec(
        asset_id="migration_vector",
        filename="migration_vector.sql",
        description="Extensión pgvector y vector index para clinical_notes.",
        domain="ia",
        database_scope="clinical",
        target_modes=("postgres",),
        auto_apply=True,
        required_relations=("clinical_notes",),
        verify_columns=(("clinical_notes", "embedding_vec"),),
        verify_indexes=("idx_notes_vector",),
        verify_extensions=("vector",),
        notes=(
            "Prepara búsqueda vectorial en clinical_notes.",
            "Requiere imagen/extensión PostgreSQL compatible con pgvector.",
        ),
    ),
)


def get_sql_asset_registry() -> Tuple[SqlAssetSpec, ...]:
    return SQL_ASSET_REGISTRY


def get_sql_asset(asset_id: str) -> SqlAssetSpec:
    for spec in SQL_ASSET_REGISTRY:
        if spec.asset_id == asset_id:
            return spec
    raise KeyError(f"sql asset desconocido: {asset_id}")


def iter_sql_assets_for_scope(database_scope: str) -> Iterable[SqlAssetSpec]:
    scope = str(database_scope or "").strip().lower() or "clinical"
    for spec in SQL_ASSET_REGISTRY:
        if spec.database_scope == "both" or spec.database_scope == scope:
            yield spec


def load_sql_asset_sql(asset_id: str) -> str:
    spec = get_sql_asset(asset_id)
    return spec.path.read_text(encoding="utf-8")
