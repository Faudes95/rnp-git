from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.services.jefatura_central_flow import (
    render_jefatura_central_home_flow,
)
from app.services.jefatura_central_insumos_flow import render_jefatura_central_insumos_flow
from app.services.jefatura_central_shared import (
    CENTRAL_MODULE,
    CENTRAL_SUBMODULES,
)
from app.services.resident_profiles_flow import (
    build_resident_card_summaries,
    build_resident_profile_viewmodel,
    resident_cards,
)


JEFATURA_UROLOGIA_MODULES: List[Dict[str, Any]] = [
    dict(CENTRAL_MODULE),
    {
        "slug": "programa-academico",
        "nombre": "Programa Académico",
        "icono": "🎓",
        "descripcion": "Planeación docente, sesiones académicas y seguimiento de cumplimiento por residente.",
        "color": "naranja",
    },
    {
        "slug": "insumos",
        "nombre": "Insumos",
        "icono": "🧰",
        "descripcion": "Control modular de inventario crítico quirúrgico y alertas de reposición.",
        "color": "rojo",
        "hidden_home": True,
    },
    {
        "slug": "anuer",
        "nombre": "ANUER",
        "icono": "🧾",
        "descripcion": "Módulo institucional para reportes administrativos y normativos de urología.",
        "color": "naranja",
    },
    {
        "slug": "distribucion-salas",
        "nombre": "Distribución de Salas",
        "icono": "🗂️",
        "descripcion": "Asignación modular de salas quirúrgicas por prioridad, turno y especialidad.",
        "color": "azul",
    },
    {
        "slug": "gobernanza",
        "href": "/gobernanza",
        "nombre": "Gobernanza",
        "icono": "🛡️",
        "descripcion": "Acceso al módulo institucional de gobernanza clínica y normativa.",
        "color": "azul",
    },
    {
        "slug": "firma-electronica",
        "href": "/firma-electronica",
        "nombre": "Firma Electrónica",
        "icono": "✍️",
        "descripcion": "Acceso directo al flujo de firma electrónica sin alterar su lógica actual.",
        "color": "rojo",
    },
]

REPORTE_JEFATURA_SUBMODULES: List[Dict[str, str]] = list(CENTRAL_SUBMODULES)


PROGRAMA_ACADEMICO_DIRECTORIO: List[Dict[str, str]] = [
    {
        "slug": "santaella",
        "nombre": "Dr. Felix Santaella",
        "cargo": "Jefe de Servicio de Urología",
        "foto": "/static/img/jefatura/santaella.png",
    },
    {
        "slug": "beltran",
        "nombre": "Dr. Edgar Beltrán",
        "cargo": "Profesor Titular Adjunto",
        "foto": "/static/img/jefatura/beltran.png",
    },
    {
        "slug": "navarro",
        "nombre": "Dr. Mario Navarro",
        "cargo": "Jefe de Residentes",
        "foto": "/static/img/jefatura/navarro.png",
    },
    {
        "slug": "torres",
        "nombre": "Dra. Lily Torres",
        "cargo": "Jefa de Residentes",
        "foto": "/static/img/jefatura/torres.png",
    },
]


PROGRAMA_ACADEMICO_SUBMODULES: List[Dict[str, str]] = [
    {
        "slug": "programa-operativo",
        "nombre": "Programa Operativo",
        "icono": "📘",
        "descripcion": "Calendario docente anual, sesiones por semana y metas de cumplimiento por generación.",
    },
    {
        "slug": "vacaciones",
        "nombre": "Vacaciones",
        "icono": "🌴",
        "descripcion": "Planeación de periodos vacacionales con cobertura segura de guardias y rotaciones.",
    },
    {
        "slug": "rol-guardias",
        "nombre": "Rol de Guardias",
        "icono": "🛎️",
        "descripcion": "Asignación modular de R5-R1 por día, turnos y respaldo operativo del servicio.",
    },
    {
        "slug": "residentes",
        "nombre": "Residentes",
        "icono": "👩‍⚕️",
        "descripcion": "Gestión longitudinal de residentes, evaluaciones y evolución académica por ciclo.",
    },
]


def _find_module(module_slug: str) -> Optional[Dict[str, Any]]:
    for module in JEFATURA_UROLOGIA_MODULES:
        if module.get("slug") == module_slug:
            return module
    return None


def _find_programa_submodule(section_slug: str) -> Optional[Dict[str, str]]:
    for section in PROGRAMA_ACADEMICO_SUBMODULES:
        if section.get("slug") == section_slug:
            return section
    return None


async def render_jefatura_urologia_home_flow(request):
    modules_home = [m for m in JEFATURA_UROLOGIA_MODULES if not m.get("hidden_home")]
    return m.render_template(
        "jefatura_urologia_home.html",
        request=request,
        modules=modules_home,
    )


async def render_jefatura_urologia_programa_academico_flow(request):
    return m.render_template(
        "jefatura_urologia_programa_academico.html",
        request=request,
        directorio=PROGRAMA_ACADEMICO_DIRECTORIO,
        submodules=PROGRAMA_ACADEMICO_SUBMODULES,
    )


async def render_jefatura_urologia_programa_submodule_flow(
    request,
    section_slug: str,
    db: Optional[Session] = None,
):
    if section_slug == "programa-operativo":
        return m.render_template(
            "jefatura_urologia_programa_operativo.html",
            request=request,
        )
    if section_slug == "residentes":
        cards = resident_cards()
        card_summaries = build_resident_card_summaries(db) if db is not None else {}
        grouped_cards: Dict[str, List[Dict[str, Any]]] = {}
        for card in cards:
            summary = card_summaries.get(card["code"]) or card_summaries.get(card["name"].upper()) or {}
            enriched = {
                **card,
                "href": f"/jefatura-urologia/programa-academico/residentes/{card['code']}",
                "summary": {
                    "total_cirugias": int(summary.get("total_cirugias") or 0),
                    "procedimientos": int(summary.get("procedimientos") or 0),
                    "ultima_fecha": summary.get("ultima_fecha"),
                    "abordaje_dominante": summary.get("abordaje_dominante") or "N/E",
                },
            }
            grouped_cards.setdefault(card["grade"], []).append(enriched)
        return m.render_template(
            "jefatura_urologia_programa_residentes.html",
            request=request,
            residents_by_grade=grouped_cards,
            resident_cards=cards,
        )

    section = _find_programa_submodule(section_slug)
    if not section:
        section = {
            "slug": "desconocido",
            "nombre": "Submódulo no encontrado",
            "icono": "⚠️",
            "descripcion": "El submódulo solicitado no existe en Programa Académico.",
        }
    return m.render_template(
        "jefatura_urologia_programa_modulo_placeholder.html",
        request=request,
        section=section,
    )


async def render_jefatura_urologia_module_flow(request, module_slug: str, db: Optional[Session] = None):
    if module_slug == "programa-academico":
        return await render_jefatura_urologia_programa_academico_flow(request)
    if module_slug in {"central", "reporte-jefatura"} and db is not None:
        return await render_jefatura_central_home_flow(request, db)
    if module_slug == "insumos" and db is not None:
        return await render_jefatura_central_insumos_flow(request, db)

    module = _find_module(module_slug)
    if not module:
        module = {
            "slug": "desconocido",
            "nombre": "Módulo no encontrado",
            "icono": "⚠️",
            "descripcion": "El enlace solicitado no existe. Vuelva al panel de Jefatura de Urología.",
            "color": "rojo",
    }
    nested_modules: List[Dict[str, str]] = []
    if module.get("slug") in {"central", "reporte-jefatura"}:
        nested_modules = REPORTE_JEFATURA_SUBMODULES
    return m.render_template(
        "jefatura_urologia_modulo_placeholder.html",
        request=request,
        module=module,
        nested_modules=nested_modules,
    )


async def render_jefatura_urologia_residente_profile_flow(
    request,
    db: Session,
    resident_code: str,
    *,
    flash: Optional[Dict[str, str]] = None,
    drawer_open: bool = False,
):
    profile = build_resident_profile_viewmodel(db, resident_code)
    resolved_flash = flash
    if resolved_flash is None and str(request.query_params.get("saved") or "") == "1":
        resolved_flash = {
            "kind": "success",
            "message": "Perfil actualizado correctamente.",
        }
    return m.render_template(
        "jefatura_urologia_residente_perfil.html",
        request=request,
        profile=profile,
        resident=profile["resident"],
        kpis=profile["kpis"],
        curva_aprendizaje=profile["curva_aprendizaje"],
        sangrado_rows=profile["sangrado_por_procedimiento"],
        charts=profile["charts"],
        flash=resolved_flash,
        drawer_open=drawer_open,
    )
