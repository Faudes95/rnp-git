from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.core.app_context import main_proxy as m


JEFATURA_UROLOGIA_MODULES: List[Dict[str, Any]] = [
    {
        "slug": "reporte-jefatura",
        "nombre": "Reporte de Jefatura",
        "icono": "📈",
        "descripcion": "Indicadores estratégicos de productividad, tiempos y resultados clínicos del servicio.",
        "color": "azul",
    },
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

REPORTE_JEFATURA_SUBMODULES: List[Dict[str, str]] = [
    {
        "slug": "insumos",
        "nombre": "Insumos",
        "icono": "🧰",
        "descripcion": "Control modular de inventario crítico quirúrgico y alertas de reposición.",
        "href": "/jefatura-urologia/insumos",
        "color": "rojo",
    }
]


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


async def render_jefatura_urologia_programa_submodule_flow(request, section_slug: str):
    if section_slug == "programa-operativo":
        return m.render_template(
            "jefatura_urologia_programa_operativo.html",
            request=request,
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


async def render_jefatura_urologia_module_flow(request, module_slug: str):
    if module_slug == "programa-academico":
        return await render_jefatura_urologia_programa_academico_flow(request)

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
    if module.get("slug") == "reporte-jefatura":
        nested_modules = REPORTE_JEFATURA_SUBMODULES
    return m.render_template(
        "jefatura_urologia_modulo_placeholder.html",
        request=request,
        module=module,
        nested_modules=nested_modules,
    )
