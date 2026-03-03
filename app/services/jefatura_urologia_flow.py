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
    },
    {
        "slug": "proveedores",
        "nombre": "Proveedores",
        "icono": "🏭",
        "descripcion": "Gestión de convenios, tiempos de entrega y trazabilidad de abastecimiento.",
        "color": "azul",
    },
    {
        "slug": "anuer",
        "nombre": "ANUER",
        "icono": "🧾",
        "descripcion": "Módulo institucional para reportes administrativos y normativos de urología.",
        "color": "naranja",
    },
    {
        "slug": "vacaciones",
        "nombre": "Vacaciones",
        "icono": "🏖️",
        "descripcion": "Planeación anual de ausencias y cobertura operativa del servicio.",
        "color": "rojo",
    },
    {
        "slug": "distribucion-salas",
        "nombre": "Distribución de Salas",
        "icono": "🗂️",
        "descripcion": "Asignación modular de salas quirúrgicas por prioridad, turno y especialidad.",
        "color": "azul",
    },
    {
        "slug": "contaduria",
        "nombre": "Contaduría",
        "icono": "💼",
        "descripcion": "Concentrado financiero y auditoría interna para decisiones de jefatura.",
        "color": "naranja",
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


def _find_module(module_slug: str) -> Optional[Dict[str, Any]]:
    for module in JEFATURA_UROLOGIA_MODULES:
        if module.get("slug") == module_slug:
            return module
    return None


async def render_jefatura_urologia_home_flow(request):
    return m.render_template(
        "jefatura_urologia_home.html",
        request=request,
        modules=JEFATURA_UROLOGIA_MODULES,
    )


async def render_jefatura_urologia_module_flow(request, module_slug: str):
    module = _find_module(module_slug)
    if not module:
        module = {
            "slug": "desconocido",
            "nombre": "Módulo no encontrado",
            "icono": "⚠️",
            "descripcion": "El enlace solicitado no existe. Vuelva al panel de Jefatura de Urología.",
            "color": "rojo",
        }
    return m.render_template(
        "jefatura_urologia_modulo_placeholder.html",
        request=request,
        module=module,
    )
