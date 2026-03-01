#!/usr/bin/env python3
"""Extrae plantillas *_TEMPLATE embebidas en main.py a app/templates/*.html."""

from __future__ import annotations

import pathlib
import re


ROOT = pathlib.Path(__file__).resolve().parents[1]
MAIN_FILE = ROOT / "main.py"
OUT_DIR = ROOT / "app" / "templates"

NAME_TO_FILE = {
    "MENU_TEMPLATE": "menu.html",
    "CONSULTA_TEMPLATE": "consulta.html",
    "CONFIRMACION_TEMPLATE": "confirmacion.html",
    "HOSPITALIZACION_LISTA_TEMPLATE": "hospitalizacion_lista.html",
    "HOSPITALIZACION_NUEVO_TEMPLATE": "hospitalizacion_nuevo.html",
    "QUIROFANO_HOME_TEMPLATE": "quirofano_home.html",
    "QUIROFANO_PROGRAMADA_TEMPLATE": "quirofano_programada.html",
    "QUIROFANO_PLACEHOLDER_TEMPLATE": "quirofano_placeholder.html",
    "QUIROFANO_LISTA_TEMPLATE": "quirofano_lista.html",
    "QUIROFANO_NUEVO_TEMPLATE": "quirofano_nuevo.html",
    "EXPEDIENTE_TEMPLATE": "expediente.html",
    "BUSQUEDA_TEMPLATE": "busqueda.html",
    "BUSQUEDA_SEMANTICA_TEMPLATE": "busqueda_semantica.html",
    "REPORTE_TEMPLATE": "reporte.html",
    "DASHBOARD_TEMPLATE": "dashboard.html",
    "CARGA_ARCHIVOS_TEMPLATE": "carga_archivos.html",
}


def main() -> None:
    text = MAIN_FILE.read_text(encoding="utf-8")
    pattern = re.compile(
        r"^(?P<name>[A-Z0-9_]+_TEMPLATE)\s*=\s*\"\"\"(?P<body>.*?)\"\"\"",
        re.M | re.S,
    )
    templates = {m.group("name"): m.group("body") for m in pattern.finditer(text)}
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    written = 0
    missing = []
    for const_name, filename in NAME_TO_FILE.items():
        body = templates.get(const_name)
        if body is None:
            missing.append(const_name)
            continue
        out_path = OUT_DIR / filename
        out_path.write_text(body, encoding="utf-8")
        written += 1

    print(f"templates_written={written}")
    if missing:
        print("missing_constants=" + ",".join(missing))


if __name__ == "__main__":
    main()

