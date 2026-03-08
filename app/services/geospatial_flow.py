from __future__ import annotations

from typing import Any, Callable

from fastapi.responses import HTMLResponse, JSONResponse


def _map_unavailable_page(title: str, detail: str) -> HTMLResponse:
    return HTMLResponse(
        f"""
        <html lang="es">
          <head>
            <meta charset="utf-8" />
            <title>{title}</title>
            <style>
              body {{
                margin: 0;
                font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
                background: #f4f7fb;
                color: #12304a;
              }}
              .wrap {{
                max-width: 760px;
                margin: 64px auto;
                padding: 32px;
              }}
              .card {{
                background: #ffffff;
                border: 1px solid #d5dfeb;
                border-radius: 24px;
                padding: 32px;
                box-shadow: 0 20px 40px rgba(18, 48, 74, 0.08);
              }}
              h1 {{
                margin: 0 0 12px;
                font-size: 2rem;
                color: #103b78;
              }}
              p {{
                margin: 0;
                font-size: 1.05rem;
                line-height: 1.6;
              }}
            </style>
          </head>
          <body>
            <div class="wrap">
              <div class="card">
                <h1>{title}</h1>
                <p>{detail}</p>
              </div>
            </div>
          </body>
        </html>
        """,
        status_code=200,
    )


def admin_geocodificar_flow(
    *,
    limite: int,
    sleep_seconds: float,
    db: Any,
    sdb: Any,
    geocodificar_pacientes_pendientes_fn: Callable[..., int],
) -> JSONResponse:
    try:
        actualizados = geocodificar_pacientes_pendientes_fn(
            sdb=sdb,
            db=db,
            limite=limite,
            sleep_seconds=max(0.0, float(sleep_seconds)),
        )
        return JSONResponse(content={"status": "ok", "actualizados": int(actualizados)})
    except Exception as exc:
        sdb.rollback()
        return JSONResponse(status_code=500, content={"status": "error", "message": str(exc)})


def api_geostats_pacientes_flow(
    *,
    limit: int,
    sdb: Any,
    build_geojson_pacientes_programados_fn: Callable[..., dict],
) -> JSONResponse:
    geojson_data = build_geojson_pacientes_programados_fn(sdb=sdb, limit=limit)
    return JSONResponse(content=geojson_data)


def mapa_epidemiologico_geojson_flow(
    *,
    sdb: Any,
    folium_module: Any,
    build_geojson_pacientes_programados_fn: Callable[..., dict],
) -> HTMLResponse:
    if folium_module is None:
        return _map_unavailable_page(
            "Mapa epidemiológico en modo básico",
            "El visor interactivo no está disponible en este entorno porque falta el componente de mapas. "
            "La ruta sigue operativa para validación y puede habilitarse instalando folium.",
        )

    geojson_data = build_geojson_pacientes_programados_fn(sdb=sdb, limit=2000)
    mapa = folium_module.Map(location=[19.4326, -99.1332], zoom_start=10)
    folium_module.GeoJson(
        geojson_data,
        name="Pacientes programados",
        popup=folium_module.GeoJsonPopup(fields=["hgz", "patologia", "edad", "sexo"]),
    ).add_to(mapa)
    folium_module.LayerControl().add_to(mapa)
    return HTMLResponse(mapa._repr_html_())


def mapa_epidemiologico_flow(
    *,
    sdb: Any,
    folium_module: Any,
    marker_cluster_cls: Any,
    hecho_programacion_cls: Any,
    sql_func: Any,
) -> HTMLResponse:
    if folium_module is None or marker_cluster_cls is None:
        return _map_unavailable_page(
            "Mapa epidemiológico en modo básico",
            "El visor interactivo no está disponible en este entorno porque falta el componente de mapas. "
            "La ruta sigue operativa para validación y puede habilitarse instalando folium.",
        )
    mapa = folium_module.Map(location=[19.4326, -99.1332], zoom_start=10)
    marker_cluster = marker_cluster_cls().add_to(mapa)
    hgz_coords = {
        "HGZ 1": (19.435, -99.141),
        "HGZ 2": (19.421, -99.180),
        "HGZ 27": (19.486, -99.123),
    }
    rows = (
        sdb.query(
            hecho_programacion_cls.hgz,
            sql_func.count(hecho_programacion_cls.id).label("total"),
        )
        .group_by(hecho_programacion_cls.hgz)
        .all()
    )
    for hgz, total in rows:
        if not hgz:
            continue
        latlon = hgz_coords.get(hgz)
        if not latlon:
            continue
        folium_module.Marker(
            location=[latlon[0], latlon[1]],
            popup=f"{hgz}: {int(total)} pacientes programados",
            icon=folium_module.Icon(color="green", icon="info-sign"),
        ).add_to(marker_cluster)
    return HTMLResponse(mapa._repr_html_())
