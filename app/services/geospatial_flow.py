from __future__ import annotations

from typing import Any, Callable

from fastapi.responses import HTMLResponse, JSONResponse


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
        return HTMLResponse("<h1>Mapa no disponible</h1><p>Instale folium para habilitar este módulo.</p>", status_code=503)

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
        return HTMLResponse("<h1>Mapa no disponible</h1><p>Instale folium para habilitar este módulo.</p>", status_code=503)
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
