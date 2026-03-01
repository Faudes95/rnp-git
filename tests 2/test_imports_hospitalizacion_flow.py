def test_import_hospitalizacion_flow_exports() -> None:
    from app.services import hospitalizacion_flow as hf

    assert hasattr(hf, "guardar_hospitalizacion_flow")
    assert hasattr(hf, "hospitalizacion_ingreso_preop_imprimir_docx_flow")


def test_import_hospitalizacion_router_routes() -> None:
    from app.api.hospitalizacion import router

    paths = {getattr(route, "path", "") for route in router.routes}
    assert "/hospitalizacion/nuevo" in paths
    assert "/hospitalizacion/ingreso/docx/{hospitalizacion_id}" in paths
