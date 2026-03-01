from __future__ import annotations

import importlib


def test_imports_smoke() -> None:
    modules = [
        "app.api.fau_bot",
        "app.services.fau_central_brain",
        "app.services.fau_bot_flow",
    ]
    for mod_name in modules:
        importlib.import_module(mod_name)

