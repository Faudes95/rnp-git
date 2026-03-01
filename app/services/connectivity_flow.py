from __future__ import annotations

import os
from typing import Any, Dict

from app.services.db_platform_flow import get_database_platform_status


def build_connectivity_payload(
    *,
    app_static_dir: str,
    menu_imss_logo_fallback_path: str,
    menu_imss_pattern_fallback_path: str,
    menu_urologia_logo_fallback_path: str,
    menu_hospital_bg_fallback_path: str,
    connectivity_mode: str,
    offline_strict_mode: bool,
    geocoder_available: bool,
) -> Dict[str, Any]:
    vendor_files = {
        "vue": os.path.isfile(os.path.join(app_static_dir, "vendor", "vue.min.js")),
        "axios": os.path.isfile(os.path.join(app_static_dir, "vendor", "axios.min.js")),
        "chart": os.path.isfile(os.path.join(app_static_dir, "vendor", "chart.min.js")),
        "fonts_css": os.path.isfile(os.path.join(app_static_dir, "css", "fonts_offline.css")),
    }
    fallback_assets = {
        "imss_logo_fallback": os.path.isfile(menu_imss_logo_fallback_path),
        "imss_pattern_fallback": os.path.isfile(menu_imss_pattern_fallback_path),
        "urologia_logo_fallback": os.path.isfile(menu_urologia_logo_fallback_path),
        "hospital_bg_fallback": os.path.isfile(menu_hospital_bg_fallback_path),
    }
    return {
        "status": "ok",
        "connectivity_mode": connectivity_mode,
        "offline_strict_mode": bool(offline_strict_mode),
        "geocoder_enabled": bool(geocoder_available and not offline_strict_mode),
        "static_mounted": os.path.isdir(app_static_dir),
        "vendor_assets": vendor_files,
        "fallback_assets": fallback_assets,
        "database_platform": get_database_platform_status(),
    }

