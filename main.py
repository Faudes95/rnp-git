# -*- coding: utf-8 -*-
import os

from app.core.boot_profile import (
    BOOT_PROFILE_FULL,
    is_minimal_jefatura_profile,
    normalize_app_boot_profile,
)


APP_BOOT_PROFILE = normalize_app_boot_profile(os.getenv("APP_BOOT_PROFILE", BOOT_PROFILE_FULL))

if is_minimal_jefatura_profile(APP_BOOT_PROFILE):
    from app.entrypoints.minimal_jefatura_main import *  # noqa: F401,F403
else:
    from main_full import *  # noqa: F401,F403
