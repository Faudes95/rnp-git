from __future__ import annotations

import threading
import time
from typing import Any, Callable, Dict, List


def normalize_startup_mode(raw_mode: str, *, default: str = "background") -> str:
    mode = str(raw_mode or default).strip().lower()
    if mode not in {"sync", "background", "off"}:
        return default
    return mode


def launch_background_task(
    *,
    task_name: str,
    logger: Any,
    fn: Callable[[], Any],
    delay_sec: float = 0.0,
) -> str:
    def _runner() -> None:
        if delay_sec > 0:
            time.sleep(delay_sec)
        try:
            result = fn()
            logger.info({"event": f"{task_name}_completed", "result": result})
        except Exception as exc:
            logger.warning({"event": f"{task_name}_failed", "detail": str(exc)})

    thread_name = f"{task_name}-bg"
    t = threading.Thread(target=_runner, name=thread_name, daemon=True)
    t.start()
    logger.info({"event": f"{task_name}_started_background", "delay_sec": delay_sec, "thread": thread_name})
    return thread_name


def schedule_model_warmup(
    *,
    task_name: str,
    logger: Any,
    warmup_fn: Callable[[List[str]], Dict[str, bool]],
    model_paths: List[str],
    mode: str,
    delay_sec: float = 0.0,
) -> Dict[str, Any]:
    resolved_mode = normalize_startup_mode(mode, default="background")
    if resolved_mode == "off":
        logger.info({"event": f"{task_name}_skipped", "mode": "off"})
        return {"mode": "off", "started": False, "loaded": {}}

    if resolved_mode == "sync":
        loaded = warmup_fn(model_paths)
        logger.info({"event": task_name, "mode": "sync", "loaded": {k: bool(v) for k, v in loaded.items()}})
        return {"mode": "sync", "started": False, "loaded": loaded}

    # background
    launch_background_task(
        task_name=task_name,
        logger=logger,
        delay_sec=delay_sec,
        fn=lambda: warmup_fn(model_paths),
    )
    return {"mode": "background", "started": True, "loaded": {}}
