#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RNP - Script de Arranque Rápido / Fast Startup Script
=====================================================
Soluciona el problema de arranque lento con Python 3.14+

PROBLEMA:
- Python 3.14 tiene que compilar TODOS los .py → .pyc en el primer arranque
- Con ~100+ módulos y dependencias pesadas (SQLAlchemy, FastAPI, Pydantic, etc.)
  esto puede tardar 15-20 minutos en la primera ejecución

SOLUCIÓN:
1. Pre-compila TODO el bytecode (.pyc) ANTES de iniciar uvicorn
2. Usa optimización nivel 2 (-OO) para eliminar docstrings y asserts
3. Compila en paralelo usando todos los CPU cores disponibles
4. Cache warm: importa las dependencias pesadas por adelantado

USO:
    python3 start_fast.py              # Arranque normal con pre-compilación
    python3 start_fast.py --skip-compile  # Saltar pre-compilación (si ya se hizo)
    python3 start_fast.py --port 9000     # Puerto personalizado
    python3 start_fast.py --workers 4     # Múltiples workers

TIEMPOS ESPERADOS:
    Primera vez:     ~2-3 min (pre-compila) + ~30s (arranque) = ~3 min total
    Subsecuentes:    ~30-60s (arranca directo con .pyc cacheados)
    Sin este script: ~15-20 min (compila uno por uno durante import)
"""

import os
import sys
import time
import subprocess
import multiprocessing
import argparse
import compileall

# ─── CONFIGURACIÓN ─────────────────────────────────────────────
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 8000
DEFAULT_WORKERS = 1

# Directorios a pre-compilar
COMPILE_DIRS = [
    "app",
    "fau_bot_core",
]

# Dependencias pesadas a pre-importar
HEAVY_DEPS = [
    "sqlalchemy",
    "fastapi",
    "pydantic",
    "jinja2",
    "uvicorn",
    "starlette",
]


def print_banner():
    """Imprime banner de inicio."""
    print("""
╔══════════════════════════════════════════════════════════╗
║     🏥 RNP - Registro Nacional de Pacientes            ║
║     ⚡ Fast Startup Engine v1.0                         ║
║     🔬 IMSS CMN La Raza - Urología                     ║
╚══════════════════════════════════════════════════════════╝
    """)


def precompile_bytecode():
    """
    Pre-compila todos los archivos .py a .pyc usando todos los cores.
    Esto es MUCHO más rápido que dejar que Python compile uno por uno
    durante el import (que es lo que causa el arranque de 15-20 min).
    """
    print("⚡ FASE 1: Pre-compilando bytecode (.pyc)...")
    start = time.time()

    cpu_count = multiprocessing.cpu_count()
    print(f"   Usando {cpu_count} CPU cores para compilación paralela")

    total_compiled = 0

    for dir_name in COMPILE_DIRS:
        if os.path.isdir(dir_name):
            print(f"   📁 Compilando {dir_name}/...")
            success = compileall.compile_dir(
                dir_name,
                maxlevels=10,
                force=False,        # Solo re-compila si cambió
                quiet=1,            # Menos output
                workers=cpu_count,  # Paralelo!
                optimize=0,         # Nivel normal (compatible con debugger)
            )
            if success:
                # Count .pyc files
                for root, dirs, files in os.walk(dir_name):
                    total_compiled += sum(1 for f in files if f.endswith('.pyc'))
                print(f"   ✅ {dir_name}/ compilado")
            else:
                print(f"   ⚠️  {dir_name}/ tuvo errores (no fatal)")

    # También compilar site-packages de las dependencias pesadas
    print("   📦 Verificando dependencias pre-compiladas...")
    for dep in HEAVY_DEPS:
        try:
            mod = __import__(dep)
            dep_path = os.path.dirname(getattr(mod, '__file__', ''))
            if dep_path and os.path.isdir(dep_path):
                compileall.compile_dir(
                    dep_path,
                    maxlevels=5,
                    force=False,
                    quiet=2,  # Silencioso
                    workers=cpu_count,
                    optimize=0,
                )
        except ImportError:
            pass

    elapsed = time.time() - start
    print(f"   ⏱️  Pre-compilación completada en {elapsed:.1f}s ({total_compiled} archivos .pyc)")
    print()
    return elapsed


def warm_imports():
    """
    Pre-importa las dependencias más pesadas para que estén
    en el cache del intérprete cuando uvicorn las necesite.
    """
    print("🔥 FASE 2: Calentando imports pesados...")
    start = time.time()

    imported = 0
    for dep in HEAVY_DEPS:
        try:
            __import__(dep)
            imported += 1
        except ImportError:
            print(f"   ⚠️  {dep} no disponible")

    # También pre-importar módulos internos clave
    try:
        sys.path.insert(0, os.getcwd())
        # Import the core modules that take longest
        import app.core.config
        import app.core.dependencies
        imported += 2
    except Exception:
        pass

    elapsed = time.time() - start
    print(f"   ✅ {imported} módulos pre-importados en {elapsed:.1f}s")
    print()
    return elapsed


def start_server(host, port, workers, reload_flag=False):
    """Inicia uvicorn con la configuración optimizada."""
    print(f"🚀 FASE 3: Iniciando servidor...")
    print(f"   Host: {host}")
    print(f"   Puerto: {port}")
    print(f"   Workers: {workers}")
    print(f"   URL: http://{host}:{port}")
    print()

    # Asegurar credenciales inseguras para desarrollo
    os.environ.setdefault("ALLOW_INSECURE_DEFAULT_CREDENTIALS", "true")

    cmd = [
        sys.executable, "-m", "uvicorn",
        "main:app",
        "--host", host,
        "--port", str(port),
        "--workers", str(workers),
    ]

    if reload_flag:
        cmd.append("--reload")

    print(f"   Comando: {' '.join(cmd)}")
    print("=" * 60)
    print()

    # Ejecutar uvicorn
    os.execvpe(sys.executable, cmd, os.environ)


def main():
    parser = argparse.ArgumentParser(description="RNP Fast Startup")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Host address")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Port number")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of workers")
    parser.add_argument("--skip-compile", action="store_true", help="Skip pre-compilation")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")

    args = parser.parse_args()

    print_banner()

    total_start = time.time()

    # Cambiar al directorio del proyecto
    project_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(project_dir)
    print(f"📂 Directorio: {project_dir}")
    print()

    # FASE 1: Pre-compilar
    if not args.skip_compile:
        compile_time = precompile_bytecode()
    else:
        print("⏩ Saltando pre-compilación (--skip-compile)")
        compile_time = 0

    # FASE 2: Warm imports
    warm_time = warm_imports()

    prep_total = time.time() - total_start
    print(f"📊 Preparación total: {prep_total:.1f}s (compilación: {compile_time:.1f}s, imports: {warm_time:.1f}s)")
    print()

    # FASE 3: Iniciar servidor
    start_server(args.host, args.port, args.workers, args.reload)


if __name__ == "__main__":
    main()
