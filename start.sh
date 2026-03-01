#!/bin/bash
# ═══════════════════════════════════════════════════════════
# RNP - Arranque Rápido / Fast Start
# ═══════════════════════════════════════════════════════════
# USO:
#   ./start.sh              → Arranque normal
#   ./start.sh --quick      → Sin pre-compilación (segunda vez+)
#   ./start.sh --port 9000  → Puerto personalizado
#
# PRIMERA VEZ: ~2-3 min (pre-compila todo)
# SEGUNDA VEZ+: ~30-60s (usa cache .pyc)
# ═══════════════════════════════════════════════════════════

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PORT=${2:-8000}
QUICK=false

for arg in "$@"; do
    case $arg in
        --quick) QUICK=true ;;
        --port) ;; # handled by positional
    esac
done

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  🏥 RNP - Registro Nacional de Pacientes                ║"
echo "║  ⚡ Fast Startup v1.0                                   ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

# Matar procesos previos
echo "🧹 Limpiando procesos previos..."
pkill -f "uvicorn main:app" 2>/dev/null || true
sleep 1

# Env vars
export ALLOW_INSECURE_DEFAULT_CREDENTIALS=true

if [ "$QUICK" = false ]; then
    echo "⚡ Pre-compilando bytecode (paralelo)..."
    START_COMPILE=$(date +%s)

    # Compilar en paralelo con todos los cores
    python3 -m compileall -q -j 0 app/ 2>/dev/null || true
    python3 -m compileall -q -j 0 fau_bot_core/ 2>/dev/null || true

    END_COMPILE=$(date +%s)
    echo "✅ Pre-compilación: $((END_COMPILE - START_COMPILE))s"
    echo ""
fi

echo "🚀 Iniciando servidor en puerto $PORT..."
echo "   URL: http://localhost:$PORT"
echo "   Credenciales: admin:admin"
echo "   Docs: http://localhost:$PORT/docs"
echo ""

python3 -m uvicorn main:app --host 0.0.0.0 --port "$PORT"
