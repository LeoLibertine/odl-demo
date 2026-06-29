#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
#  ODL Demo — arranque de app + dashboard
#  Uso:  ./start.sh            (levanta API:8788 y Dashboard:8787)
#        ./start.sh stop       (detiene ambos)
# ══════════════════════════════════════════════════════════════════════════════
set -euo pipefail
cd "$(dirname "$0")"

PY="${PYTHON:-python3}"

# ── stop ──────────────────────────────────────────────────────────────────────
if [[ "${1:-}" == "stop" ]]; then
  for p in 8788 8787; do
    pid=$(lsof -ti:"$p" 2>/dev/null || true)
    [[ -n "$pid" ]] && kill "$pid" && echo "🛑 Detenido puerto $p (pid $pid)" || echo "ℹ️  Nada en $p"
  done
  exit 0
fi

# ── .env ──────────────────────────────────────────────────────────────────────
if [[ ! -f .env ]]; then
  echo "❌ No existe .env. Cópialo y complétalo:  cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

if [[ -z "${MONGODB_ODL_URI:-}" ]]; then
  echo "❌ MONGODB_ODL_URI vacío en .env. Es obligatorio."
  exit 1
fi

# ── venv + deps ───────────────────────────────────────────────────────────────
if [[ ! -d .venv ]]; then
  echo "📦 Creando entorno virtual e instalando dependencias..."
  "$PY" -m venv .venv
  ./.venv/bin/pip install -q --upgrade pip
  ./.venv/bin/pip install -q -r requirements.txt
fi
VENV_PY=./.venv/bin/python

# ── arranque ──────────────────────────────────────────────────────────────────
echo "🚀 Levantando API (8788) y Dashboard (8787)..."
nohup "$VENV_PY" -u api_canales.py   > logs_api.log       2>&1 &
nohup "$VENV_PY" -u api_dashboard.py > logs_dashboard.log 2>&1 &

sleep 3
echo ""
echo "✅ Servicios arrancando. Logs: logs_api.log / logs_dashboard.log"
echo "   App:        http://localhost:8788/app"
echo "   API docs:   http://localhost:8788/docs"
echo "   Dashboard:  http://localhost:8787/"
echo ""
echo "   El dashboard tarda ~30-60s en el primer cálculo (procesa millones de docs)."
echo "   Para detener:  ./start.sh stop"
