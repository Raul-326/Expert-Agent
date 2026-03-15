#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$ROOT_DIR/backend/.venv"
PORT="${PORT:-3000}"
BACKEND_PORT="${BACKEND_PORT:-8000}"

if [ -z "${DATABASE_URL:-}" ]; then
  echo "[Error] DATABASE_URL 未设置"
  exit 1
fi

if [ ! -x "$VENV/bin/python" ]; then
  echo "[Backend] 创建虚拟环境..."
  python3 -m venv "$VENV"
fi

echo "[Backend] 安装/校验依赖..."
"$VENV/bin/pip" install -q -r "$ROOT_DIR/backend/requirements.txt"

echo "[Frontend] 安装/校验依赖..."
cd "$ROOT_DIR/frontend"
npm ci
npm run build

echo "[Backend] 执行数据库迁移..."
cd "$ROOT_DIR/backend"
"$VENV/bin/alembic" upgrade head

echo "[Backend] 启动 FastAPI..."
"$VENV/bin/uvicorn" app.main:app --host 127.0.0.1 --port "$BACKEND_PORT" --log-level info &
BACKEND_PID=$!

echo "[Frontend] 启动 Next.js..."
cd "$ROOT_DIR/frontend"
HOSTNAME="0.0.0.0" PORT="$PORT" npm run start &
FRONTEND_PID=$!

cleanup() {
  kill "$BACKEND_PID" "$FRONTEND_PID" 2>/dev/null || true
}

trap cleanup EXIT INT TERM

echo ""
echo "服务已启动:"
echo "  Frontend: http://127.0.0.1:$PORT"
echo "  Backend:  http://127.0.0.1:$BACKEND_PORT"
echo ""

wait -n "$BACKEND_PID" "$FRONTEND_PID"
