#!/bin/bash
# 本地开发启动脚本
# 使用方法: bash deploy/start_dev.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "==========================================="
echo "  Expert Agent Multi-Agent Web Platform"
echo "==========================================="

# 1. 检查后端 venv，不存在就创建并安装依赖
VENV="$PROJECT_ROOT/backend/.venv"
if [ ! -f "$VENV/bin/uvicorn" ]; then
    echo "[Backend] 未找到虚拟环境，正在创建..."
    python3 -m venv "$VENV"
    "$VENV/bin/pip" install -q -r "$PROJECT_ROOT/backend/requirements.txt"
    echo "[Backend] 依赖安装完成"
fi

# 2. 启动 FastAPI 后端 (后台运行)
echo "[Backend] 正在启动 FastAPI... http://127.0.0.1:8000"
cd "$PROJECT_ROOT/backend"
"$VENV/bin/uvicorn" app.main:app --reload --port 8000 --log-level info &
BACKEND_PID=$!

# 3. 等待后端就绪
sleep 2
echo "[Backend] PID=$BACKEND_PID 已启动"

# 4. 启动 Next.js 前端
echo "[Frontend] 正在启动 Next.js... http://127.0.0.1:3000"
cd "$PROJECT_ROOT/frontend"
npm run dev &
FRONTEND_PID=$!

echo ""
echo "✅ 服务已全部启动:"
echo "   Boss 数据大盘:   http://localhost:3000/dashboard"
echo "   Admin 入库作业:  http://localhost:3000/workspace"
echo "   FastAPI 文档:    http://localhost:8000/docs"
echo ""
echo "停止服务请按 Ctrl+C 或运行: kill $BACKEND_PID $FRONTEND_PID"

# 等待任意进程退出
wait
