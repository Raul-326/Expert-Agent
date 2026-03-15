#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUTPUT_DIR="$ROOT_DIR/output"

echo "[SCM] 清理旧构建产物..."
rm -rf "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR"

if ! command -v npm >/dev/null 2>&1; then
  echo "[SCM] 未找到 npm。请在 SCM 中选择包含 Node.js/npm 的编译镜像。"
  exit 1
fi

echo "[SCM] 构建前端..."
cd "$ROOT_DIR/frontend"
npm ci
npm run build

echo "[SCM] 复制前端运行产物..."
mkdir -p "$OUTPUT_DIR/frontend/.next"
cp -R .next/standalone/. "$OUTPUT_DIR/frontend/"
cp -R .next/static "$OUTPUT_DIR/frontend/.next/static"
cp -R public "$OUTPUT_DIR/frontend/public"

echo "[SCM] 复制后端运行文件..."
mkdir -p "$OUTPUT_DIR/backend"
cp -R "$ROOT_DIR/backend/app" "$OUTPUT_DIR/backend/app"
cp -R "$ROOT_DIR/backend/alembic" "$OUTPUT_DIR/backend/alembic"
cp "$ROOT_DIR/backend/alembic.ini" "$OUTPUT_DIR/backend/alembic.ini"
cp "$ROOT_DIR/backend/requirements.txt" "$OUTPUT_DIR/backend/requirements.txt"

echo "[SCM] 复制根目录依赖..."
cp -R "$ROOT_DIR/agent" "$OUTPUT_DIR/agent"
cp "$ROOT_DIR/workflow_feishu.py" "$OUTPUT_DIR/workflow_feishu.py"
cp "$ROOT_DIR/panel_metrics.py" "$OUTPUT_DIR/panel_metrics.py"
cp "$ROOT_DIR/feishu_token_manager.py" "$OUTPUT_DIR/feishu_token_manager.py"
cp "$ROOT_DIR/name_roster.txt" "$OUTPUT_DIR/name_roster.txt"

echo "[SCM] 复制部署脚本..."
mkdir -p "$OUTPUT_DIR/deploy"
cp "$ROOT_DIR/deploy/start_scm.sh" "$OUTPUT_DIR/deploy/start_scm.sh"
cp "$ROOT_DIR/DEPLOY_INTERNAL.md" "$OUTPUT_DIR/DEPLOY_INTERNAL.md"

chmod +x "$OUTPUT_DIR/deploy/start_scm.sh"

echo "[SCM] 构建完成，产物目录: $OUTPUT_DIR"
