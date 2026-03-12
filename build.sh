#!/bin/bash
# ByteDance SCM 增强版构建脚本 (支持 output 目录模式)

set -e

echo "--- 1. 清理并创建 output 目录 ---"
rm -rf output && mkdir output

# --- 2. 后端整理 ---
echo "--- 2. 准备后端文件 ---"
# 拷贝后端核心代码和配置文件
mkdir -p output/backend
cp -r backend/app output/backend/
cp backend/requirements.txt output/backend/
cp backend/.env output/backend/ 2>/dev/null || true

# 拷贝根目录下的核心依赖算法和名单
cp workflow_feishu.py panel_metrics.py name_roster.txt output/

# --- 3. 前端构建 ---
if [ -d "frontend" ]; then
    echo "--- 3. 开始前端构建 (Next.js) ---"
    cd frontend
    
    # 注入环境变量（如果在 SCM 环境变量中没配，则尝试在 build 时设置）
    # export NEXT_PUBLIC_API_URL=${NEXT_PUBLIC_API_URL:-"http://localhost:8000"}
    
    npm install
    npm run build
    cd ..
    
    # 将前端产物拷贝到 output
    mkdir -p output/frontend
    cp -r frontend/.next output/frontend/
    cp -r frontend/public output/frontend/
    cp frontend/package.json output/frontend/
    cp frontend/next.config.ts output/frontend/ 2>/dev/null || cp frontend/next.config.js output/frontend/ 2>/dev/null || true
fi

# --- 4. 拷贝部署脚本 ---
cp Dockerfile.prod output/
cp build.sh output/

echo "--- 构建成功！所有运行所需文件已存放在 output 目录中 ---"
ls -R output
