#!/bin/bash
# ByteDance SCM Build Script

set -e

echo "Starting build process..."

# 1. 后端依赖检查
echo "Checking backend dependencies..."
cd backend
pip install -r requirements.txt
cd ..

# 2. 前端构建 (如果有 Node 环境)
if command -v npm &> /dev/null
then
    echo "Building frontend..."
    cd frontend
    npm install
    npm run build
    cd ..
fi

echo "Build finished successfully."
