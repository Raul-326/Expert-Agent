# 内网部署说明

当前项目是 `Next.js + FastAPI + PostgreSQL` 架构。

内网推荐方式是“服务器直跑”：

- 浏览器访问 `Next.js` 前端
- 前端通过同域 `/api/*` 代理到本机 `FastAPI`
- 启动前先执行 `Alembic` 迁移
- 不依赖 Docker

## SCM 发布

如果走字节内部 `SCM`，推荐这样配置：

- 执行方式：`SCM 编译脚本`
- 编译脚本相对路径：`build.sh`
- 编译产物上传目录：`output`

`build.sh` 会完成：

```bash
npm ci
npm run build
复制 frontend standalone 运行产物
复制 backend/alembic/根目录依赖文件
```

发布到目标机器后，使用：

```bash
bash deploy/start_scm.sh
```

它会执行：

```bash
python3 -m venv backend/.venv
pip install -r backend/requirements.txt
alembic upgrade head
uvicorn app.main:app
node frontend/server.js
```

## 必需环境变量

```bash
export DATABASE_URL=postgresql://user:password@host:5432/expert_agent
export ARK_API_KEY=你的ark_key
export FEISHU_APP_ID=你的feishu_app_id
export FEISHU_APP_SECRET=你的feishu_app_secret
```

可选环境变量：

```bash
export PORT=3000
export BACKEND_PORT=8000
export AUTO_CREATE_SCHEMA=false
```

说明：

- 正式环境不应依赖 `AUTO_CREATE_SCHEMA`
- 数据库结构统一通过 `alembic upgrade head` 管理

## 服务器直跑

服务器需要准备：

- `Python 3.11`
- `Node.js 20`
- `npm`
- 可访问的 `PostgreSQL`

推荐直接使用脚本：

```bash
bash deploy/start_internal.sh
```

脚本会自动执行：

```bash
pip install -r backend/requirements.txt
npm ci
npm run build
alembic upgrade head
uvicorn app.main:app
npm run start
```

默认端口：

- 前端 `3000`
- 后端 `8000`

如果你要改端口：

```bash
export PORT=3100
export BACKEND_PORT=8100
bash deploy/start_internal.sh
```

如果你不想前台挂着跑，可以用：

```bash
nohup bash deploy/start_internal.sh > /tmp/expert_agent_internal.log 2>&1 &
```

## Docker 启动

构建镜像：

```bash
docker build -t expert-agent-web:latest .
```

运行镜像：

```bash
docker run -d \
  --name expert-agent-web \
  -p 3000:3000 \
  -e DATABASE_URL=postgresql://user:password@host:5432/expert_agent \
  -e ARK_API_KEY=你的ark_key \
  -e FEISHU_APP_ID=你的feishu_app_id \
  -e FEISHU_APP_SECRET=你的feishu_app_secret \
  expert-agent-web:latest
```

容器入口会自动执行：

```bash
alembic upgrade head
uvicorn app.main:app
node server.js
```

## 本地开发

开发模式仍然使用：

```bash
bash deploy/start_dev.sh
```

它会先执行数据库迁移，再启动后端和前端。

## 反向代理

如果需要通过网关/Nginx 暴露固定 URL，只需要代理前端端口即可：

- 外部 `80/443` -> 容器 `3000`

前端会把 `/api/*` 自动转发到同容器内的 FastAPI。

## 数据

- 不再使用 SQLite 本地文件作为主库
- 使用独立 PostgreSQL 实例
- 数据库升级通过 Alembic revision 管理
