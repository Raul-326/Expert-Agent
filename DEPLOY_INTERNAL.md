# 内网部署说明

这个项目适合部署成常驻 `streamlit` 服务，再由公司内网 Nginx 或网关反向代理成固定内部 URL。

## 启动

```bash
cd /path/to/automation_accuracy
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

设置环境变量：

```bash
export PANEL_DB_PATH=/data/automation_accuracy/metrics_panel.db
export PANEL_OPERATOR=panel
export PANEL_AUTH_MODE=user
export FEISHU_USER_ACCESS_TOKEN=你的飞书token
```

启动面板：

```bash
bash deploy/start_panel.sh
```

默认监听 `0.0.0.0:8501`。

如果要启动老板版面板：

```bash
APP_FILE=boss_panel_app.py PORT=8502 bash deploy/start_panel.sh
```

## Docker 启动

如果你们走容器平台，可以直接构建镜像：

```bash
docker build -t automation-accuracy:latest .
```

运行时挂载数据库目录，并通过环境变量指定数据库路径：

```bash
docker run -d \
  --name automation-accuracy \
  -p 8501:8501 \
  -e PANEL_DB_PATH=/data/metrics_panel.db \
  -e PANEL_OPERATOR=panel \
  -e PANEL_AUTH_MODE=user \
  -e FEISHU_USER_ACCESS_TOKEN=你的飞书token \
  -v /data/automation_accuracy:/data \
  automation-accuracy:latest
```

如果要启动老板版面板：

```bash
docker run -d \
  --name automation-accuracy-boss \
  -p 8502:8502 \
  -e APP_FILE=boss_panel_app.py \
  -e PORT=8502 \
  -e PANEL_DB_PATH=/data/metrics_panel.db \
  -v /data/automation_accuracy:/data \
  automation-accuracy:latest
```

## 反向代理

参考 [deploy/expert-agent-panel.nginx.conf](/Users/bytedance/code/Expert Agent/deploy/expert-agent-panel.nginx.conf)：

- `server_name` 改成你的内网域名
- `proxy_pass` 指向实际服务端口

最终访问地址应为公司内网域名，而不是 `localhost:8501`。

## 数据

- 数据库文件不要进 Git
- 数据库文件不要打进 Docker 镜像
- 服务器上单独放置 SQLite 文件，并通过 `PANEL_DB_PATH` 指向它
- 当前是单机 SQLite 部署，不适合多实例同时写入
