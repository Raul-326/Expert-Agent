FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOST=0.0.0.0 \
    PORT=8501 \
    APP_FILE=panel_app.py

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY .streamlit/ .streamlit/
COPY agent/ agent/
COPY deploy/ deploy/
COPY panel_app.py boss_panel_app.py panel_db.py panel_metrics.py workflow_feishu.py ./
COPY feishu_token_manager.py backfill_projects.py backfill_projects.csv seed_test_panel_db.py agent_run.py ./
COPY DEPLOY_INTERNAL.md ./

EXPOSE 8501

CMD ["bash", "deploy/start_panel.sh"]
