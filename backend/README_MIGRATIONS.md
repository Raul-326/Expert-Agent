# 数据库迁移

后端数据库结构通过 `Alembic` 管理，不再依赖应用启动时自动建表。

## 常用命令

在 `backend/` 目录执行：

```bash
.venv/bin/alembic upgrade head
```

查看当前 revision：

```bash
.venv/bin/alembic current
```

生成新迁移：

```bash
.venv/bin/alembic revision -m "describe change"
```

## 环境变量

必须提供：

```bash
DATABASE_URL=postgresql://user:password@host:5432/expert_agent
```

可选：

```bash
AUTO_CREATE_SCHEMA=false
```

`AUTO_CREATE_SCHEMA` 仅作为本地开发兜底，不应在正式环境开启。
