import sqlite3
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime

# 配置路径
SQLITE_DB = "metrics_panel.db"
POSTGRES_URL = "postgresql://agent_user:agent_password@localhost:5432/expert_agent"

def migrate():
    if not os.path.exists(SQLITE_DB):
        print(f"❌ 未找到 SQLite 数据库文件: {SQLITE_DB}")
        return

    print(f"🚀 开始从 {SQLITE_DB} 迁移数据到 PostgreSQL...")
    
    # 1. 连接数据库
    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(POSTGRES_URL)
    pg_cursor = pg_conn.cursor()

    # 定义要迁移的表（按外键依赖顺序）
    tables = [
        "project_groups",
        "runs",
        "project_sheets",
        "person_metrics_base",
        "project_metrics_base",
        "poc_scores",
        "audit_logs"
    ]

    try:
        for table in tables:
            print(f"📦 正在迁移表: {table}...")
            
            # 读取 SQLite 数据
            sl_cursor = sqlite_conn.cursor()
            try:
                sl_cursor.execute(f"SELECT * FROM {table}")
                rows = sl_cursor.fetchall()
            except sqlite3.OperationalError:
                print(f"   ⚠️ 跳过表 {table} (SQLite 中不存在)")
                continue

            if not rows:
                print(f"   ℹ️ 表 {table} 为空，跳过")
                continue

            # 获取列名
            columns = rows[0].keys()
            col_names = ",".join(columns)
            placeholders = ",".join(["%s"] * len(columns))
            
            # 写入 PostgreSQL
            # 注意：如果 ID 是自增的，我们要保留原有的 ID
            insert_query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT (id) DO NOTHING"
            
            data = [tuple(row) for row in rows]
            execute_values(pg_cursor, f"INSERT INTO {table} ({col_names}) VALUES %s ON CONFLICT (id) DO NOTHING", data)
            
            # 更新自增序列（PostgreSQL 特有）
            pg_cursor.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), MAX(id)) FROM {table}")
            
            print(f"   ✅ 成功迁移 {len(rows)} 条记录")

        pg_conn.commit()
        print("\n✨ 迁移全部完成！旧数据均已导入 PostgreSQL。")

    except Exception as e:
        pg_conn.rollback()
        print(f"\n❌ 迁移失败: {str(e)}")
    finally:
        sqlite_conn.close()
        pg_conn.close()

if __name__ == "__main__":
    migrate()
