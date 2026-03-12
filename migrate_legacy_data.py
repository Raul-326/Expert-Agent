import sqlite3
import psycopg2
from psycopg2.extras import execute_values
import os
from datetime import datetime

# 配置路径
SOURCE_DB = "test_panel.db" 
POSTGRES_URL = "postgresql://agent_user:agent_password@localhost:5432/expert_agent"

def migrate():
    if not os.path.exists(SOURCE_DB):
        print(f"❌ 未找到源数据库文件: {SOURCE_DB}")
        return

    print(f"🚀 深度迁移: 从 {SOURCE_DB} 导入历史数据到 PostgreSQL...")
    
    # 1. 连接数据库
    sqlite_conn = sqlite3.connect(SOURCE_DB)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(POSTGRES_URL)
    pg_cursor = pg_conn.cursor()

    try:
        # --- A. 迁移项目 ---
        print("📦 正在迁移项目 (Projects -> ProjectGroups)...")
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT * FROM projects")
        old_projects = sqlite_cursor.fetchall()
        
        project_id_map = {} 
        for op in old_projects:
            pg_cursor.execute(
                """INSERT INTO project_groups (project_group_name, spreadsheet_token, poc_name, created_at)
                   VALUES (%s, %s, %s, %s) RETURNING id""",
                (op['project_group_name'], op['spreadsheet_token'], op['display_name'] or 'Legacy', op['updated_at'] or datetime.now())
            )
            new_id = pg_cursor.fetchone()[0]
            project_id_map[op['project_id']] = new_id
        
        print(f"   ✅ 已同步 {len(old_projects)} 个项目组")

        # --- B. 迁移 Run ---
        print("📦 正在迁移运行批次 (Runs)...")
        sqlite_cursor.execute("SELECT * FROM runs")
        old_runs = sqlite_cursor.fetchall()
        
        run_id_map = {} 
        for orun in old_runs:
            old_pid = orun['project_id']
            if old_pid not in project_id_map:
                continue
            
            new_pid = project_id_map[old_pid]
            pg_cursor.execute(
                """INSERT INTO runs (project_group_id, batch_project_name, batch_no, run_at, status)
                   VALUES (%s, %s, %s, %s, %s) RETURNING id""",
                (new_pid, None, orun['run_id'], orun['run_at'], 'COMPLETED')
            )
            new_rid = pg_cursor.fetchone()[0]
            run_id_map[orun['run_id']] = new_rid

        print(f"   ✅ 已同步 {len(run_id_map)} 个批次记录")

        # --- C. 迁移人员指标 ---
        print("📦 正在迁移人员效能数据 (Person Metrics)...")
        sqlite_cursor.execute("SELECT * FROM person_metrics_base")
        old_metrics = sqlite_cursor.fetchall()
        
        # 检查是否有 difficulty_coef 列
        keys = old_metrics[0].keys() if old_metrics else []
        has_diff = 'difficulty_coef' in keys

        metrics_to_insert = []
        for om in old_metrics:
            old_rid = om['run_id']
            if old_rid not in run_id_map:
                continue
            
            new_rid = run_id_map[old_rid]
            diff = float(om['difficulty_coef']) if (has_diff and om['difficulty_coef'] is not None) else 1.0
            
            metrics_to_insert.append((
                new_rid,
                om['person_name'],
                om['role'],
                int(om['volume'] or 0),
                int(om['inspected_count'] or 0),
                int(om['pass_count'] or 0),
                float(om['accuracy'] or 0.0),
                float(om['weighted_accuracy'] or 0.0),
                diff
            ))
        
        if metrics_to_insert:
            execute_values(
                pg_cursor,
                """INSERT INTO person_metrics_base 
                   (run_id, person_name, role, volume, inspected_count, pass_count, accuracy, weighted_accuracy, difficulty_coef)
                   VALUES %s""",
                metrics_to_insert
            )

        print(f"   ✅ 已同步 {len(metrics_to_insert)} 条人员指标")

        pg_conn.commit()
        print("\n✨ 迁移成功！PostgreSQL 现在拥有了所有 test_panel 中的历史记录。")

    except Exception as e:
        pg_conn.rollback()
        print(f"\n❌ 迁移失败: {str(e)}")
    finally:
        sqlite_conn.close()
        pg_conn.close()

if __name__ == "__main__":
    migrate()
