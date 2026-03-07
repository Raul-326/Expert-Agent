#!/usr/bin/env python3
"""SQLite 存储层：项目运行快照、覆盖值、审计日志与查询接口。"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from panel_metrics import compute_effective_person_overall, compute_effective_project_metrics, safe_float


BATCH_NAME_PATTERN = re.compile(r"^(?P<base>.+?)\s(?P<batch>\d{3})$")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_projects_migration(conn: sqlite3.Connection) -> None:
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(projects)").fetchall()}

    if "project_group_id" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN project_group_id TEXT")
    if "project_group_name" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN project_group_name TEXT")
    if "sheet_title" not in cols:
        conn.execute("ALTER TABLE projects ADD COLUMN sheet_title TEXT")

    conn.execute(
        """
        UPDATE projects
        SET project_group_id = spreadsheet_token
        WHERE project_group_id IS NULL OR TRIM(project_group_id) = ''
        """
    )
    conn.execute(
        """
        UPDATE projects
        SET project_group_name = COALESCE(NULLIF(project_group_name, ''), spreadsheet_token)
        WHERE project_group_name IS NULL OR TRIM(project_group_name) = ''
        """
    )
    conn.execute(
        """
        UPDATE projects
        SET sheet_title = COALESCE(NULLIF(sheet_title, ''), sheet_ref)
        WHERE sheet_title IS NULL OR TRIM(sheet_title) = ''
        """
    )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_projects_group ON projects(project_group_id)")


def init_db(db_path: str = "./metrics_panel.db") -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA foreign_keys=ON;

            CREATE TABLE IF NOT EXISTS projects (
                project_id TEXT PRIMARY KEY,
                project_group_id TEXT,
                project_group_name TEXT,
                spreadsheet_token TEXT NOT NULL,
                sheet_ref TEXT NOT NULL,
                sheet_title TEXT,
                display_name TEXT,
                result_spreadsheet_token TEXT,
                result_sheet_ref TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                run_at TEXT NOT NULL,
                difficulty_coef REAL,
                source_type TEXT NOT NULL,
                raw_meta_json TEXT,
                FOREIGN KEY(project_id) REFERENCES projects(project_id)
            );

            CREATE INDEX IF NOT EXISTS idx_runs_project_runat ON runs(project_id, run_at DESC);

            CREATE TABLE IF NOT EXISTS person_metrics_base (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                person_name TEXT,
                role TEXT,
                volume REAL,
                inspected_count REAL,
                pass_count REAL,
                accuracy REAL,
                weighted_accuracy REAL,
                difficulty_coef REAL,
                UNIQUE(run_id, project_id, person_name, role),
                FOREIGN KEY(run_id) REFERENCES runs(run_id),
                FOREIGN KEY(project_id) REFERENCES projects(project_id)
            );

            CREATE INDEX IF NOT EXISTS idx_person_metrics_project_role ON person_metrics_base(project_id, role);
            CREATE INDEX IF NOT EXISTS idx_person_metrics_person ON person_metrics_base(person_name);

            CREATE TABLE IF NOT EXISTS project_metrics_base (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                metric_group TEXT NOT NULL,
                volume_total REAL,
                inspected_total REAL,
                pass_total REAL,
                accuracy REAL,
                weighted_accuracy REAL,
                difficulty_coef REAL,
                UNIQUE(run_id, project_id, metric_group),
                FOREIGN KEY(run_id) REFERENCES runs(run_id),
                FOREIGN KEY(project_id) REFERENCES projects(project_id)
            );

            CREATE TABLE IF NOT EXISTS overrides (
                override_id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id TEXT NOT NULL,
                person_name TEXT,
                role TEXT,
                metric_key TEXT NOT NULL,
                override_value TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                updated_by TEXT,
                reason TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(project_id) REFERENCES projects(project_id)
            );

            CREATE INDEX IF NOT EXISTS idx_overrides_project_active ON overrides(project_id, is_active);

            CREATE TABLE IF NOT EXISTS audit_logs (
                audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_key TEXT NOT NULL,
                before_json TEXT,
                after_json TEXT,
                updated_by TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_audit_updated_at ON audit_logs(updated_at DESC);

            CREATE TABLE IF NOT EXISTS agent_jobs (
                job_id TEXT PRIMARY KEY,
                project_group_id TEXT,
                request_json TEXT,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                error TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_agent_jobs_group ON agent_jobs(project_group_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS agent_skill_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                skill_name TEXT NOT NULL,
                skill_version TEXT,
                status TEXT NOT NULL,
                input_hash TEXT,
                output_json TEXT,
                error TEXT,
                started_at TEXT,
                finished_at TEXT,
                FOREIGN KEY(job_id) REFERENCES agent_jobs(job_id)
            );

            CREATE INDEX IF NOT EXISTS idx_agent_skill_runs_job ON agent_skill_runs(job_id, id DESC);

            CREATE TABLE IF NOT EXISTS project_poc_scores (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                project_group_id TEXT NOT NULL,
                project_owner TEXT,
                sop_score REAL,
                sheet_score REAL,
                total_score REAL,
                grade TEXT,
                sop_source_type TEXT,
                model_name TEXT,
                prompt_version TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES agent_jobs(job_id)
            );

            CREATE INDEX IF NOT EXISTS idx_project_poc_scores_group ON project_poc_scores(project_group_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS project_poc_score_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                score_id INTEGER NOT NULL,
                section TEXT NOT NULL,
                reason_text TEXT,
                evidence_json TEXT,
                raw_model_output_json TEXT,
                FOREIGN KEY(score_id) REFERENCES project_poc_scores(id)
            );

            CREATE INDEX IF NOT EXISTS idx_project_poc_score_details_score ON project_poc_score_details(score_id, id);

            CREATE TABLE IF NOT EXISTS poc_score_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                score_id INTEGER NOT NULL,
                before_score_json TEXT,
                after_score_json TEXT,
                updated_by TEXT,
                reason TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(score_id) REFERENCES project_poc_scores(id)
            );

            CREATE INDEX IF NOT EXISTS idx_poc_score_overrides_score ON poc_score_overrides(score_id, updated_at DESC);
            """
        )
        _ensure_projects_migration(conn)


def _to_dict_rows(rows: List[sqlite3.Row]) -> List[Dict[str, Any]]:
    return [dict(r) for r in rows]


def _get_latest_run_row(conn: sqlite3.Connection, project_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        "SELECT * FROM runs WHERE project_id=? ORDER BY run_at DESC LIMIT 1",
        (project_id,),
    ).fetchone()
    return dict(row) if row else None


def _fetch_active_overrides(conn: sqlite3.Connection, project_id: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM overrides WHERE project_id=? AND is_active=1 ORDER BY updated_at DESC",
        (project_id,),
    ).fetchall()
    return _to_dict_rows(rows)


def parse_batch_project_name(name: str) -> Dict[str, Optional[str]]:
    batch_project_name = str(name or "").strip()
    if not batch_project_name:
        return {
            "batch_project_name": "",
            "logical_project_name": "",
            "batch_no": None,
        }

    m = BATCH_NAME_PATTERN.match(batch_project_name)
    if not m:
        return {
            "batch_project_name": batch_project_name,
            "logical_project_name": batch_project_name,
            "batch_no": None,
        }

    return {
        "batch_project_name": batch_project_name,
        "logical_project_name": m.group("base").strip(),
        "batch_no": m.group("batch"),
    }


def save_run_snapshot(snapshot: Dict[str, Any], db_path: str = "./metrics_panel.db") -> str:
    """保存 workflow 运行快照，返回 run_id。"""
    init_db(db_path)

    project_meta = snapshot.get("project_meta", {})
    project_id = snapshot.get("project_id") or project_meta.get("project_id")
    if not project_id:
        raise ValueError("snapshot 缺少 project_id")

    run_meta = snapshot.get("run_meta", {})
    run_id = snapshot.get("run_id") or run_meta.get("run_id")
    if not run_id:
        raise ValueError("snapshot 缺少 run_id")

    now = _now_iso()
    run_at = snapshot.get("run_at") or run_meta.get("run_at") or now
    difficulty = safe_float(snapshot.get("difficulty_coef", run_meta.get("difficulty_coef")))

    person_rows = snapshot.get("person_metrics_base", [])
    project_rows = snapshot.get("project_metrics_base", [])

    with _connect(db_path) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO projects (
                project_id, project_group_id, project_group_name,
                spreadsheet_token, sheet_ref, sheet_title, display_name,
                result_spreadsheet_token, result_sheet_ref, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                project_group_id=excluded.project_group_id,
                project_group_name=excluded.project_group_name,
                spreadsheet_token=excluded.spreadsheet_token,
                sheet_ref=excluded.sheet_ref,
                sheet_title=excluded.sheet_title,
                display_name=excluded.display_name,
                result_spreadsheet_token=excluded.result_spreadsheet_token,
                result_sheet_ref=excluded.result_sheet_ref,
                updated_at=excluded.updated_at
            """,
            (
                project_id,
                project_meta.get("project_group_id") or project_meta.get("spreadsheet_token", ""),
                project_meta.get("project_group_name") or project_meta.get("spreadsheet_token", ""),
                project_meta.get("spreadsheet_token", ""),
                project_meta.get("sheet_ref", ""),
                project_meta.get("sheet_title") or project_meta.get("sheet_ref", ""),
                project_meta.get("display_name"),
                project_meta.get("result_spreadsheet_token"),
                project_meta.get("result_sheet_ref"),
                now,
                now,
            ),
        )

        cur.execute(
            """
            INSERT OR REPLACE INTO runs (run_id, project_id, run_at, difficulty_coef, source_type, raw_meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                project_id,
                run_at,
                difficulty,
                run_meta.get("source_type", "workflow_feishu"),
                json.dumps(run_meta, ensure_ascii=False),
            ),
        )

        for r in person_rows:
            cur.execute(
                """
                INSERT OR REPLACE INTO person_metrics_base (
                    run_id, project_id, person_name, role,
                    volume, inspected_count, pass_count,
                    accuracy, weighted_accuracy, difficulty_coef
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    project_id,
                    r.get("person_name"),
                    r.get("role"),
                    safe_float(r.get("volume")),
                    safe_float(r.get("inspected_count")),
                    safe_float(r.get("pass_count")),
                    safe_float(r.get("accuracy")),
                    safe_float(r.get("weighted_accuracy")),
                    safe_float(r.get("difficulty_coef", difficulty)),
                ),
            )

        if not project_rows and person_rows:
            computed = compute_effective_project_metrics(person_rows, overrides=[])
            project_rows = computed.get("project_metrics", [])

        for pr in project_rows:
            cur.execute(
                """
                INSERT OR REPLACE INTO project_metrics_base (
                    run_id, project_id, metric_group,
                    volume_total, inspected_total, pass_total,
                    accuracy, weighted_accuracy, difficulty_coef
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    project_id,
                    pr.get("metric_group"),
                    safe_float(pr.get("volume_total")),
                    safe_float(pr.get("inspected_total")),
                    safe_float(pr.get("pass_total")),
                    safe_float(pr.get("accuracy")),
                    safe_float(pr.get("weighted_accuracy")),
                    safe_float(pr.get("difficulty_coef", difficulty)),
                ),
            )

    return run_id


def _aggregate_project_metrics_from_person_rows(person_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    agg_by_role: Dict[str, Dict[str, Any]] = {}

    for row in person_rows:
        role = row.get("role") or "未知"
        item = agg_by_role.setdefault(
            role,
            {
                "volume_total": 0.0,
                "inspected_total": 0.0,
                "pass_total": 0.0,
                "weighted_num": 0.0,
                "weighted_den": 0.0,
            },
        )

        volume = safe_float(row.get("volume"))
        inspected = safe_float(row.get("inspected_count"))
        passed = safe_float(row.get("pass_count"))
        weighted_acc = safe_float(row.get("weighted_accuracy"))

        if volume is not None:
            item["volume_total"] += volume
        if inspected is not None:
            item["inspected_total"] += inspected
        if passed is not None:
            item["pass_total"] += passed
        if inspected is not None and inspected > 0 and weighted_acc is not None:
            item["weighted_num"] += weighted_acc * inspected
            item["weighted_den"] += inspected

    project_metrics: List[Dict[str, Any]] = []
    overall_volume = 0.0
    overall_inspected = 0.0
    overall_pass = 0.0
    overall_weighted_num = 0.0
    overall_weighted_den = 0.0

    for role, item in agg_by_role.items():
        inspected = item["inspected_total"]
        passed = item["pass_total"]
        accuracy = (passed / inspected) if inspected > 0 else None
        weighted = (item["weighted_num"] / item["weighted_den"]) if item["weighted_den"] > 0 else None

        project_metrics.append(
            {
                "metric_group": role,
                "volume_total": item["volume_total"],
                "inspected_total": inspected,
                "pass_total": passed,
                "accuracy": accuracy,
                "weighted_accuracy": weighted,
                "difficulty_coef": None,
            }
        )

        if role in {"初标", "质检"}:
            overall_volume += item["volume_total"]
            overall_inspected += inspected
            overall_pass += passed
            overall_weighted_num += item["weighted_num"]
            overall_weighted_den += item["weighted_den"]

    overall_accuracy = (overall_pass / overall_inspected) if overall_inspected > 0 else None
    overall_weighted = (overall_weighted_num / overall_weighted_den) if overall_weighted_den > 0 else None
    project_metrics.append(
        {
            "metric_group": "整体",
            "volume_total": overall_volume,
            "inspected_total": overall_inspected,
            "pass_total": overall_pass,
            "accuracy": overall_accuracy,
            "weighted_accuracy": overall_weighted,
            "difficulty_coef": None,
        }
    )
    return project_metrics


def apply_override(
    db_path: str,
    project_id: str,
    metric_key: str,
    override_value: Any,
    person_name: Optional[str] = None,
    role: Optional[str] = None,
    updated_by: str = "panel",
    reason: str = "",
    is_active: bool = True,
) -> int:
    """新增/更新覆盖，并写审计日志。返回 override_id。"""
    init_db(db_path)
    now = _now_iso()

    with _connect(db_path) as conn:
        cur = conn.cursor()

        old = cur.execute(
            """
            SELECT * FROM overrides
            WHERE project_id=? AND IFNULL(person_name,'')=IFNULL(?, '')
              AND IFNULL(role,'')=IFNULL(?, '') AND metric_key=? AND is_active=1
            ORDER BY updated_at DESC LIMIT 1
            """,
            (project_id, person_name, role, metric_key),
        ).fetchone()

        before_json = json.dumps(dict(old), ensure_ascii=False) if old else None

        if old:
            override_id = int(old["override_id"])
            cur.execute(
                """
                UPDATE overrides
                SET override_value=?, is_active=?, updated_by=?, reason=?, updated_at=?
                WHERE override_id=?
                """,
                (str(override_value), 1 if is_active else 0, updated_by, reason, now, override_id),
            )
            action = "update_override"
        else:
            cur.execute(
                """
                INSERT INTO overrides (
                    project_id, person_name, role, metric_key,
                    override_value, is_active, updated_by, reason, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (project_id, person_name, role, metric_key, str(override_value), 1 if is_active else 0, updated_by, reason, now),
            )
            override_id = int(cur.lastrowid)
            action = "create_override"

        new_row = cur.execute("SELECT * FROM overrides WHERE override_id=?", (override_id,)).fetchone()
        after_json = json.dumps(dict(new_row), ensure_ascii=False) if new_row else None

        cur.execute(
            """
            INSERT INTO audit_logs (
                action, target_type, target_key,
                before_json, after_json, updated_by, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                action,
                "override",
                f"override_id={override_id}",
                before_json,
                after_json,
                updated_by,
                now,
            ),
        )

    return override_id


def deactivate_override(db_path: str, override_id: int, updated_by: str = "panel", reason: str = "") -> None:
    init_db(db_path)
    now = _now_iso()

    with _connect(db_path) as conn:
        cur = conn.cursor()
        old = cur.execute("SELECT * FROM overrides WHERE override_id=?", (override_id,)).fetchone()
        if not old:
            return

        before_json = json.dumps(dict(old), ensure_ascii=False)

        cur.execute(
            "UPDATE overrides SET is_active=0, updated_by=?, reason=?, updated_at=? WHERE override_id=?",
            (updated_by, reason, now, override_id),
        )

        new_row = cur.execute("SELECT * FROM overrides WHERE override_id=?", (override_id,)).fetchone()
        after_json = json.dumps(dict(new_row), ensure_ascii=False) if new_row else None

        cur.execute(
            """
            INSERT INTO audit_logs (
                action, target_type, target_key,
                before_json, after_json, updated_by, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "delete_override",
                "override",
                f"override_id={override_id}",
                before_json,
                after_json,
                updated_by,
                now,
            ),
        )


def list_projects(
    db_path: str,
    project_keyword: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    role: Optional[str] = None,
    vendor_suffix: Optional[str] = None,
) -> List[Dict[str, Any]]:
    init_db(db_path)

    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT p.*,
                   r.run_id AS latest_run_id,
                   r.run_at AS latest_run_at,
                   r.difficulty_coef AS latest_difficulty_coef
            FROM projects p
            LEFT JOIN runs r
              ON r.run_id = (
                SELECT r2.run_id FROM runs r2
                WHERE r2.project_id = p.project_id
                ORDER BY r2.run_at DESC
                LIMIT 1
              )
            ORDER BY r.run_at DESC
            """
        ).fetchall()

        out = []
        for row in rows:
            item = dict(row)
            if project_keyword:
                k = project_keyword.lower()
                if k not in (item.get("project_id") or "").lower() and k not in (item.get("display_name") or "").lower():
                    continue

            run_at = item.get("latest_run_at")
            if date_from and run_at and run_at < date_from:
                continue
            if date_to and run_at and run_at > date_to:
                continue

            detail = get_project_detail(
                db_path,
                item["project_id"],
                role_filter=role,
                vendor_suffix=vendor_suffix,
            )

            pm = detail.get("project_metrics", [])
            if role and role in {"初标", "质检", "POC"}:
                target = next((x for x in pm if x.get("metric_group") == role), None)
            else:
                target = next((x for x in pm if x.get("metric_group") == "整体"), None)

            people = detail.get("person_metrics", [])
            person_count = len({p.get("person_name") for p in people if p.get("person_name")})

            item["person_count"] = person_count
            item["project_accuracy"] = target.get("accuracy") if target else None
            item["project_weighted_accuracy"] = target.get("weighted_accuracy") if target else None
            out.append(item)

    return out


def rename_project_groups(
    db_path: str,
    rename_map: Dict[str, str],
    updated_by: str = "panel",
    reason: str = "manual_project_group_rename",
) -> int:
    """按 project_group_id 批量改名，并写审计日志。"""
    init_db(db_path)
    if not rename_map:
        return 0

    now = _now_iso()
    changed = 0
    with _connect(db_path) as conn:
        cur = conn.cursor()
        for group_id, new_name in (rename_map or {}).items():
            gid = str(group_id or "").strip()
            new_group_name = str(new_name or "").strip()
            if not gid or not new_group_name:
                continue

            row = cur.execute(
                """
                SELECT COALESCE(NULLIF(project_group_name, ''), spreadsheet_token) AS group_name
                FROM projects
                WHERE COALESCE(project_group_id, spreadsheet_token)=?
                LIMIT 1
                """,
                (gid,),
            ).fetchone()
            if not row:
                continue

            old_name = str(row["group_name"] or "").strip()
            if old_name == new_group_name:
                continue

            cur.execute(
                """
                UPDATE projects
                SET project_group_name=?, updated_at=?
                WHERE COALESCE(project_group_id, spreadsheet_token)=?
                """,
                (new_group_name, now, gid),
            )

            before_obj = {
                "project_group_id": gid,
                "project_group_name": old_name,
            }
            after_obj = {
                "project_group_id": gid,
                "project_group_name": new_group_name,
                "reason": reason,
            }
            cur.execute(
                """
                INSERT INTO audit_logs (
                    action, target_type, target_key,
                    before_json, after_json, updated_by, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "update_project_group_name",
                    "project_group_name_override",
                    f"project_group_id={gid}",
                    json.dumps(before_obj, ensure_ascii=False),
                    json.dumps(after_obj, ensure_ascii=False),
                    updated_by,
                    now,
                ),
            )
            changed += 1

    return changed


def list_project_groups(
    db_path: str,
    project_keyword: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    role: Optional[str] = None,
    vendor_suffix: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """按 spreadsheet_token 聚合项目。"""
    init_db(db_path)

    with _connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                COALESCE(p.project_group_id, p.spreadsheet_token) AS project_group_id,
                COALESCE(NULLIF(p.project_group_name, ''), p.spreadsheet_token) AS project_group_name
            FROM projects p
            GROUP BY
                COALESCE(p.project_group_id, p.spreadsheet_token),
                COALESCE(NULLIF(p.project_group_name, ''), p.spreadsheet_token)
            ORDER BY project_group_name
            """
        ).fetchall()

    out: List[Dict[str, Any]] = []
    for row in rows:
        group_id = row["project_group_id"]
        group_name = row["project_group_name"] or group_id
        parsed_name = parse_batch_project_name(group_name)
        logical_name = parsed_name["logical_project_name"] or group_name
        batch_no = parsed_name["batch_no"]

        if project_keyword:
            k = project_keyword.lower()
            if (
                k not in str(group_id).lower()
                and k not in str(group_name).lower()
                and k not in str(logical_name).lower()
                and k not in str(batch_no or "").lower()
            ):
                continue

        detail = get_project_group_detail(
            db_path=db_path,
            project_group_id=group_id,
            role_filter=role,
            vendor_suffix=vendor_suffix,
        )
        if not detail:
            continue

        latest_run_at = detail.get("latest_run_at")
        if date_from and latest_run_at and latest_run_at < date_from:
            continue
        if date_to and latest_run_at and latest_run_at > date_to:
            continue

        pm = detail.get("project_metrics", [])
        if role and role in {"初标", "质检", "POC"}:
            target = next((x for x in pm if x.get("metric_group") == role), None)
        else:
            target = next((x for x in pm if x.get("metric_group") == "整体"), None)

        out.append(
            {
                "project_group_id": group_id,
                "project_group_name": group_name,
                "batch_project_name": parsed_name["batch_project_name"] or group_name,
                "logical_project_name": logical_name,
                "batch_no": batch_no,
                "sheet_count": detail.get("sheet_count", 0),
                "latest_run_at": latest_run_at,
                "latest_difficulty_coef": detail.get("latest_difficulty_coef"),
                "person_count": detail.get("person_count", 0),
                "project_accuracy": target.get("accuracy") if target else None,
                "project_weighted_accuracy": target.get("weighted_accuracy") if target else None,
            }
        )

    out.sort(key=lambda x: x.get("latest_run_at") or "", reverse=True)
    return out


def list_logical_projects_for_detail(
    db_path: str,
    project_keyword: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    role: Optional[str] = None,
    vendor_suffix: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows = list_project_groups(
        db_path=db_path,
        project_keyword=project_keyword,
        date_from=date_from,
        date_to=date_to,
        role=role,
        vendor_suffix=vendor_suffix,
    )

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        logical_name = str(row.get("logical_project_name") or "").strip() or str(
            row.get("project_group_name") or row.get("project_group_id") or ""
        ).strip()
        item = grouped.setdefault(
            logical_name,
            {
                "logical_project_name": logical_name,
                "batch_count": 0,
                "sheet_count": 0,
                "latest_run_at": None,
                "batches": [],
            },
        )
        item["batch_count"] += 1
        item["sheet_count"] += int(row.get("sheet_count") or 0)
        run_at = row.get("latest_run_at")
        if run_at and (item["latest_run_at"] is None or run_at > item["latest_run_at"]):
            item["latest_run_at"] = run_at
        item["batches"].append(
            {
                "project_group_id": row.get("project_group_id"),
                "project_group_name": row.get("project_group_name"),
                "batch_project_name": row.get("batch_project_name"),
                "batch_no": row.get("batch_no"),
                "latest_run_at": row.get("latest_run_at"),
                "sheet_count": row.get("sheet_count", 0),
            }
        )

    out = list(grouped.values())
    out.sort(key=lambda x: x.get("latest_run_at") or "", reverse=True)
    return out


def _batch_sort_key(batch_item: Dict[str, Any]) -> Any:
    batch_no = str(batch_item.get("batch_no") or "").strip()
    batch_name = str(batch_item.get("batch_project_name") or batch_item.get("project_group_name") or "")
    if batch_no.isdigit():
        return (0, int(batch_no), batch_name)
    return (1, batch_name)


def get_logical_project_detail(
    db_path: str,
    logical_project_name: str,
    role_filter: Optional[str] = None,
    vendor_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    init_db(db_path)
    target = str(logical_project_name or "").strip()
    if not target:
        return {}

    groups = list_project_groups(
        db_path=db_path,
        role=role_filter,
        vendor_suffix=vendor_suffix,
    )
    matched = [g for g in groups if str(g.get("logical_project_name") or "").strip() == target]
    if not matched:
        return {}

    batches: List[Dict[str, Any]] = []
    combined_person_rows: List[Dict[str, Any]] = []
    latest_run_at: Optional[str] = None
    sheet_count = 0

    for g in matched:
        group_id = str(g.get("project_group_id") or "").strip()
        if not group_id:
            continue
        detail = get_project_group_detail(
            db_path=db_path,
            project_group_id=group_id,
            role_filter=role_filter,
            vendor_suffix=vendor_suffix,
        )
        if not detail:
            continue

        detail["batch_project_name"] = g.get("batch_project_name") or g.get("project_group_name")
        detail["logical_project_name"] = g.get("logical_project_name") or target
        detail["batch_no"] = g.get("batch_no")
        detail["project_group"] = detail.get("project_group", {})
        detail["project_group"]["batch_project_name"] = detail["batch_project_name"]
        detail["project_group"]["logical_project_name"] = detail["logical_project_name"]
        detail["project_group"]["batch_no"] = detail["batch_no"]
        batches.append(detail)

        sheet_count += int(detail.get("sheet_count") or 0)
        run_at = detail.get("latest_run_at")
        if run_at and (latest_run_at is None or run_at > latest_run_at):
            latest_run_at = run_at

        for s in detail.get("sheets", []):
            combined_person_rows.extend(s.get("person_metrics", []))

    if not batches:
        return {}

    batches.sort(key=_batch_sort_key)
    person_count = len({r.get("person_name") for r in combined_person_rows if r.get("person_name")})
    project_metrics = _aggregate_project_metrics_from_person_rows(combined_person_rows)

    return {
        "logical_project_name": target,
        "batch_count": len(batches),
        "sheet_count": sheet_count,
        "latest_run_at": latest_run_at,
        "person_count": person_count,
        "project_metrics": project_metrics,
        "batches": batches,
    }


def get_project_group_detail(
    db_path: str,
    project_group_id: str,
    role_filter: Optional[str] = None,
    vendor_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    """获取某项目组详情：顶部聚合 + 分 sheet 明细。"""
    init_db(db_path)

    with _connect(db_path) as conn:
        sheet_rows = conn.execute(
            """
            SELECT
                p.*,
                r.run_id AS latest_run_id,
                r.run_at AS latest_run_at,
                r.difficulty_coef AS latest_difficulty_coef
            FROM projects p
            LEFT JOIN runs r
              ON r.run_id = (
                SELECT r2.run_id FROM runs r2
                WHERE r2.project_id = p.project_id
                ORDER BY r2.run_at DESC
                LIMIT 1
              )
            WHERE COALESCE(p.project_group_id, p.spreadsheet_token) = ?
            ORDER BY COALESCE(NULLIF(p.sheet_title, ''), p.sheet_ref)
            """,
            (project_group_id,),
        ).fetchall()

    if not sheet_rows:
        return {}

    sheets: List[Dict[str, Any]] = []
    combined_person_rows: List[Dict[str, Any]] = []
    latest_run_at: Optional[str] = None
    latest_difficulty_coef: Optional[float] = None

    group_name = (
        sheet_rows[0]["project_group_name"]
        or sheet_rows[0]["spreadsheet_token"]
        or project_group_id
    )

    for row in sheet_rows:
        project_id = row["project_id"]
        detail = get_project_detail(
            db_path=db_path,
            project_id=project_id,
            role_filter=role_filter,
            vendor_suffix=vendor_suffix,
        )
        if not detail:
            continue

        run = detail.get("latest_run") or {}
        run_at = run.get("run_at")
        if run_at and (latest_run_at is None or run_at > latest_run_at):
            latest_run_at = run_at
            latest_difficulty_coef = run.get("difficulty_coef")

        combined_person_rows.extend(detail.get("person_metrics", []))
        sheets.append(detail)

    person_count = len({r.get("person_name") for r in combined_person_rows if r.get("person_name")})
    project_metrics = _aggregate_project_metrics_from_person_rows(combined_person_rows)

    return {
        "project_group": {
            "project_group_id": project_group_id,
            "project_group_name": group_name,
            "spreadsheet_token": sheet_rows[0]["spreadsheet_token"],
        },
        "sheet_count": len(sheet_rows),
        "person_count": person_count,
        "latest_run_at": latest_run_at,
        "latest_difficulty_coef": latest_difficulty_coef,
        "project_metrics": project_metrics,
        "sheets": sheets,
    }


def get_sheet_detail(
    db_path: str,
    project_id: str,
    role_filter: Optional[str] = None,
    vendor_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    """sheet 级详情（兼容别名）。"""
    return get_project_detail(
        db_path=db_path,
        project_id=project_id,
        role_filter=role_filter,
        vendor_suffix=vendor_suffix,
    )


def get_project_detail(
    db_path: str,
    project_id: str,
    role_filter: Optional[str] = None,
    vendor_suffix: Optional[str] = None,
) -> Dict[str, Any]:
    init_db(db_path)

    with _connect(db_path) as conn:
        p = conn.execute("SELECT * FROM projects WHERE project_id=?", (project_id,)).fetchone()
        if not p:
            return {}

        run = _get_latest_run_row(conn, project_id)
        if not run:
            return {
                "project": dict(p),
                "latest_run": None,
                "person_metrics": [],
                "project_metrics": [],
                "overrides": _fetch_active_overrides(conn, project_id),
            }

        base_rows = _to_dict_rows(
            conn.execute(
                "SELECT * FROM person_metrics_base WHERE run_id=? ORDER BY role, person_name",
                (run["run_id"],),
            ).fetchall()
        )

        overrides = _fetch_active_overrides(conn, project_id)
        computed = compute_effective_project_metrics(base_rows, overrides)
        person_metrics = computed.get("person_metrics", [])
        project_metrics = computed.get("project_metrics", [])

        if role_filter and role_filter != "全部":
            person_metrics = [x for x in person_metrics if x.get("role") == role_filter]

        if vendor_suffix and vendor_suffix != "全部":
            person_metrics = [
                x for x in person_metrics
                if x.get("person_name") and str(x.get("person_name")).endswith(vendor_suffix)
            ]

        return {
            "project": dict(p),
            "latest_run": run,
            "person_metrics": person_metrics,
            "project_metrics": project_metrics,
            "overrides": overrides,
        }


def _list_projects_latest_runs(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            p.project_id,
            p.display_name,
            p.sheet_ref,
            p.sheet_title,
            COALESCE(p.project_group_id, p.spreadsheet_token) AS project_group_id,
            COALESCE(NULLIF(p.project_group_name, ''), p.spreadsheet_token) AS project_group_name,
            r.run_id,
            r.run_at
        FROM projects p
        JOIN runs r
          ON r.run_id = (
            SELECT r2.run_id FROM runs r2
            WHERE r2.project_id=p.project_id
            ORDER BY r2.run_at DESC
            LIMIT 1
          )
        ORDER BY r.run_at ASC
        """
    ).fetchall()
    return _to_dict_rows(rows)


def get_person_overall(db_path: str, person_keyword: Optional[str] = None) -> List[Dict[str, Any]]:
    init_db(db_path)

    with _connect(db_path) as conn:
        latest_runs = _list_projects_latest_runs(conn)
        all_rows: List[Dict[str, Any]] = []

        for pr in latest_runs:
            project_id = pr["project_id"]
            run_id = pr["run_id"]
            base_rows = _to_dict_rows(
                conn.execute(
                    "SELECT * FROM person_metrics_base WHERE run_id=?",
                    (run_id,),
                ).fetchall()
            )
            overrides = _fetch_active_overrides(conn, project_id)
            computed = compute_effective_project_metrics(base_rows, overrides)
            all_rows.extend(computed.get("person_metrics", []))

        out = compute_effective_person_overall(all_rows)

        if person_keyword:
            k = person_keyword.lower()
            out = [x for x in out if k in str(x.get("person_name", "")).lower()]

        return out


def _list_latest_effective_person_rows(conn: sqlite3.Connection) -> List[Dict[str, Any]]:
    latest_runs = _list_projects_latest_runs(conn)
    all_rows: List[Dict[str, Any]] = []

    for pr in latest_runs:
        project_id = pr["project_id"]
        run_id = pr["run_id"]
        base_rows = _to_dict_rows(
            conn.execute(
                "SELECT * FROM person_metrics_base WHERE run_id=?",
                (run_id,),
            ).fetchall()
        )
        overrides = _fetch_active_overrides(conn, project_id)
        computed = compute_effective_project_metrics(base_rows, overrides)

        for row in computed.get("person_metrics", []):
            item = dict(row)
            item["project_group_id"] = pr.get("project_group_id")
            item["project_group_name"] = pr.get("project_group_name") or pr.get("project_group_id")
            item["sheet_ref"] = pr.get("sheet_ref")
            item["sheet_title"] = pr.get("sheet_title") or pr.get("sheet_ref") or project_id
            item["display_name"] = pr.get("display_name") or pr.get("project_group_name") or project_id
            item["run_at"] = pr.get("run_at")
            all_rows.append(item)

    return all_rows


def list_boss_project_cards(
    db_path: str,
    project_keyword: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> List[Dict[str, Any]]:
    rows = list_project_groups(
        db_path=db_path,
        project_keyword=project_keyword,
        date_from=date_from,
        date_to=date_to,
    )

    out: List[Dict[str, Any]] = []
    for row in rows:
        group_id = str(row.get("project_group_id") or "").strip()
        if not group_id:
            continue

        detail = get_project_group_detail(db_path=db_path, project_group_id=group_id)
        if not detail:
            continue

        overall = next((x for x in detail.get("project_metrics", []) if x.get("metric_group") == "整体"), None) or {}
        score_pack = get_latest_project_poc_score(db_path=db_path, project_group_id=group_id)
        score = score_pack.get("score") or {}

        out.append(
            {
                "project_group_id": group_id,
                "project_name": row.get("logical_project_name") or row.get("project_group_name") or group_id,
                "batch_project_name": row.get("batch_project_name") or row.get("project_group_name") or group_id,
                "batch_no": row.get("batch_no"),
                "date": row.get("latest_run_at"),
                "poc_name": score.get("project_owner") or None,
                "person_count": detail.get("person_count", 0),
                "overall_weighted_accuracy": overall.get("weighted_accuracy"),
                "total_volume": overall.get("volume_total"),
            }
        )

    out.sort(key=lambda x: x.get("date") or "", reverse=True)
    return out


def get_boss_project_detail(db_path: str, project_group_id: str) -> Dict[str, Any]:
    detail = get_project_group_detail(db_path=db_path, project_group_id=project_group_id)
    if not detail:
        return {}

    group = detail.get("project_group", {}) or {}
    group_name = group.get("project_group_name") or project_group_id
    parsed = parse_batch_project_name(group_name)
    overall = next((x for x in detail.get("project_metrics", []) if x.get("metric_group") == "整体"), None) or {}
    score_pack = get_latest_project_poc_score(db_path=db_path, project_group_id=project_group_id)
    score = score_pack.get("score") or {}

    combined_rows: List[Dict[str, Any]] = []
    for sheet in detail.get("sheets", []):
        combined_rows.extend(sheet.get("person_metrics", []))

    people = compute_effective_person_overall(combined_rows)
    people.sort(
        key=lambda x: (
            -(safe_float(x.get("volume_total")) or 0.0),
            -(safe_float(x.get("overall_accuracy")) or -1.0),
            str(x.get("person_name") or ""),
            str(x.get("role") or ""),
        )
    )

    return {
        "project_group_id": project_group_id,
        "project_name": parsed.get("logical_project_name") or group_name,
        "batch_project_name": parsed.get("batch_project_name") or group_name,
        "batch_no": parsed.get("batch_no"),
        "date": detail.get("latest_run_at"),
        "poc_name": score.get("project_owner") or None,
        "person_count": detail.get("person_count", 0),
        "overall_weighted_accuracy": overall.get("weighted_accuracy"),
        "total_volume": overall.get("volume_total"),
        "people": people,
    }


def list_boss_people(db_path: str, person_keyword: Optional[str] = None) -> List[Dict[str, Any]]:
    init_db(db_path)

    with _connect(db_path) as conn:
        rows = _list_latest_effective_person_rows(conn)

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        person_name = str(row.get("person_name") or "").strip()
        if not person_name:
            continue

        item = grouped.setdefault(
            person_name,
            {
                "person_name": person_name,
                "roles": set(),
                "project_groups": set(),
                "volume_total": 0.0,
                "inspected_total": 0.0,
                "pass_total": 0.0,
                "weighted_num": 0.0,
                "weighted_den": 0.0,
            },
        )

        role = str(row.get("role") or "").strip()
        if role:
            item["roles"].add(role)

        group_id = str(row.get("project_group_id") or row.get("project_id") or "").strip()
        if group_id:
            item["project_groups"].add(group_id)

        volume = safe_float(row.get("volume"))
        inspected = safe_float(row.get("inspected_count"))
        passed = safe_float(row.get("pass_count"))
        weighted = safe_float(row.get("weighted_accuracy"))

        if volume is not None:
            item["volume_total"] += volume
        if inspected is not None:
            item["inspected_total"] += inspected
        if passed is not None:
            item["pass_total"] += passed
        if inspected is not None and inspected > 0 and weighted is not None:
            item["weighted_num"] += weighted * inspected
            item["weighted_den"] += inspected

    out: List[Dict[str, Any]] = []
    for item in grouped.values():
        if person_keyword and person_keyword.lower() not in item["person_name"].lower():
            continue

        weighted_accuracy = (
            item["weighted_num"] / item["weighted_den"] if item["weighted_den"] > 0 else None
        )
        out.append(
            {
                "person_name": item["person_name"],
                "roles": " / ".join(sorted(item["roles"])),
                "project_count": len(item["project_groups"]),
                "volume_total": item["volume_total"],
                "inspected_total": item["inspected_total"],
                "pass_total": item["pass_total"],
                "overall_weighted_accuracy": weighted_accuracy,
            }
        )

    out.sort(
        key=lambda x: (
            -(safe_float(x.get("volume_total")) or 0.0),
            -(safe_float(x.get("overall_weighted_accuracy")) or -1.0),
            str(x.get("person_name") or ""),
        )
    )
    return out


def get_boss_person_detail(db_path: str, person_name: str) -> Dict[str, Any]:
    init_db(db_path)
    target = str(person_name or "").strip()
    if not target:
        return {}

    with _connect(db_path) as conn:
        rows = _list_latest_effective_person_rows(conn)

    filtered = [row for row in rows if str(row.get("person_name") or "").strip() == target]
    if not filtered:
        return {}

    grouped: Dict[tuple, Dict[str, Any]] = {}
    roles = set()

    for row in filtered:
        role = str(row.get("role") or "").strip()
        group_id = str(row.get("project_group_id") or row.get("project_id") or "").strip()
        key = (group_id, role)
        roles.add(role)

        item = grouped.setdefault(
            key,
            {
                "project_group_id": group_id,
                "project_name": row.get("project_group_name") or group_id,
                "role": role,
                "date": row.get("run_at"),
                "volume_total": 0.0,
                "inspected_total": 0.0,
                "pass_total": 0.0,
                "weighted_num": 0.0,
                "weighted_den": 0.0,
            },
        )

        run_at = row.get("run_at")
        if run_at and (item.get("date") is None or run_at > item["date"]):
            item["date"] = run_at

        volume = safe_float(row.get("volume"))
        inspected = safe_float(row.get("inspected_count"))
        passed = safe_float(row.get("pass_count"))
        weighted = safe_float(row.get("weighted_accuracy"))

        if volume is not None:
            item["volume_total"] += volume
        if inspected is not None:
            item["inspected_total"] += inspected
        if passed is not None:
            item["pass_total"] += passed
        if inspected is not None and inspected > 0 and weighted is not None:
            item["weighted_num"] += weighted * inspected
            item["weighted_den"] += inspected

    projects: List[Dict[str, Any]] = []
    total_volume = 0.0
    total_inspected = 0.0
    total_pass = 0.0
    total_weighted_num = 0.0
    total_weighted_den = 0.0

    for item in grouped.values():
        weighted_accuracy = (
            item["weighted_num"] / item["weighted_den"] if item["weighted_den"] > 0 else None
        )
        projects.append(
            {
                "project_group_id": item["project_group_id"],
                "project_name": item["project_name"],
                "role": item["role"],
                "date": item["date"],
                "volume_total": item["volume_total"],
                "inspected_total": item["inspected_total"],
                "pass_total": item["pass_total"],
                "weighted_accuracy": weighted_accuracy,
            }
        )
        total_volume += item["volume_total"]
        total_inspected += item["inspected_total"]
        total_pass += item["pass_total"]
        total_weighted_num += item["weighted_num"]
        total_weighted_den += item["weighted_den"]

    projects.sort(
        key=lambda x: (
            x.get("date") or "",
            str(x.get("project_name") or ""),
            str(x.get("role") or ""),
        )
    )

    return {
        "person_name": target,
        "roles": " / ".join(sorted(x for x in roles if x)),
        "project_count": len({x.get("project_group_id") for x in projects if x.get("project_group_id")}),
        "volume_total": total_volume,
        "inspected_total": total_inspected,
        "pass_total": total_pass,
        "overall_weighted_accuracy": (
            total_weighted_num / total_weighted_den if total_weighted_den > 0 else None
        ),
        "projects": projects,
    }


def get_person_project_series(
    db_path: str,
    person_name: str,
    role: str,
    granularity: str = "project",
) -> List[Dict[str, Any]]:
    init_db(db_path)

    with _connect(db_path) as conn:
        latest_runs = _list_projects_latest_runs(conn)
        sheet_points: List[Dict[str, Any]] = []

        for pr in latest_runs:
            project_id = pr["project_id"]
            run_id = pr["run_id"]
            base_rows = _to_dict_rows(
                conn.execute(
                    "SELECT * FROM person_metrics_base WHERE run_id=?",
                    (run_id,),
                ).fetchall()
            )
            overrides = _fetch_active_overrides(conn, project_id)
            computed = compute_effective_project_metrics(base_rows, overrides)

            rows = [
                r for r in computed.get("person_metrics", [])
                if r.get("person_name") == person_name and r.get("role") == role
            ]
            if not rows:
                continue

            r = rows[0]
            sheet_points.append({
                "project_id": project_id,
                "project_group_id": pr.get("project_group_id"),
                "project_group_name": pr.get("project_group_name") or pr.get("project_group_id"),
                "sheet_ref": pr.get("sheet_ref"),
                "sheet_title": pr.get("sheet_title") or pr.get("sheet_ref") or project_id,
                "display_name": pr.get("display_name") or pr.get("project_group_name") or project_id,
                "run_at": pr.get("run_at"),
                "volume": r.get("volume"),
                "accuracy": r.get("accuracy"),
                "weighted_accuracy": r.get("weighted_accuracy"),
                "inspected_count": r.get("inspected_count"),
                "pass_count": r.get("pass_count"),
            })

        gran = (granularity or "project").strip().lower()
        if gran == "sheet":
            sheet_points.sort(key=lambda x: x.get("run_at") or "")
            series: List[Dict[str, Any]] = []
            for idx, p in enumerate(sheet_points, start=1):
                row = dict(p)
                row["seq"] = idx
                series.append(row)
            return series

        grouped: Dict[str, Dict[str, Any]] = {}
        for p in sheet_points:
            gid = p.get("project_group_id") or p.get("project_id")
            g = grouped.setdefault(
                gid,
                {
                    "project_group_id": gid,
                    "project_group_name": p.get("project_group_name") or gid,
                    "run_at": p.get("run_at"),
                    "volume_total": 0.0,
                    "inspected_total": 0.0,
                    "pass_total": 0.0,
                    "weighted_num": 0.0,
                    "weighted_den": 0.0,
                },
            )

            if p.get("run_at") and (g.get("run_at") is None or p["run_at"] > g["run_at"]):
                g["run_at"] = p["run_at"]

            volume = safe_float(p.get("volume"))
            inspected = safe_float(p.get("inspected_count"))
            passed = safe_float(p.get("pass_count"))
            weighted = safe_float(p.get("weighted_accuracy"))

            if volume is not None:
                g["volume_total"] += volume
            if inspected is not None:
                g["inspected_total"] += inspected
            if passed is not None:
                g["pass_total"] += passed
            if inspected is not None and inspected > 0 and weighted is not None:
                g["weighted_num"] += weighted * inspected
                g["weighted_den"] += inspected

        project_points: List[Dict[str, Any]] = []
        for gid, g in grouped.items():
            inspected_total = g["inspected_total"]
            pass_total = g["pass_total"]
            accuracy = (pass_total / inspected_total) if inspected_total > 0 else None
            weighted_accuracy = (g["weighted_num"] / g["weighted_den"]) if g["weighted_den"] > 0 else None
            project_points.append(
                {
                    "project_id": gid,
                    "project_group_id": gid,
                    "project_group_name": g["project_group_name"],
                    "sheet_ref": None,
                    "sheet_title": None,
                    "display_name": g["project_group_name"],
                    "run_at": g["run_at"],
                    "volume": g["volume_total"],
                    "accuracy": accuracy,
                    "weighted_accuracy": weighted_accuracy,
                    "inspected_count": inspected_total,
                    "pass_count": pass_total,
                }
            )

        project_points.sort(key=lambda x: x.get("run_at") or "")
        series: List[Dict[str, Any]] = []
        for idx, p in enumerate(project_points, start=1):
            row = dict(p)
            row["seq"] = idx
            series.append(row)
        return series


def list_active_overrides(
    db_path: str,
    project_id: Optional[str] = None,
    person_name: Optional[str] = None,
    role: Optional[str] = None,
) -> List[Dict[str, Any]]:
    init_db(db_path)

    sql = "SELECT * FROM overrides WHERE is_active=1"
    args: List[Any] = []
    if project_id:
        sql += " AND project_id=?"
        args.append(project_id)
    if person_name:
        sql += " AND person_name=?"
        args.append(person_name)
    if role:
        sql += " AND role=?"
        args.append(role)
    sql += " ORDER BY updated_at DESC"

    with _connect(db_path) as conn:
        rows = conn.execute(sql, tuple(args)).fetchall()
    return _to_dict_rows(rows)


def list_audit_logs(
    db_path: str,
    project_id: Optional[str] = None,
    person_name: Optional[str] = None,
    updated_by: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    init_db(db_path)

    sql = "SELECT * FROM audit_logs WHERE 1=1"
    args: List[Any] = []

    if updated_by:
        sql += " AND updated_by=?"
        args.append(updated_by)

    # project/person 过滤基于 target_key/before/after 文本匹配
    if project_id:
        sql += " AND (target_key LIKE ? OR before_json LIKE ? OR after_json LIKE ?)"
        pat = f"%{project_id}%"
        args.extend([pat, pat, pat])

    if person_name:
        sql += " AND (before_json LIKE ? OR after_json LIKE ?)"
        pat = f"%{person_name}%"
        args.extend([pat, pat])

    sql += " ORDER BY updated_at DESC LIMIT ?"
    args.append(limit)

    with _connect(db_path) as conn:
        rows = conn.execute(sql, tuple(args)).fetchall()
    return _to_dict_rows(rows)


def create_agent_job(
    db_path: str,
    job_id: str,
    project_group_id: str,
    request_json: Dict[str, Any],
    status: str = "created",
) -> None:
    init_db(db_path)
    now = _now_iso()
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO agent_jobs (
                job_id, project_group_id, request_json, status,
                created_at, started_at, finished_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                project_group_id=excluded.project_group_id,
                request_json=excluded.request_json,
                status=excluded.status,
                started_at=COALESCE(excluded.started_at, agent_jobs.started_at),
                finished_at=COALESCE(excluded.finished_at, agent_jobs.finished_at),
                error=excluded.error
            """,
            (
                job_id,
                project_group_id,
                json.dumps(request_json, ensure_ascii=False),
                status,
                now,
                now if status == "running" else None,
                now if status in {"success", "failed"} else None,
                None,
            ),
        )


def update_agent_job_status(
    db_path: str,
    job_id: str,
    status: str,
    error: str = "",
) -> None:
    init_db(db_path)
    now = _now_iso()
    started_at = now if status == "running" else None
    finished_at = now if status in {"success", "failed"} else None
    with _connect(db_path) as conn:
        conn.execute(
            """
            UPDATE agent_jobs
            SET status=?,
                started_at=COALESCE(?, started_at),
                finished_at=COALESCE(?, finished_at),
                error=?
            WHERE job_id=?
            """,
            (status, started_at, finished_at, error or None, job_id),
        )


def save_agent_skill_run(
    db_path: str,
    job_id: str,
    skill_name: str,
    skill_version: str,
    status: str,
    input_payload: Optional[Dict[str, Any]] = None,
    output_payload: Optional[Dict[str, Any]] = None,
    error: str = "",
) -> int:
    init_db(db_path)
    now = _now_iso()
    input_hash = None
    if input_payload:
        try:
            input_hash = str(abs(hash(json.dumps(input_payload, ensure_ascii=False, sort_keys=True))))
        except Exception:
            input_hash = None
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO agent_skill_runs (
                job_id, skill_name, skill_version, status,
                input_hash, output_json, error, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                skill_name,
                skill_version,
                status,
                input_hash,
                json.dumps(output_payload or {}, ensure_ascii=False),
                error or None,
                now,
                now,
            ),
        )
        return int(cur.lastrowid)


def save_project_poc_score(
    db_path: str,
    job_id: str,
    project_group_id: str,
    project_owner: str,
    sop_score: Optional[float],
    sheet_score: Optional[float],
    total_score: Optional[float],
    grade: str,
    sop_source_type: str,
    model_name: str,
    prompt_version: str,
    sop_reason: str = "",
    sop_evidence: Optional[List[Any]] = None,
    sop_raw_output: Optional[Dict[str, Any]] = None,
    sheet_reason: str = "",
    sheet_evidence: Optional[List[Any]] = None,
    sheet_raw_output: Optional[Dict[str, Any]] = None,
) -> int:
    init_db(db_path)
    now = _now_iso()
    with _connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO project_poc_scores (
                job_id, project_group_id, project_owner, sop_score, sheet_score,
                total_score, grade, sop_source_type, model_name, prompt_version, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job_id,
                project_group_id,
                project_owner or None,
                safe_float(sop_score),
                safe_float(sheet_score),
                safe_float(total_score),
                grade or None,
                sop_source_type or None,
                model_name or None,
                prompt_version or None,
                now,
            ),
        )
        score_id = int(cur.lastrowid)

        cur.execute(
            """
            INSERT INTO project_poc_score_details (
                score_id, section, reason_text, evidence_json, raw_model_output_json
            ) VALUES (?, 'sop', ?, ?, ?)
            """,
            (
                score_id,
                sop_reason or "",
                json.dumps(sop_evidence or [], ensure_ascii=False),
                json.dumps(sop_raw_output or {}, ensure_ascii=False),
            ),
        )
        cur.execute(
            """
            INSERT INTO project_poc_score_details (
                score_id, section, reason_text, evidence_json, raw_model_output_json
            ) VALUES (?, 'sheet', ?, ?, ?)
            """,
            (
                score_id,
                sheet_reason or "",
                json.dumps(sheet_evidence or [], ensure_ascii=False),
                json.dumps(sheet_raw_output or {}, ensure_ascii=False),
            ),
        )
    return score_id


def _grade_from_total(total_score: Optional[float]) -> str:
    s = safe_float(total_score)
    if s is None:
        return ""
    if s >= 85:
        return "A"
    if s >= 70:
        return "B"
    if s >= 55:
        return "C"
    return "D"


def _safe_json_loads(text: Any) -> Dict[str, Any]:
    if text is None:
        return {}
    if isinstance(text, dict):
        return dict(text)
    raw = str(text).strip()
    if not raw:
        return {}
    try:
        val = json.loads(raw)
        if isinstance(val, dict):
            return val
    except Exception:
        pass
    return {}


def _load_score_row(conn: sqlite3.Connection, score_id: int) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM project_poc_scores WHERE id=?", (score_id,)).fetchone()
    return dict(row) if row else None


def _load_latest_score_row(conn: sqlite3.Connection, project_group_id: str) -> Optional[Dict[str, Any]]:
    row = conn.execute(
        """
        SELECT * FROM project_poc_scores
        WHERE project_group_id=?
        ORDER BY created_at DESC, id DESC
        LIMIT 1
        """,
        (project_group_id,),
    ).fetchone()
    return dict(row) if row else None


def _apply_latest_override(conn: sqlite3.Connection, score_row: Dict[str, Any]) -> Dict[str, Any]:
    if not score_row:
        return {}
    score_id = int(score_row["id"])
    ov = conn.execute(
        """
        SELECT * FROM poc_score_overrides
        WHERE score_id=?
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
        """,
        (score_id,),
    ).fetchone()
    if not ov:
        return score_row
    after = _safe_json_loads(ov["after_score_json"])
    merged = dict(score_row)
    merged.update(after)
    merged["override_id"] = int(ov["id"])
    merged["override_updated_by"] = ov["updated_by"]
    merged["override_updated_at"] = ov["updated_at"]
    return merged


def get_latest_project_poc_score(db_path: str, project_group_id: str) -> Dict[str, Any]:
    init_db(db_path)
    with _connect(db_path) as conn:
        base = _load_latest_score_row(conn, project_group_id)
        if not base:
            return {}
        score = _apply_latest_override(conn, base)
        details = _to_dict_rows(
            conn.execute(
                "SELECT * FROM project_poc_score_details WHERE score_id=? ORDER BY id",
                (base["id"],),
            ).fetchall()
        )
    return {
        "score": score,
        "details": details,
    }


def list_poc_score_overrides(db_path: str, score_id: int) -> List[Dict[str, Any]]:
    init_db(db_path)
    with _connect(db_path) as conn:
        rows = conn.execute(
            "SELECT * FROM poc_score_overrides WHERE score_id=? ORDER BY updated_at DESC, id DESC",
            (score_id,),
        ).fetchall()
    return _to_dict_rows(rows)


def apply_poc_score_override(
    db_path: str,
    score_id: int,
    updated_by: str,
    reason: str,
    override_fields: Dict[str, Any],
) -> int:
    init_db(db_path)
    now = _now_iso()
    with _connect(db_path) as conn:
        cur = conn.cursor()
        base = _load_score_row(conn, score_id)
        if not base:
            raise ValueError(f"score_id 不存在: {score_id}")
        effective_before = _apply_latest_override(conn, base)
        before_json = json.dumps(effective_before, ensure_ascii=False)

        after = dict(effective_before)
        for k, v in (override_fields or {}).items():
            if k in {"sop_score", "sheet_score", "total_score"}:
                after[k] = safe_float(v)
            elif k in {"project_owner", "grade", "sop_source_type", "model_name", "prompt_version"}:
                after[k] = ("" if v is None else str(v))

        if after.get("total_score") is None:
            sop = safe_float(after.get("sop_score"))
            sheet = safe_float(after.get("sheet_score"))
            if sop is not None and sheet is not None:
                after["total_score"] = round((sop * 0.5) + (sheet * 0.5), 2)

        if not after.get("grade"):
            after["grade"] = _grade_from_total(after.get("total_score"))
        if safe_float(after.get("total_score")) is not None:
            after["grade"] = _grade_from_total(after.get("total_score"))

        after_json = json.dumps(after, ensure_ascii=False)

        cur.execute(
            """
            INSERT INTO poc_score_overrides (
                score_id, before_score_json, after_score_json, updated_by, reason, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (score_id, before_json, after_json, updated_by, reason, now),
        )
        override_id = int(cur.lastrowid)

        cur.execute(
            """
            INSERT INTO audit_logs (
                action, target_type, target_key,
                before_json, after_json, updated_by, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "poc_score_override",
                "poc_score_override",
                f"score_id={score_id};override_id={override_id}",
                before_json,
                after_json,
                updated_by,
                now,
            ),
        )

    return override_id
