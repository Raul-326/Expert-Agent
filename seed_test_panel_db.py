#!/usr/bin/env python3
"""基于正式库字段生成老板版演示专用 test SQLite。"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

from panel_db import (
    get_latest_project_poc_score,
    get_person_overall,
    list_project_groups,
    save_project_poc_score,
    save_run_snapshot,
)


DEFAULT_SOURCE_DB = str(Path(__file__).with_name("metrics_panel.db").resolve())
DEFAULT_TARGET_DB = str(Path(__file__).with_name("test_panel.db").resolve())


def _clamp_ratio(value: float) -> float:
    return max(0.0, min(float(value), 1.0))


def _make_snapshot(
    project_id: str,
    group_id: str,
    group_name: str,
    sheet_ref: str,
    sheet_title: str,
    run_id: str,
    run_at: str,
    difficulty: float,
    person_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return {
        "project_id": project_id,
        "run_id": run_id,
        "run_at": run_at,
        "difficulty_coef": difficulty,
        "project_meta": {
            "project_id": project_id,
            "project_group_id": group_id,
            "project_group_name": group_name,
            "spreadsheet_token": group_id,
            "sheet_ref": sheet_ref,
            "sheet_title": sheet_title,
            "display_name": sheet_title,
            "result_spreadsheet_token": group_id,
            "result_sheet_ref": "结果",
        },
        "run_meta": {
            "run_id": run_id,
            "run_at": run_at,
            "source_type": "boss_demo_seed",
            "difficulty_coef": difficulty,
        },
        "person_metrics_base": person_rows,
    }


def _build_person_pool(source_db: str) -> List[str]:
    names: List[str] = []
    seen = set()
    for row in get_person_overall(source_db):
        person_name = str(row.get("person_name") or "").strip()
        if not person_name or person_name in seen:
            continue
        seen.add(person_name)
        names.append(person_name)
    if not names:
        raise ValueError("源数据库没有可用人员名称，无法生成测试库")
    return names


def _build_project_pool(source_db: str) -> List[Dict[str, Any]]:
    rows = list_project_groups(source_db)
    if not rows:
        raise ValueError("源数据库没有可用项目，无法生成测试库")
    return rows[:10]


def seed_demo_db(source_db: str, target_db: str) -> None:
    source_projects = _build_project_pool(source_db)
    person_pool = _build_person_pool(source_db)
    poc_pool: List[str] = []

    for row in source_projects:
        score_pack = get_latest_project_poc_score(source_db, str(row.get("project_group_id") or ""))
        owner = str((score_pack.get("score") or {}).get("project_owner") or "").strip()
        if owner and owner not in poc_pool:
            poc_pool.append(owner)

    for name in person_pool:
        if name not in poc_pool:
            poc_pool.append(name)

    target_path = Path(target_db)
    if target_path.exists():
        target_path.unlink()

    base_time = datetime(2026, 3, 1, 9, 0, tzinfo=timezone.utc)

    for idx, row in enumerate(source_projects):
        group_id = f"bossdemo_{idx + 1:02d}"
        group_name = str(row.get("batch_project_name") or row.get("project_group_name") or f"演示项目 {idx + 1:02d}")
        difficulty = round(1.0 + (idx % 5) * 0.05, 2)
        sheet_count = 2 if idx % 3 == 0 else 1
        member_count = 4 + (idx % 3)
        start_offset = (idx * 2) % len(person_pool)
        members = [person_pool[(start_offset + n) % len(person_pool)] for n in range(member_count)]
        project_has_inspection = idx % 5 != 2

        for sheet_idx in range(sheet_count):
            run_at = (base_time + timedelta(days=idx, hours=sheet_idx)).isoformat(timespec="seconds")
            project_id = f"{group_id}:s{sheet_idx + 1}"
            person_rows: List[Dict[str, Any]] = []

            for person_idx, person_name in enumerate(members):
                role = "质检" if (person_idx + idx + sheet_idx) % 4 == 0 else "初标"
                volume = float(25 + idx * 7 + person_idx * 6 + sheet_idx * 3)

                if project_has_inspection:
                    inspected = float(max(5, int(volume * (0.55 + ((person_idx + idx) % 3) * 0.1))))
                    deduction = (idx + person_idx * 2 + sheet_idx) % 9
                    passed = float(max(0, min(int(inspected), int(inspected) - deduction)))
                    accuracy = passed / inspected if inspected > 0 else None
                    max_accuracy = _clamp_ratio(1.0 / difficulty)
                    if accuracy is not None and accuracy > max_accuracy:
                        passed = float(int(inspected * max_accuracy))
                        accuracy = passed / inspected if inspected > 0 else None
                else:
                    inspected = None
                    passed = None
                    accuracy = None

                weighted = round(_clamp_ratio(accuracy * difficulty), 6) if accuracy is not None else None
                person_rows.append(
                    {
                        "project_id": project_id,
                        "person_name": person_name,
                        "role": role,
                        "volume": volume,
                        "inspected_count": inspected,
                        "pass_count": passed,
                        "accuracy": accuracy,
                        "weighted_accuracy": weighted,
                        "difficulty_coef": difficulty,
                    }
                )

            save_run_snapshot(
                _make_snapshot(
                    project_id=project_id,
                    group_id=group_id,
                    group_name=group_name,
                    sheet_ref=f"s{sheet_idx + 1}",
                    sheet_title=f"Sheet {sheet_idx + 1}",
                    run_id=f"bossdemo_run_{idx + 1:02d}_{sheet_idx + 1:02d}",
                    run_at=run_at,
                    difficulty=difficulty,
                    person_rows=person_rows,
                ),
                db_path=target_db,
            )

        if idx % 4 != 3:
            owner = poc_pool[idx % len(poc_pool)]
            sop_score = float(70 + (idx % 5) * 4)
            sheet_score = float(65 + (idx % 4) * 5)
            total_score = round((sop_score + sheet_score) / 2.0, 2)
            grade = "A" if total_score >= 85 else "B" if total_score >= 70 else "C"
            save_project_poc_score(
                db_path=target_db,
                job_id=f"bossdemo_job_{idx + 1:02d}",
                project_group_id=group_id,
                project_owner=owner,
                sop_score=sop_score,
                sheet_score=sheet_score,
                total_score=total_score,
                grade=grade,
                sop_source_type="seed",
                model_name="seed-script",
                prompt_version="boss-demo-v1",
                sop_reason="老板版演示数据",
                sheet_reason="老板版演示数据",
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="生成老板版演示 test SQLite")
    parser.add_argument("--source-db", default=DEFAULT_SOURCE_DB, help="源数据库路径")
    parser.add_argument("--target-db", default=DEFAULT_TARGET_DB, help="目标 test 数据库路径")
    args = parser.parse_args()

    seed_demo_db(source_db=args.source_db, target_db=args.target_db)
    print(args.target_db)


if __name__ == "__main__":
    main()
