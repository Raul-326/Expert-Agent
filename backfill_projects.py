#!/usr/bin/env python3
"""一次性历史回填脚本：批量调用 workflow_feishu.py 并自动入库。"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List


def load_records(input_path: str) -> List[Dict[str, Any]]:
    p = Path(input_path)
    if not p.exists():
        raise FileNotFoundError(f"回填文件不存在: {input_path}")

    if p.suffix.lower() == ".json":
        data = json.loads(p.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data = data.get("projects", [])
        if not isinstance(data, list):
            raise ValueError("JSON 格式不正确，需为数组或包含 projects 数组")
        return [dict(x) for x in data]

    if p.suffix.lower() == ".csv":
        with p.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            return [dict(r) for r in reader]

    raise ValueError("仅支持 CSV/JSON 回填文件")


def build_cmd(args: argparse.Namespace, row: Dict[str, Any]) -> List[str]:
    source_url = (row.get("source_url") or "").strip()
    sheet = (row.get("sheet") or "").strip()
    if not source_url:
        raise ValueError("source_url 不能为空")
    if not sheet:
        raise ValueError("sheet 不能为空")

    cmd = [
        args.python,
        args.workflow_script,
        "--url",
        source_url,
        "--sheet",
        sheet,
        "--auth-mode",
        args.auth_mode,
        "--db-path",
        args.db_path,
        "--operator",
        args.operator,
    ]

    difficulty_coef = (row.get("difficulty_coef") or "").strip()
    sop_url = (row.get("sop_url") or "").strip()
    result_url = (row.get("result_url") or "").strip()
    result_sheet = (row.get("result_sheet") or "").strip()
    project_display_name = (row.get("project_display_name") or "").strip()

    if difficulty_coef:
        cmd.extend(["--difficulty-coef", difficulty_coef])
    elif sop_url:
        cmd.extend(["--sop-url", sop_url])

    if result_url:
        cmd.extend(["--result-url", result_url])
    if result_sheet:
        cmd.extend(["--result-sheet", result_sheet])
    if project_display_name:
        cmd.extend(["--project-display-name", project_display_name])
    if args.user_access_token:
        cmd.extend(["--user-access-token", args.user_access_token])

    if args.no_write_back:
        cmd.append("--no-write-back")

    return cmd


def main() -> None:
    parser = argparse.ArgumentParser(description="批量回填项目到面板 SQLite")
    parser.add_argument("--input", default="backfill_projects.csv", help="回填清单 CSV/JSON")
    parser.add_argument("--workflow-script", default="workflow_feishu.py", help="workflow 脚本路径")
    parser.add_argument("--python", default=sys.executable, help="Python 可执行文件")
    parser.add_argument("--db-path", default="./metrics_panel.db", help="面板 SQLite 路径")
    parser.add_argument("--operator", default="backfill", help="操作人")
    parser.add_argument("--auth-mode", choices=["user", "tenant"], default="user", help="飞书鉴权模式，默认 user")
    parser.add_argument("--user-access-token", help="飞书 user_access_token（不填则走环境变量）")
    parser.add_argument("--no-write-back", action="store_true", help="不写回飞书，仅入库")
    parser.add_argument("--dry-run", action="store_true", help="仅打印命令，不执行")
    parser.add_argument("--stop-on-error", action="store_true", help="遇错即停")
    args = parser.parse_args()

    records = load_records(args.input)
    if not records:
        print("回填清单为空，结束")
        return

    print(f"共 {len(records)} 条待回填")

    success = 0
    failed = 0
    for idx, row in enumerate(records, start=1):
        try:
            cmd = build_cmd(args, row)
        except Exception as e:
            failed += 1
            print(f"[{idx}/{len(records)}] 配置错误: {e}")
            if args.stop_on_error:
                break
            continue

        print(f"\n[{idx}/{len(records)}] 执行: {' '.join(cmd)}")
        if args.dry_run:
            success += 1
            continue

        proc = subprocess.run(cmd)
        if proc.returncode == 0:
            success += 1
            print(f"[{idx}] 成功")
        else:
            failed += 1
            print(f"[{idx}] 失败，退出码={proc.returncode}")
            if args.stop_on_error:
                break

    print("\n回填完成")
    print(f"成功: {success}")
    print(f"失败: {failed}")


if __name__ == "__main__":
    main()
