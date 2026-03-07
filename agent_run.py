#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from agent.orchestrator import run_task
from agent.types import AgentTaskRequest


def main() -> None:
    parser = argparse.ArgumentParser(description="运行 Agent 评分任务（LLM 主评分）")
    parser.add_argument("--source-url", required=True, help="源作业表 URL 或 spreadsheet token")
    parser.add_argument("--sheet", action="append", dest="sheets", help="可重复传入多个 sheet 引用")
    parser.add_argument("--sop-url", default="", help="SOP 文档 URL")
    parser.add_argument("--manual-sop-score", type=float, help="未提供 SOP 时人工输入 SOP 分（0-100）")
    parser.add_argument("--poc-owner", default="", help="POC 负责人（手动兜底）")
    parser.add_argument("--db-path", default="./metrics_panel.db", help="SQLite DB 路径")
    parser.add_argument("--auth-mode", choices=["user", "tenant"], default="user", help="飞书鉴权模式")
    parser.add_argument("--user-access-token", default="", help="user_access_token")
    parser.add_argument("--operator", default="agent_cli", help="操作人")
    parser.add_argument("--name-roster-file", default="./name_roster.txt", help="姓名名单文件")
    parser.add_argument("--header-depth", choices=["auto", "1", "2"], default="auto", help="表头层级")
    parser.add_argument("--reference-keywords", default="", help="参考侧关键词，逗号分隔")
    parser.add_argument("--objective-keywords", default="", help="客观列关键词，逗号分隔")
    parser.add_argument("--subjective-keywords", default="", help="主观列关键词，逗号分隔")
    parser.add_argument("--ark-reference-confidence-threshold", type=float, default=0.6, help="Ark 参考列识别置信阈值")

    args = parser.parse_args()

    req = AgentTaskRequest(
        source_url=args.source_url,
        sheet_refs=args.sheets or [],
        sop_url=args.sop_url,
        manual_sop_score=args.manual_sop_score,
        poc_owner=args.poc_owner,
        auth_mode=args.auth_mode,
        user_access_token=args.user_access_token,
        db_path=args.db_path,
        operator=args.operator,
        flags={
            "name_roster_file": args.name_roster_file,
            "header_depth": args.header_depth,
            "reference_keywords": [x.strip() for x in args.reference_keywords.split(",") if x.strip()],
            "objective_keywords": [x.strip() for x in args.objective_keywords.split(",") if x.strip()],
            "subjective_keywords": [x.strip() for x in args.subjective_keywords.split(",") if x.strip()],
            "ark_reference_confidence_threshold": args.ark_reference_confidence_threshold,
        },
    )

    result = run_task(req)
    print(json.dumps({
        "task_id": result.task_id,
        "project_group_id": result.project_group_id,
        "run_ids": result.run_ids,
        "poc_score_id": result.poc_score_id,
        "score_card": result.score_card,
        "warnings": result.warnings,
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
