from __future__ import annotations

import re
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

import workflow_feishu as wf
from panel_db import (
    create_agent_job,
    save_agent_skill_run,
    save_project_poc_score,
    save_run_snapshot,
    update_agent_job_status,
)

from .registry import get_skill, list_skills, register_skill
from .skills import (
    detect_project_owner,
    metrics_compute_skill,
    name_standardize_skill,
    poc_score_aggregate_skill,
    schema_detect_skill,
    sheet_ingest_skill,
    sheet_quality_skill,
    sop_quality_skill,
)
from .types import AgentRunResult, AgentTaskRequest

SKILL_VERSION = "v1"


def _register_default_skills() -> None:
    if list_skills():
        return
    register_skill("sheet_ingest_skill", SKILL_VERSION, sheet_ingest_skill)
    register_skill("schema_detect_skill", SKILL_VERSION, schema_detect_skill)
    register_skill("metrics_compute_skill", SKILL_VERSION, metrics_compute_skill)
    register_skill("name_standardize_skill", SKILL_VERSION, name_standardize_skill)
    register_skill("sop_quality_skill", SKILL_VERSION, sop_quality_skill)
    register_skill("sheet_quality_skill", SKILL_VERSION, sheet_quality_skill)
    register_skill("poc_score_aggregate_skill", SKILL_VERSION, poc_score_aggregate_skill)


def _sanitize_for_log(data: Any) -> Any:
    if isinstance(data, pd.DataFrame):
        return {
            "__type__": "DataFrame",
            "rows": int(len(data)),
            "columns": [str(c) for c in data.columns.tolist()[:30]],
        }
    if isinstance(data, dict):
        return {str(k): _sanitize_for_log(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_sanitize_for_log(x) for x in data[:50]]
    if isinstance(data, (str, int, float, bool)) or data is None:
        return data
    return str(data)


def _invoke_skill(
    db_path: str,
    job_id: str,
    skill_name: str,
    kwargs: Dict[str, Any],
    persist: bool = True,
) -> Dict[str, Any]:
    skill = get_skill(skill_name)
    if not persist:
        return skill.handler(**kwargs)
    try:
        output = skill.handler(**kwargs)
        save_agent_skill_run(
            db_path=db_path,
            job_id=job_id,
            skill_name=skill_name,
            skill_version=skill.version,
            status="success",
            input_payload=_sanitize_for_log(kwargs),
            output_payload=_sanitize_for_log(output),
        )
        return output
    except Exception as e:
        save_agent_skill_run(
            db_path=db_path,
            job_id=job_id,
            skill_name=skill_name,
            skill_version=skill.version,
            status="failed",
            input_payload=_sanitize_for_log(kwargs),
            output_payload={},
            error=str(e),
        )
        raise


def _extract_sheet_refs(request: AgentTaskRequest) -> List[str]:
    if request.sheet_refs:
        return [str(s).strip() for s in request.sheet_refs if str(s).strip()]

    sheet = wf.extract_sheet_ref_from_url(request.source_url)
    if sheet:
        return [sheet]
    return ["Sheet1"]


def _resolve_source_url(source_url: str) -> str:
    src = (source_url or "").strip()
    if src.startswith("http://") or src.startswith("https://"):
        return src

    m = re.search(r"^[A-Za-z0-9]{10,}$", src)
    if m:
        return f"https://bytedance.larkoffice.com/sheets/{src}"

    raise ValueError("source_url 不能为空，且必须是飞书 URL 或 spreadsheet token")


def run_task(request: AgentTaskRequest) -> AgentRunResult:
    _register_default_skills()

    source_url = _resolve_source_url(request.source_url)
    sheet_refs = _extract_sheet_refs(request)
    db_path = request.db_path
    dry_run = bool((request.flags or {}).get("dry_run"))

    warnings: List[str] = []
    if not dry_run:
        create_agent_job(
            db_path=db_path,
            job_id=request.task_id,
            project_group_id="",
            request_json=asdict(request),
            status="created",
        )
        update_agent_job_status(db_path=db_path, job_id=request.task_id, status="running")

    try:
        roster_file = request.flags.get("name_roster_file") or str(Path(__file__).resolve().parent.parent / "name_roster.txt")
        difficulty = float(request.flags.get("difficulty_coef", 1.0) or 1.0)
        header_depth = str(request.flags.get("header_depth", "auto") or "auto")
        reference_keywords = request.flags.get("reference_keywords")
        objective_keywords = request.flags.get("objective_keywords")
        subjective_keywords = request.flags.get("subjective_keywords")
        try:
            ark_ref_conf_th = float(request.flags.get("ark_reference_confidence_threshold", 0.6) or 0.6)
        except Exception:
            ark_ref_conf_th = 0.6

        raw_dfs: List[pd.DataFrame] = []
        mappings: List[Dict[str, str]] = []
        pocs_frames: List[pd.DataFrame] = []
        snapshots: List[Dict[str, Any]] = []
        ingest_metas: List[Dict[str, Any]] = []
        run_ids: List[str] = []

        for sheet_ref in sheet_refs:
            ingest = _invoke_skill(
                db_path,
                request.task_id,
                "sheet_ingest_skill",
                {
                    "source_url": source_url,
                    "sheet_ref": sheet_ref,
                    "auth_mode": request.auth_mode,
                    "user_access_token": request.user_access_token,
                    "header_depth": header_depth,
                },
                persist=not dry_run,
            )
            df = ingest["df"]
            ingest_metas.append(ingest)
            raw_dfs.append(df)

            schema = _invoke_skill(
                db_path,
                request.task_id,
                "schema_detect_skill",
                {"df": df},
                persist=not dry_run,
            )
            mappings.append(schema.get("mapping", {}))

            metrics = _invoke_skill(
                db_path,
                request.task_id,
                "metrics_compute_skill",
                {
                    "df": df,
                    "schema_type": schema.get("schema_type", "normal"),
                    "mapping": schema.get("mapping", {}),
                    "reference_keywords": reference_keywords,
                    "objective_keywords": objective_keywords,
                    "subjective_keywords": subjective_keywords,
                    "ark_reference_confidence_threshold": ark_ref_conf_th,
                },
                persist=not dry_run,
            )

            named = _invoke_skill(
                db_path,
                request.task_id,
                "name_standardize_skill",
                {
                    "annotators": metrics.get("annotators", pd.DataFrame()),
                    "qas": metrics.get("qas", pd.DataFrame()),
                    "pocs": metrics.get("pocs", pd.DataFrame()),
                    "roster_file": roster_file,
                },
                persist=not dry_run,
            )

            annotators = named.get("annotators", pd.DataFrame())
            qas = named.get("qas", pd.DataFrame())
            pocs = named.get("pocs", pd.DataFrame())
            pocs_frames.append(pocs)

            annotators = wf.apply_weighted_accuracy(annotators, "初标准确率", "加权初标准确率", difficulty)
            qas = wf.apply_weighted_accuracy(qas, "质检准确率", "加权质检准确率", difficulty)

            class Args:
                operator = request.operator or "agent"
                sheet = sheet_ref
                header_row = None
                header_depth = str(request.flags.get("header_depth", "auto") or "auto")
                sop_url = request.sop_url or ""
                result_sheet = str((request.result_target or {}).get("result_sheet_ref") or "产量&准确率统计")
                reference_keywords = request.flags.get("reference_keywords") or []
                objective_keywords = request.flags.get("objective_keywords") or []
                subjective_keywords = request.flags.get("subjective_keywords") or []
                ark_reference_confidence_threshold = ark_ref_conf_th
                no_write_back = True

            snapshot = wf.build_panel_snapshot(
                spreadsheet_token=ingest["spreadsheet_token"],
                sheet_ref=str(ingest.get("sheet_ref") or sheet_ref),
                sheet_title=str(ingest.get("sheet_title") or sheet_ref),
                spreadsheet_title=str(ingest.get("spreadsheet_title") or ingest["spreadsheet_token"]),
                result_spreadsheet_token=str((request.result_target or {}).get("spreadsheet_token") or ingest["spreadsheet_token"]),
                result_sheet_ref=str((request.result_target or {}).get("result_sheet_ref") or "产量&准确率统计"),
                project_display_name=request.flags.get("project_display_name") or None,
                annotators=annotators,
                qas=qas,
                pocs=pocs,
                difficulty=difficulty,
                args=Args(),
                mapping=schema.get("mapping", {}),
            )
            snapshots.append(snapshot)

        if not ingest_metas:
            raise ValueError("未读取到任何 sheet")

        project_group_id = ingest_metas[0].get("spreadsheet_token", "")
        if not dry_run:
            create_agent_job(
                db_path=db_path,
                job_id=request.task_id,
                project_group_id=project_group_id,
                request_json=asdict(request),
                status="running",
            )

        owner = detect_project_owner(raw_dfs, pocs_frames, manual_owner=request.poc_owner)

        sop_eval = _invoke_skill(
            db_path,
            request.task_id,
            "sop_quality_skill",
            {
                "sop_url": request.sop_url,
                "token": ingest_metas[0]["token"],
                "manual_sop_score": request.manual_sop_score,
            },
            persist=not dry_run,
        )

        sheet_eval = _invoke_skill(
            db_path,
            request.task_id,
            "sheet_quality_skill",
            {
                "dfs": raw_dfs,
                "mappings": mappings,
            },
            persist=not dry_run,
        )

        score_card = _invoke_skill(
            db_path,
            request.task_id,
            "poc_score_aggregate_skill",
            {
                "sop_score": sop_eval.get("sop_score", 0),
                "sheet_score": sheet_eval.get("sheet_score", 0),
                "project_owner": owner,
            },
            persist=not dry_run,
        )

        if dry_run:
            run_ids = [str(s.get("run_id")) for s in snapshots if s.get("run_id")]
        elif request.flags.get("skip_run_snapshot"):
            run_ids = [str(s.get("run_id")) for s in snapshots if s.get("run_id")]
        else:
            for snapshot in snapshots:
                run_ids.append(save_run_snapshot(snapshot=snapshot, db_path=db_path))

        if dry_run:
            score_id = None
        else:
            score_id = save_project_poc_score(
                db_path=db_path,
                job_id=request.task_id,
                project_group_id=project_group_id,
                project_owner=score_card.get("project_owner", ""),
                sop_score=sop_eval.get("sop_score"),
                sheet_score=sheet_eval.get("sheet_score"),
                total_score=score_card.get("poc_total_score"),
                grade=score_card.get("grade", ""),
                sop_source_type=sop_eval.get("source_type", "llm"),
                model_name=sop_eval.get("model_name") or sheet_eval.get("model_name") or wf.DEFAULT_MODEL,
                prompt_version=f"{sop_eval.get('prompt_version', 'poc_sop_v1')}+{sheet_eval.get('prompt_version', 'poc_sheet_v1')}",
                sop_reason=sop_eval.get("sop_reason", ""),
                sop_evidence=sop_eval.get("sop_evidence", []),
                sop_raw_output={
                    "raw_output": sop_eval.get("raw_model_output", ""),
                    "raw_payload": sop_eval.get("raw_payload", {}),
                },
                sheet_reason=sheet_eval.get("sheet_reason", ""),
                sheet_evidence=sheet_eval.get("sheet_evidence", []),
                sheet_raw_output={
                    "raw_output": sheet_eval.get("raw_model_output", ""),
                    "raw_payload": sheet_eval.get("raw_payload", {}),
                },
            )

        if not dry_run:
            update_agent_job_status(db_path=db_path, job_id=request.task_id, status="success")
        return AgentRunResult(
            task_id=request.task_id,
            run_ids=run_ids,
            project_group_id=project_group_id,
            poc_score_id=score_id,
            writeback_status="skipped",
            warnings=warnings,
            score_card=score_card,
        )
    except Exception as e:
        if not dry_run:
            update_agent_job_status(db_path=db_path, job_id=request.task_id, status="failed", error=str(e))
        raise
