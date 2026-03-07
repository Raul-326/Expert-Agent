from __future__ import annotations

import json
import re
from collections import Counter
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

import workflow_feishu as wf


def clamp_score(value: Any) -> float:
    try:
        num = float(value)
    except Exception:
        num = 0.0
    if num < 0:
        num = 0.0
    if num > 100:
        num = 100.0
    return round(num, 2)


def grade_from_score(score: float) -> str:
    if score >= 85:
        return "A"
    if score >= 70:
        return "B"
    if score >= 55:
        return "C"
    return "D"


def _extract_first_json_object(text: str) -> Dict[str, Any]:
    txt = (text or "").strip()
    if not txt:
        raise ValueError("LLM 输出为空")

    fenced = re.search(r"```json\s*(\{[\s\S]*?\})\s*```", txt, flags=re.I)
    if fenced:
        return json.loads(fenced.group(1))

    direct = re.search(r"(\{[\s\S]*\})", txt)
    if direct:
        return json.loads(direct.group(1))

    raise ValueError("LLM 输出中未找到 JSON")


def _parse_score_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    score = payload.get("total_score", payload.get("score", payload.get("final_score", payload.get("总分", 0))))
    reason = payload.get("reason", payload.get("summary", payload.get("评分理由", "")))
    evidence = payload.get("evidence", payload.get("evidence_points", payload.get("证据", [])))
    dimension_scores = payload.get("dimension_scores", payload.get("dimensions", {}))

    if not isinstance(evidence, list):
        evidence = [str(evidence)] if evidence is not None else []

    return {
        "score": clamp_score(score),
        "reason": str(reason or "").strip(),
        "evidence": evidence,
        "dimension_scores": dimension_scores if isinstance(dimension_scores, dict) else {},
        "raw_payload": payload,
    }


def _call_llm_json_with_retry(prompt: str, retries: int = 1) -> Dict[str, Any]:
    last_err = None
    for _ in range(retries + 1):
        try:
            output = wf.call_modelark_text(prompt)
            payload = _extract_first_json_object(output)
            parsed = _parse_score_payload(payload)
            parsed["raw_output"] = output
            return parsed
        except Exception as e:
            last_err = e
    raise ValueError(f"解析 LLM JSON 失败: {last_err}")


def sheet_ingest_skill(
    source_url: str,
    sheet_ref: str,
    auth_mode: str,
    user_access_token: str,
    header_depth: str = "auto",
) -> Dict[str, Any]:
    token = wf.resolve_feishu_access_token(auth_mode, user_access_token)
    spreadsheet_token, spreadsheet_title_from_url = wf.resolve_spreadsheet_info_from_url(source_url, token)
    df = wf.read_feishu_sheet(spreadsheet_token, sheet_ref, token=token, header_depth=header_depth)
    return {
        "token": token,
        "spreadsheet_token": spreadsheet_token,
        "spreadsheet_title_from_url": spreadsheet_title_from_url,
        "df": df,
        "sheet_ref": df.attrs.get("sheet_id") or sheet_ref,
        "sheet_title": df.attrs.get("sheet_title") or sheet_ref,
        "spreadsheet_title": df.attrs.get("spreadsheet_title") or spreadsheet_title_from_url or spreadsheet_token,
    }


def schema_detect_skill(df: pd.DataFrame) -> Dict[str, Any]:
    if wf.detect_back_to_back_schema(df):
        return {
            "schema_type": "b2b",
            "mapping": {},
            "confidence": 1.0,
        }
    mapping = wf.intelligent_column_mapping(df.columns.tolist(), df=df)
    coverage = len(mapping) / max(1, len(wf.STANDARD_COLUMNS))
    return {
        "schema_type": "normal",
        "mapping": mapping,
        "confidence": round(min(1.0, coverage), 2),
    }


def metrics_compute_skill(
    df: pd.DataFrame,
    schema_type: str,
    mapping: Dict[str, str],
    reference_keywords: Optional[List[str]] = None,
    objective_keywords: Optional[List[str]] = None,
    subjective_keywords: Optional[List[str]] = None,
    ark_reference_confidence_threshold: float = 0.6,
) -> Dict[str, Any]:
    if schema_type == "b2b":
        annotators, qas, pocs = wf.calculate_back_to_back_annotator_stats(df)
    else:
        annotators, qas, pocs = wf.calculate_accuracy_workflow(
            df,
            mapping,
            reference_keywords=reference_keywords,
            objective_keywords=objective_keywords,
            subjective_keywords=subjective_keywords,
            ark_reference_confidence_threshold=ark_reference_confidence_threshold,
        )
    return {
        "annotators": annotators,
        "qas": qas,
        "pocs": pocs,
    }


def name_standardize_skill(
    annotators: pd.DataFrame,
    qas: pd.DataFrame,
    pocs: pd.DataFrame,
    roster_file: str,
) -> Dict[str, Any]:
    roster = wf.load_name_roster(roster_file)
    if not roster:
        return {
            "annotators": annotators,
            "qas": qas,
            "pocs": pocs,
            "name_standardized": False,
            "roster_size": 0,
        }

    alias_index = wf.build_name_alias_index(roster)
    annotators, qas, pocs = wf.apply_name_standardization(annotators, qas, pocs, alias_index)
    return {
        "annotators": annotators,
        "qas": qas,
        "pocs": pocs,
        "name_standardized": True,
        "roster_size": len(roster),
    }


def detect_project_owner(raw_dfs: List[pd.DataFrame], pocs_frames: List[pd.DataFrame], manual_owner: str = "") -> str:
    if manual_owner and str(manual_owner).strip():
        return str(manual_owner).strip()

    # 优先使用 POC 统计结果
    poc_counter: Counter = Counter()
    for p in pocs_frames:
        if p is None or p.empty:
            continue
        if "POC 姓名" in p.columns:
            for _, row in p.iterrows():
                name = str(row.get("POC 姓名", "")).strip()
                vol = wf.parse_number(row.get("抽检产量")) or 1
                if name:
                    poc_counter[name] += int(vol)

    if poc_counter:
        return poc_counter.most_common(1)[0][0]

    # 其次从原始表常见负责人列推断
    owner_col_keywords = ["poc", "owner", "负责人", "project owner", "项目负责人"]
    candidate_counter: Counter = Counter()
    for df in raw_dfs:
        if df is None or df.empty:
            continue
        for col in df.columns:
            col_key = str(col).strip().lower()
            if not any(k in col_key for k in owner_col_keywords):
                continue
            series = df[col].dropna().astype(str).str.strip()
            series = series[series != ""]
            for v in series.tolist():
                # 过滤过长文本
                if len(v) <= 80:
                    candidate_counter[v] += 1

    if candidate_counter:
        return candidate_counter.most_common(1)[0][0]

    return ""


def sop_quality_skill(
    sop_url: str,
    token: str,
    manual_sop_score: Optional[float] = None,
    prompt_version: str = "poc_sop_v1",
) -> Dict[str, Any]:
    if not sop_url:
        if manual_sop_score is None:
            raise ValueError("缺少 SOP 链接，且未提供 manual_sop_score")
        score = clamp_score(manual_sop_score)
        return {
            "sop_score": score,
            "sop_grade": grade_from_score(score),
            "sop_reason": "未提供SOP链接，使用人工录入分数",
            "sop_evidence": [f"manual_sop_score={score}"],
            "source_type": "manual",
            "model_name": "manual",
            "prompt_version": prompt_version,
            "raw_model_output": "",
            "raw_payload": {},
        }

    sop_title, sop_content = wf.read_sop_content(sop_url, token)
    sop_excerpt = (sop_content or "")[:22000]

    prompt = f"""
你是项目质控负责人，请对SOP清晰度打分（0-100）。
评分维度：流程完整性、角色职责明确性、判定标准可执行性、异常处理说明、示例充分性。
要求：
1. 只输出 JSON，不要输出任何额外文本。
2. JSON 格式：
{{
  "total_score": 0,
  "dimension_scores": {{"流程完整性":0,"角色职责明确性":0,"判定标准可执行性":0,"异常处理说明":0,"示例充分性":0}},
  "reason": "...",
  "evidence": ["...", "..."]
}}

SOP标题：{sop_title}
SOP内容：
{sop_excerpt}
"""
    parsed = _call_llm_json_with_retry(prompt, retries=1)
    score = clamp_score(parsed["score"])

    return {
        "sop_score": score,
        "sop_grade": grade_from_score(score),
        "sop_reason": parsed.get("reason", ""),
        "sop_evidence": parsed.get("evidence", []),
        "source_type": "llm",
        "model_name": wf.DEFAULT_MODEL,
        "prompt_version": prompt_version,
        "raw_model_output": parsed.get("raw_output", ""),
        "raw_payload": parsed.get("raw_payload", {}),
    }


def sheet_quality_skill(
    dfs: List[pd.DataFrame],
    mappings: List[Dict[str, str]],
    prompt_version: str = "poc_sheet_v1",
) -> Dict[str, Any]:
    structure_parts: List[str] = []
    samples: List[Dict[str, Any]] = []

    for idx, df in enumerate(dfs, start=1):
        part = wf.build_sheet_structure_summary(df)
        structure_parts.append(f"[Sheet{idx}]\n{part}")
        _, recs = wf.sample_real_tasks(df, sample_size=20)
        samples.extend(recs)

    # 限制样本条数
    samples = samples[:50]
    mapping_summary = json.dumps(mappings, ensure_ascii=False, indent=2)
    structure_summary = "\n\n".join(structure_parts)
    sample_json = json.dumps(samples, ensure_ascii=False, indent=2)

    prompt = f"""
你是数据作业规范审计专家，请对作业表规范度打分（0-100）。
评分维度：字段命名规范、必填完整性、判定列可识别性、结果闭环一致性、可审计性。
要求：
1. 只输出 JSON，不要输出任何额外文本。
2. JSON 格式：
{{
  "total_score": 0,
  "dimension_scores": {{"字段命名规范":0,"必填完整性":0,"判定列可识别性":0,"结果闭环一致性":0,"可审计性":0}},
  "reason": "...",
  "evidence": ["...", "..."]
}}

列映射结果：
{mapping_summary}

表结构摘要：
{structure_summary}

随机样本（最多50条）：
{sample_json}
"""

    parsed = _call_llm_json_with_retry(prompt, retries=1)
    score = clamp_score(parsed["score"])

    return {
        "sheet_score": score,
        "sheet_grade": grade_from_score(score),
        "sheet_reason": parsed.get("reason", ""),
        "sheet_evidence": parsed.get("evidence", []),
        "source_type": "llm",
        "model_name": wf.DEFAULT_MODEL,
        "prompt_version": prompt_version,
        "raw_model_output": parsed.get("raw_output", ""),
        "raw_payload": parsed.get("raw_payload", {}),
    }


def poc_score_aggregate_skill(sop_score: float, sheet_score: float, project_owner: str) -> Dict[str, Any]:
    sop_score = clamp_score(sop_score)
    sheet_score = clamp_score(sheet_score)
    total = round(sop_score * 0.5 + sheet_score * 0.5, 2)
    grade = grade_from_score(total)

    return {
        "project_owner": (project_owner or "").strip(),
        "sop_score": sop_score,
        "sheet_score": sheet_score,
        "poc_total_score": total,
        "grade": grade,
        "summary": f"项目负责人评分={total}（SOP {sop_score} / 表格 {sheet_score}）",
    }
