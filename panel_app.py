#!/usr/bin/env python3
"""Streamlit 面板：项目与人员准确率统计、覆盖编辑、审计日志。"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from panel_db import (
    apply_poc_score_override,
    apply_override,
    deactivate_override,
    get_person_overall,
    get_person_project_series,
    get_latest_project_poc_score,
    get_logical_project_detail,
    list_logical_projects_for_detail,
    list_poc_score_overrides,
    list_audit_logs,
    list_project_groups,
)
from panel_metrics import to_percent
from workflow_feishu import (
    WorkflowComputeRequest,
    WritebackTarget,
    compute_workflow,
    persist_workflow_result,
    writeback_workflow_result,
)

DEFAULT_DB_PATH = str(Path(__file__).with_name("metrics_panel.db").resolve())
ROLE_OPTIONS = ["全部", "初标", "质检", "POC"]
VENDOR_OPTIONS = ["全部", "_TMX", "_Appen", "_校企", "_CL"]
EDITABLE_KEYS = ["volume", "inspected_count", "pass_count", "accuracy", "difficulty_coef"]


def fmt_percent(v: Optional[float]) -> str:
    p = to_percent(v)
    return p if p is not None else "-"


def fmt_num(v: Optional[float]) -> str:
    if v is None:
        return "-"
    if float(v).is_integer():
        return str(int(v))
    return f"{v:.4f}"


def parse_value(v: Any) -> Optional[float]:
    if v is None:
        return None
    text = str(v).strip()
    if not text:
        return None
    if text.endswith("%"):
        try:
            return float(text[:-1].strip()) / 100.0
        except Exception:
            return None
    try:
        return float(text)
    except Exception:
        return None


def parse_optional_float_text(v: str) -> Optional[float]:
    text = str(v or "").strip()
    if not text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def parse_sheet_refs_text(raw: str) -> List[str]:
    refs: List[str] = []
    for line in str(raw or "").splitlines():
        item = line.strip()
        if not item:
            continue
        refs.append(item)
    return refs


def _pending_compute_key() -> str:
    return "pending_compute_result_v1"


def to_project_overview_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    data = []
    for r in rows:
        data.append(
            {
                "project_name": r.get("batch_project_name") or r.get("project_group_name") or "",
                "logical_project_name": r.get("logical_project_name") or r.get("project_group_name") or "",
                "batch_no": r.get("batch_no") or "-",
                "sheet_count": r.get("sheet_count", 0),
                "latest_run_at": r.get("latest_run_at") or "",
                "difficulty": r.get("latest_difficulty_coef"),
                "person_count": r.get("person_count", 0),
                "project_accuracy": fmt_percent(r.get("project_accuracy")),
                "project_weighted_accuracy": fmt_percent(r.get("project_weighted_accuracy")),
            }
        )
    return pd.DataFrame(data)


def render_project_overview(db_path: str) -> None:
    st.subheader("项目总览")

    c1, c2, c3, c4 = st.columns(4)
    keyword = c1.text_input("项目筛选")
    role = c2.selectbox("角色口径", ROLE_OPTIONS, index=0)
    vendor = c3.selectbox("供应商后缀", VENDOR_OPTIONS, index=0)
    date_from = c4.text_input("开始时间(ISO，可选)", value="")
    date_to = st.text_input("结束时间(ISO，可选)", value="")

    rows = list_project_groups(
        db_path=db_path,
        project_keyword=keyword or None,
        date_from=date_from or None,
        date_to=date_to or None,
        role=role if role != "全部" else None,
        vendor_suffix=vendor if vendor != "全部" else None,
    )

    df = to_project_overview_df(rows)
    st.caption(f"项目数: {len(df)}")
    st.dataframe(df, use_container_width=True, hide_index=True)


def _make_editable_person_df(person_rows: List[Dict[str, Any]]) -> pd.DataFrame:
    display_rows = []
    for r in person_rows:
        display_rows.append(
            {
                "person_name": r.get("person_name"),
                "role": r.get("role"),
                "volume": r.get("volume"),
                "inspected_count": r.get("inspected_count"),
                "pass_count": r.get("pass_count"),
                "accuracy": r.get("accuracy"),
                "weighted_accuracy": r.get("weighted_accuracy"),
                "difficulty_coef": r.get("difficulty_coef"),
            }
        )
    return pd.DataFrame(display_rows)


def _render_project_metric_cards(project_metrics: List[Dict[str, Any]]) -> None:
    overall = next((x for x in project_metrics if x.get("metric_group") == "整体"), None)
    if not overall:
        st.info("暂无项目汇总数据")
        return

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("总产量", fmt_num(overall.get("volume_total")))
    c2.metric("总被检数", fmt_num(overall.get("inspected_total")))
    c3.metric("总通过数", fmt_num(overall.get("pass_total")))
    c4.metric("整体准确率", fmt_percent(overall.get("accuracy")))
    c5.metric("整体加权准确率", fmt_percent(overall.get("weighted_accuracy")))


def _details_to_map(details: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for d in details or []:
        section = str(d.get("section") or "").strip()
        if section:
            out[section] = d
    return out


def _render_poc_score_card(db_path: str, project_group_id: str) -> Dict[str, Any]:
    score_pack = get_latest_project_poc_score(db_path=db_path, project_group_id=project_group_id)
    if not score_pack:
        st.info("当前项目暂无 POC 评分")
        return {}

    score = score_pack.get("score") or {}
    details = _details_to_map(score_pack.get("details") or [])

    st.markdown("#### POC 评分卡（LLM 主评分）")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("POC总分", fmt_num(score.get("total_score")))
    c2.metric("等级", score.get("grade") or "-")
    c3.metric("SOP分", fmt_num(score.get("sop_score")))
    c4.metric("表格分", fmt_num(score.get("sheet_score")))
    c5.metric("负责人", score.get("project_owner") or "-")
    st.caption(
        f"评分来源: {score.get('sop_source_type') or '-'} | 模型: {score.get('model_name') or '-'} | "
        f"更新时间: {score.get('override_updated_at') or score.get('created_at') or '-'}"
    )

    sop_detail = details.get("sop", {})
    sheet_detail = details.get("sheet", {})
    with st.expander("评分解释与证据", expanded=False):
        st.markdown("**SOP 评分理由**")
        st.write(sop_detail.get("reason_text") or "-")
        st.markdown("**SOP 证据**")
        try:
            st.json(json.loads(sop_detail.get("evidence_json") or "[]"))
        except Exception:
            st.write(sop_detail.get("evidence_json") or "[]")
        st.markdown("**表格评分理由**")
        st.write(sheet_detail.get("reason_text") or "-")
        st.markdown("**表格证据**")
        try:
            st.json(json.loads(sheet_detail.get("evidence_json") or "[]"))
        except Exception:
            st.write(sheet_detail.get("evidence_json") or "[]")

    return score


def _apply_table_overrides(
    db_path: str,
    project_id: str,
    original_df: pd.DataFrame,
    edited_df: pd.DataFrame,
    updated_by: str,
    reason: str,
) -> int:
    changes = 0
    if original_df.empty or edited_df.empty:
        return changes

    original_map = {
        (str(r["person_name"]), str(r["role"])): r for _, r in original_df.iterrows()
    }

    for _, row in edited_df.iterrows():
        key = (str(row.get("person_name")), str(row.get("role")))
        old = original_map.get(key)
        if old is None:
            continue

        for metric_key in EDITABLE_KEYS:
            old_v = parse_value(old.get(metric_key))
            new_v = parse_value(row.get(metric_key))
            if old_v is None and new_v is None:
                continue
            if old_v is not None and new_v is not None and abs(old_v - new_v) < 1e-12:
                continue

            apply_override(
                db_path=db_path,
                project_id=project_id,
                person_name=key[0],
                role=key[1],
                metric_key=metric_key,
                override_value=new_v,
                updated_by=updated_by,
                reason=reason,
                is_active=True,
            )
            changes += 1

    return changes


def _result_get(result: Any, key: str, default: Any = None) -> Any:
    if isinstance(result, dict):
        return result.get(key, default)
    return getattr(result, key, default)


def _render_compute_preview(result: Any) -> None:
    sheets = _result_get(result, "sheets", []) or []
    warnings = _result_get(result, "warnings", []) or []
    errors = _result_get(result, "errors", []) or []
    logs = _result_get(result, "logs", []) or []
    project_preview = _result_get(result, "project_aggregate_preview", {}) or {}
    project_display_name = _result_get(result, "project_display_name", "") or ""
    poc_owner = _result_get(result, "poc_owner", "") or ""
    spreadsheet_title = _result_get(result, "spreadsheet_title", "") or ""

    st.caption(
        f"项目显示名: {project_display_name or '-'} | POC负责人: {poc_owner or '-'} | "
        f"Spreadsheet: {spreadsheet_title or '-'}"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("成功Sheet数", len(sheets))
    c2.metric("失败Sheet数", len(errors))
    c3.metric("项目人数", project_preview.get("person_count", 0))

    if warnings:
        for w in warnings:
            st.warning(str(w))

    if errors:
        st.markdown("#### 失败明细")
        st.dataframe(pd.DataFrame(errors), use_container_width=True, hide_index=True)

    st.markdown("#### 项目级预览汇总")
    pm = pd.DataFrame(project_preview.get("project_metrics", []))
    if not pm.empty:
        if "accuracy" in pm.columns:
            pm["accuracy"] = pm["accuracy"].apply(fmt_percent)
        if "weighted_accuracy" in pm.columns:
            pm["weighted_accuracy"] = pm["weighted_accuracy"].apply(fmt_percent)
    st.dataframe(pm, use_container_width=True, hide_index=True)

    poc_preview = _result_get(result, "poc_score_preview", {}) or {}
    score_card = poc_preview.get("score_card") if isinstance(poc_preview, dict) else None
    if score_card:
        st.markdown("#### POC 评分预览")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("POC总分", fmt_num(score_card.get("poc_total_score")))
        c2.metric("等级", score_card.get("grade") or "-")
        c3.metric("SOP分", fmt_num(score_card.get("sop_score")))
        c4.metric("表格分", fmt_num(score_card.get("sheet_score")))
        st.caption(f"负责人: {score_card.get('project_owner') or '-'}")

    st.markdown("#### 各 Sheet 结果")
    for idx, item in enumerate(sheets, start=1):
        name = item.get("sheet_title") or item.get("sheet_ref") or f"sheet_{idx}"
        with st.expander(f"{idx}. {name}", expanded=(idx == 1)):
            st.caption(
                f"sheet_ref={item.get('sheet_ref')} | schema={item.get('schema_type')} | "
                f"difficulty={item.get('difficulty_coef')}"
            )
            mapping = item.get("mapping") or {}
            st.markdown("**列映射**")
            st.json(mapping)

            st.markdown("**初标统计**")
            st.dataframe(pd.DataFrame(item.get("annotators", [])), use_container_width=True, hide_index=True)
            st.markdown("**质检统计**")
            st.dataframe(pd.DataFrame(item.get("qas", [])), use_container_width=True, hide_index=True)
            st.markdown("**POC统计**")
            st.dataframe(pd.DataFrame(item.get("pocs", [])), use_container_width=True, hide_index=True)

    if logs:
        with st.expander("执行日志", expanded=False):
            st.code("\n".join(str(x) for x in logs), language="text")


def render_job_compute(
    db_path: str,
    operator: str,
    auth_mode: str,
    user_access_token: str,
    name_roster_file: str,
) -> None:
    st.subheader("作业计算")
    st.caption("输入作业表 URL 后可直接预览计算结果；默认不自动入库、不自动写回。")

    pending_key = _pending_compute_key()

    with st.form("compute_preview_form"):
        source_url = st.text_input("作业表 URL（必填）", value=st.session_state.get("compute_source_url", ""))
        sheet_refs_raw = st.text_area(
            "Sheet 列表（可选，一行一个；可填 sheet_id/名称/完整sheet URL）",
            value=st.session_state.get("compute_sheet_refs_raw", ""),
            height=110,
        )
        c1, c2, c3 = st.columns(3)
        sop_url = c1.text_input("SOP URL（可选）", value=st.session_state.get("compute_sop_url", ""))
        manual_sop_score = c2.text_input("手工 SOP 分（可选）", value=st.session_state.get("compute_manual_sop_score", ""))
        difficulty_text = c3.text_input("手动难度系数（可选）", value=st.session_state.get("compute_difficulty", ""))

        c4, c5 = st.columns(2)
        project_display_name = c4.text_input("项目名称（可选）", value=st.session_state.get("compute_project_display_name", ""))
        poc_owner = c5.text_input("POC负责人（可选）", value=st.session_state.get("compute_poc_owner", ""))

        c6, c7, c8 = st.columns(3)
        result_url = c6.text_input("写回目标 URL（可选）", value=st.session_state.get("compute_result_url", ""))
        result_sheet_ref = c7.text_input("结果Sheet（写回用）", value=st.session_state.get("compute_result_sheet_ref", "产量&准确率统计"))
        evaluate_poc_score = c8.checkbox("启用 POC 评分预览", value=st.session_state.get("compute_eval_poc", True))

        submit_compute = st.form_submit_button("开始计算（仅预览）")

    if submit_compute:
        st.session_state["compute_source_url"] = source_url
        st.session_state["compute_sheet_refs_raw"] = sheet_refs_raw
        st.session_state["compute_sop_url"] = sop_url
        st.session_state["compute_manual_sop_score"] = manual_sop_score
        st.session_state["compute_difficulty"] = difficulty_text
        st.session_state["compute_project_display_name"] = project_display_name
        st.session_state["compute_poc_owner"] = poc_owner
        st.session_state["compute_result_url"] = result_url
        st.session_state["compute_result_sheet_ref"] = result_sheet_ref
        st.session_state["compute_eval_poc"] = evaluate_poc_score

        if auth_mode == "user" and not user_access_token.strip():
            st.error("请在左侧填写 user_access_token")
        elif not source_url.strip():
            st.error("作业表 URL 不能为空")
        else:
            try:
                req = WorkflowComputeRequest(
                    source_url=source_url.strip(),
                    sheet_refs=parse_sheet_refs_text(sheet_refs_raw),
                    sop_url=sop_url.strip(),
                    manual_sop_score=parse_optional_float_text(manual_sop_score),
                    poc_owner=poc_owner.strip(),
                    result_url=result_url.strip(),
                    result_sheet_ref=result_sheet_ref.strip() or "产量&准确率统计",
                    difficulty_coef=parse_optional_float_text(difficulty_text),
                    project_display_name=project_display_name.strip(),
                    auth_mode=auth_mode,
                    user_access_token=user_access_token.strip(),
                    name_roster_file=name_roster_file,
                    operator=operator,
                    evaluate_poc_score=bool(evaluate_poc_score),
                )

                with st.spinner("正在读取并计算，请稍候..."):
                    computed = compute_workflow(req)
                st.session_state[pending_key] = computed
                st.success(
                    f"计算完成：成功 {len(_result_get(computed, 'sheets', []) or [])} 个 sheet，"
                    f"失败 {len(_result_get(computed, 'errors', []) or [])} 个 sheet。"
                )
            except Exception as e:
                st.error(f"计算失败：{e}")

    pending = st.session_state.get(pending_key)
    if not pending:
        st.info("暂无预览结果。请先点击“开始计算（仅预览）”。")
        return

    st.markdown("---")
    _render_compute_preview(pending)

    st.markdown("#### 结果操作")
    c1, c2, c3 = st.columns(3)
    if c1.button("确认入库", key="confirm_persist_preview"):
        try:
            run_ids = persist_workflow_result(pending, db_path=db_path)
            st.success(f"入库完成，共写入 {len(run_ids)} 个 run：{run_ids}")
            st.rerun()
        except Exception as e:
            st.error(f"入库失败：{e}")

    wb_append = c2.checkbox("写回时追加到空行", value=True, key="preview_wb_append")
    if c3.button("写回飞书结果", key="preview_writeback"):
        if auth_mode == "user" and not user_access_token.strip():
            st.error("写回失败：请在左侧填写 user_access_token")
        else:
            try:
                wb_target = WritebackTarget(
                    result_url=st.session_state.get("compute_result_url", "").strip(),
                    result_sheet_ref=st.session_state.get("compute_result_sheet_ref", "产量&准确率统计").strip() or "产量&准确率统计",
                    append_mode=wb_append,
                    auth_mode=auth_mode,
                    user_access_token=user_access_token.strip(),
                )
                wb_result = writeback_workflow_result(pending, wb_target)
                st.success(f"写回完成：成功 {wb_result.success_count}，失败 {wb_result.failed_count}")
                if wb_result.details:
                    st.dataframe(pd.DataFrame(wb_result.details), use_container_width=True, hide_index=True)
            except Exception as e:
                st.error(f"写回失败：{e}")

    if st.button("清空当前预览", key="clear_pending_preview"):
        st.session_state.pop(pending_key, None)
        st.rerun()


def render_project_detail(
    db_path: str,
    operator: str,
    user_access_token: str,
    auth_mode: str,
    name_roster_file: str,
) -> None:
    st.subheader("项目详情")

    logical_projects = list_logical_projects_for_detail(db_path=db_path)
    if not logical_projects:
        st.warning("暂无项目数据")
        return

    option_map = {}
    for item in logical_projects:
        logical_name = item.get("logical_project_name") or ""
        label = f"{logical_name}（{item.get('batch_count', 0)} 批）"
        option_map[label] = logical_name
    selected = st.selectbox("选择主项目", list(option_map.keys()))
    logical_project_name = option_map[selected]

    c1, c2 = st.columns(2)
    role_filter = c1.selectbox("角色过滤", ROLE_OPTIONS, index=0)
    vendor_filter = c2.selectbox("后缀过滤", VENDOR_OPTIONS, index=0)

    detail = get_logical_project_detail(
        db_path=db_path,
        logical_project_name=logical_project_name,
        role_filter=role_filter,
        vendor_suffix=vendor_filter,
    )
    if not detail:
        st.warning("项目不存在")
        return

    st.caption(
        f"主项目: {detail.get('logical_project_name', '-')}"
        f" | 最新run时间: {detail.get('latest_run_at', '-')}"
        f" | 批次数: {detail.get('batch_count', 0)}"
        f" | 子项目数: {detail.get('sheet_count', 0)}"
    )

    project_metrics = detail.get("project_metrics", [])
    _render_project_metric_cards(project_metrics)

    st.markdown("#### 主项目汇总")
    pm_df = pd.DataFrame(project_metrics)
    if not pm_df.empty:
        if "accuracy" in pm_df.columns:
            pm_df["accuracy"] = pm_df["accuracy"].apply(fmt_percent)
        if "weighted_accuracy" in pm_df.columns:
            pm_df["weighted_accuracy"] = pm_df["weighted_accuracy"].apply(fmt_percent)
    st.dataframe(pm_df, use_container_width=True, hide_index=True)

    batches = detail.get("batches", []) or []
    if not batches:
        st.info("该主项目暂无批次明细")
        return

    batch_label_by_id: Dict[str, str] = {}
    for b in batches:
        group = b.get("project_group", {}) or {}
        gid = str(group.get("project_group_id") or "")
        batch_name = b.get("batch_project_name") or group.get("project_group_name") or gid
        batch_no = str(b.get("batch_no") or "").strip()
        batch_label_by_id[gid] = batch_name if not batch_no else f"{batch_name}（批次 {batch_no}）"

    batch_filter_options = ["全部"] + [str((b.get("project_group", {}) or {}).get("project_group_id") or "") for b in batches]
    selected_batch_id = st.selectbox(
        "批次过滤",
        batch_filter_options,
        index=0,
        format_func=lambda x: "全部" if x == "全部" else batch_label_by_id.get(str(x), str(x)),
    )
    visible_batches = batches if selected_batch_id == "全部" else [
        b for b in batches
        if str((b.get("project_group", {}) or {}).get("project_group_id") or "") == str(selected_batch_id)
    ]

    st.markdown("#### 批次详情")
    for batch in visible_batches:
        group = batch.get("project_group", {}) or {}
        project_group_id = str(group.get("project_group_id") or "")
        batch_name = batch.get("batch_project_name") or group.get("project_group_name") or project_group_id
        batch_no = str(batch.get("batch_no") or "").strip()
        batch_title = batch_name if not batch_no else f"{batch_name}（批次 {batch_no}）"
        st.markdown(f"### {batch_title}")
        st.caption(
            f"最新run时间: {batch.get('latest_run_at', '-')}"
            f" | 子项目数: {batch.get('sheet_count', 0)}"
            f" | 人数: {batch.get('person_count', 0)}"
        )

        batch_metrics = batch.get("project_metrics", [])
        _render_project_metric_cards(batch_metrics)
        st.markdown("#### 批次级汇总")
        batch_pm_df = pd.DataFrame(batch_metrics)
        if not batch_pm_df.empty:
            if "accuracy" in batch_pm_df.columns:
                batch_pm_df["accuracy"] = batch_pm_df["accuracy"].apply(fmt_percent)
            if "weighted_accuracy" in batch_pm_df.columns:
                batch_pm_df["weighted_accuracy"] = batch_pm_df["weighted_accuracy"].apply(fmt_percent)
        st.dataframe(batch_pm_df, use_container_width=True, hide_index=True)

        current_score = _render_poc_score_card(db_path=db_path, project_group_id=project_group_id)

        st.markdown("#### Agent 评估")
        with st.form(key=f"agent_run_{project_group_id}_batch"):
            sf1, sf2, sf3 = st.columns(3)
            sop_url = sf1.text_input("SOP URL（可选）", value="", key=f"sop_{project_group_id}")
            manual_sop_text = sf2.text_input("手工 SOP 分（缺SOP时必填）", value="", key=f"manual_sop_{project_group_id}")
            poc_owner = sf3.text_input("POC 负责人（可选）", value="", key=f"owner_{project_group_id}")
            submit_agent = st.form_submit_button("运行 Agent 评估")

        if submit_agent:
            if auth_mode == "user" and not user_access_token.strip():
                st.error("请先在左侧填入 user_access_token")
            else:
                manual_sop = parse_optional_float_text(manual_sop_text)
                if not sop_url.strip() and manual_sop is None:
                    st.error("缺少 SOP 链接时，必须提供手工 SOP 分（0-100）")
                else:
                    try:
                        from agent.orchestrator import run_task
                        from agent.types import AgentTaskRequest

                        sheets = batch.get("sheets", [])
                        sheet_refs = [str((s.get("project", {}) or {}).get("sheet_ref") or "") for s in sheets]
                        sheet_refs = [x for x in sheet_refs if x]

                        source_token = group.get("spreadsheet_token") or project_group_id
                        source_url = f"https://bytedance.larkoffice.com/sheets/{source_token}"

                        req = AgentTaskRequest(
                            source_url=source_url,
                            sheet_refs=sheet_refs,
                            sop_url=sop_url.strip(),
                            manual_sop_score=manual_sop,
                            poc_owner=poc_owner.strip(),
                            auth_mode=auth_mode,
                            user_access_token=user_access_token.strip(),
                            db_path=db_path,
                            operator=operator,
                            flags={"name_roster_file": name_roster_file},
                        )
                        result = run_task(req)
                        st.success(
                            f"Agent 完成: score_id={result.poc_score_id}, "
                            f"POC总分={result.score_card.get('poc_total_score')}, 等级={result.score_card.get('grade')}"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Agent 运行失败: {e}")

        if current_score and current_score.get("id"):
            st.markdown("#### 人工修订 POC 分（强制审计）")
            with st.form(key=f"poc_override_{project_group_id}_batch"):
                of1, of2, of3, of4 = st.columns(4)
                new_total = of1.text_input("新总分（可选）", value="", key=f"new_total_{project_group_id}")
                new_sop = of2.text_input("新SOP分（可选）", value="", key=f"new_sop_{project_group_id}")
                new_sheet = of3.text_input("新表格分（可选）", value="", key=f"new_sheet_{project_group_id}")
                new_owner = of4.text_input("新负责人（可选）", value="", key=f"new_owner_{project_group_id}")
                reason = st.text_input("修订原因（必填）", value="", key=f"reason_override_{project_group_id}")
                submit_override = st.form_submit_button("保存修订")

            if submit_override:
                fields: Dict[str, Any] = {}
                total_v = parse_optional_float_text(new_total)
                sop_v = parse_optional_float_text(new_sop)
                sheet_v = parse_optional_float_text(new_sheet)
                if total_v is not None:
                    fields["total_score"] = total_v
                if sop_v is not None:
                    fields["sop_score"] = sop_v
                if sheet_v is not None:
                    fields["sheet_score"] = sheet_v
                if new_owner.strip():
                    fields["project_owner"] = new_owner.strip()

                if not reason.strip():
                    st.error("修订原因不能为空")
                elif not fields:
                    st.error("至少填写一个要修订的字段")
                else:
                    try:
                        override_id = apply_poc_score_override(
                            db_path=db_path,
                            score_id=int(current_score["id"]),
                            updated_by=operator,
                            reason=reason.strip(),
                            override_fields=fields,
                        )
                        st.success(f"已保存修订 override_id={override_id}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"保存修订失败: {e}")

            ov_rows = list_poc_score_overrides(db_path=db_path, score_id=int(current_score["id"]))
            if ov_rows:
                st.markdown("##### 修订历史")
                st.dataframe(pd.DataFrame(ov_rows), use_container_width=True, hide_index=True)

        sheets = batch.get("sheets", [])
        if not sheets:
            st.info("该批次暂无子项目数据")
            st.markdown("---")
            continue

        sheet_options = ["全部"]
        for s in sheets:
            p = s.get("project", {})
            sheet_options.append(f"{p.get('sheet_title') or p.get('sheet_ref') or p.get('project_id')}")
        sheet_only = st.selectbox("查看子项目", sheet_options, index=0, key=f"sheet_filter_{project_group_id}_batch")

        st.markdown("#### 子项目详情（按 Sheet）")
        for s in sheets:
            p = s.get("project", {})
            sheet_name = p.get("sheet_title") or p.get("sheet_ref") or p.get("project_id")
            project_id = p.get("project_id")
            if sheet_only != "全部" and sheet_name != sheet_only:
                continue

            run = s.get("latest_run") or {}
            person_rows = s.get("person_metrics", [])
            sheet_metrics = s.get("project_metrics", [])
            overrides = s.get("overrides", [])

            with st.expander(f"{sheet_name} | {project_id}", expanded=True):
                st.caption(f"最新 run: {run.get('run_id', '-')} | run_at: {run.get('run_at', '-')} | difficulty: {run.get('difficulty_coef', '-')}")

                sm_df = pd.DataFrame(sheet_metrics)
                if not sm_df.empty:
                    if "accuracy" in sm_df.columns:
                        sm_df["accuracy"] = sm_df["accuracy"].apply(fmt_percent)
                    if "weighted_accuracy" in sm_df.columns:
                        sm_df["weighted_accuracy"] = sm_df["weighted_accuracy"].apply(fmt_percent)
                st.dataframe(sm_df, use_container_width=True, hide_index=True)

                st.markdown("##### 人员明细（可编辑，默认仅当前sheet生效）")
                base_df = _make_editable_person_df(person_rows)
                edited = st.data_editor(
                    base_df,
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    disabled=["person_name", "role", "weighted_accuracy"],
                    key=f"editor_{project_id}",
                )

                reason = st.text_input("修改原因", value="panel_edit", key=f"reason_{project_id}")
                if st.button("保存人员修改", key=f"save_person_{project_id}"):
                    count = _apply_table_overrides(
                        db_path=db_path,
                        project_id=project_id,
                        original_df=base_df,
                        edited_df=edited,
                        updated_by=operator,
                        reason=reason,
                    )
                    st.success(f"已写入 {count} 条覆盖")
                    st.rerun()

                st.markdown("##### 当前sheet生效覆盖")
                ov_df = pd.DataFrame(overrides)
                st.dataframe(ov_df, use_container_width=True, hide_index=True)

                if not ov_df.empty:
                    ov_ids = ov_df["override_id"].astype(int).tolist()
                    deact_id = st.selectbox("选择要停用的 override_id", ov_ids, key=f"deact_{project_id}")
                    if st.button("停用所选覆盖", key=f"deact_btn_{project_id}"):
                        deactivate_override(db_path, int(deact_id), updated_by=operator, reason="manual_deactivate")
                        st.success(f"已停用 override_id={deact_id}")
                        st.rerun()

        st.markdown("---")


def render_person_overview(db_path: str) -> None:
    st.subheader("人员总览")
    keyword = st.text_input("人员搜索")

    rows = get_person_overall(db_path=db_path, person_keyword=keyword or None)
    data = []
    for r in rows:
        data.append(
            {
                "person_name": r.get("person_name"),
                "role": r.get("role"),
                "project_count": r.get("project_count"),
                "volume_total": r.get("volume_total"),
                "inspected_total": r.get("inspected_total"),
                "pass_total": r.get("pass_total"),
                "overall_accuracy": fmt_percent(r.get("overall_accuracy")),
                "overall_weighted_accuracy": fmt_percent(r.get("overall_weighted_accuracy")),
            }
        )

    df = pd.DataFrame(data)
    st.caption(f"人员记录: {len(df)}")
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_person_detail(db_path: str) -> None:
    st.subheader("人员详情")
    rows = get_person_overall(db_path=db_path)
    if not rows:
        st.warning("暂无人员数据")
        return

    names = sorted({r.get("person_name") for r in rows if r.get("person_name")})
    person_name = st.selectbox("选择人员", names)
    role = st.selectbox("角色", ["初标", "质检"], index=0)
    granularity_label = st.radio("波动粒度", ["按项目聚合", "按sheet分别"], index=0, horizontal=True)
    granularity = "project" if granularity_label == "按项目聚合" else "sheet"

    series = get_person_project_series(
        db_path=db_path,
        person_name=person_name,
        role=role,
        granularity=granularity,
    )
    if not series:
        st.info("该人员在该角色下暂无项目数据")
        return

    df = pd.DataFrame(series)
    show_df = df.copy()
    if "project_group_id" in show_df.columns:
        show_df = show_df.drop(columns=["project_group_id"])
    show_df["accuracy"] = show_df["accuracy"].apply(fmt_percent)
    show_df["weighted_accuracy"] = show_df["weighted_accuracy"].apply(fmt_percent)
    st.dataframe(show_df, use_container_width=True, hide_index=True)

    fig = px.line(
        df,
        x="seq",
        y="weighted_accuracy",
        markers=True,
        hover_data=[
            "project_group_name",
            "sheet_title",
            "project_id",
            "display_name",
            "run_at",
            "accuracy",
            "inspected_count",
            "pass_count",
        ],
        title=f"{person_name} - {role} 加权准确率波动（{granularity_label}）",
    )
    fig.update_layout(xaxis_title="项目序列", yaxis_title="加权准确率")
    st.plotly_chart(fig, use_container_width=True)


def render_audit_logs(db_path: str) -> None:
    st.subheader("审计日志")
    c1, c2, c3, c4 = st.columns(4)
    project_id = c1.text_input("项目过滤")
    person_name = c2.text_input("人员过滤")
    updated_by = c3.text_input("操作人过滤")
    limit = c4.number_input("条数", min_value=10, max_value=2000, value=200, step=10)

    rows = list_audit_logs(
        db_path=db_path,
        project_id=project_id or None,
        person_name=person_name or None,
        updated_by=updated_by or None,
        limit=int(limit),
    )

    if not rows:
        st.info("暂无审计日志")
        return

    show = pd.DataFrame(rows)
    st.dataframe(show[["audit_id", "action", "target_type", "target_key", "updated_by", "updated_at"]], use_container_width=True, hide_index=True)

    for row in rows[:50]:
        title = f"audit_id={row.get('audit_id')} | {row.get('action')} | {row.get('updated_at')}"
        with st.expander(title):
            before_raw = row.get("before_json") or ""
            after_raw = row.get("after_json") or ""
            try:
                before_obj = json.loads(before_raw) if before_raw else {}
            except Exception:
                before_obj = {"raw": before_raw}
            try:
                after_obj = json.loads(after_raw) if after_raw else {}
            except Exception:
                after_obj = {"raw": after_raw}

            c1, c2 = st.columns(2)
            c1.markdown("**Before**")
            c1.json(before_obj)
            c2.markdown("**After**")
            c2.json(after_obj)


def main() -> None:
    st.set_page_config(page_title="Dola标评数据看板", layout="wide")
    st.title("Dola标评数据看板")

    with st.sidebar:
        st.header("配置")
        db_path = st.text_input("SQLite 路径", value=DEFAULT_DB_PATH)
        operator = st.text_input("操作人", value="panel")
        auth_mode = st.selectbox("飞书鉴权模式", ["user", "tenant"], index=0)
        token_default = os.environ.get("FEISHU_USER_ACCESS_TOKEN", "")
        user_access_token = st.text_input("user_access_token", value=token_default, type="password")
        name_roster_file = st.text_input(
            "姓名名单文件",
            value=str(Path(__file__).with_name("name_roster.txt").resolve()),
        )
        page = st.radio(
            "页面",
            ["作业计算", "项目总览", "项目详情", "人员总览", "人员详情", "审计日志"],
            index=0,
        )

    if page == "作业计算":
        render_job_compute(
            db_path=db_path,
            operator=operator,
            auth_mode=auth_mode,
            user_access_token=user_access_token,
            name_roster_file=name_roster_file,
        )
    elif page == "项目总览":
        render_project_overview(db_path)
    elif page == "项目详情":
        render_project_detail(
            db_path=db_path,
            operator=operator,
            user_access_token=user_access_token,
            auth_mode=auth_mode,
            name_roster_file=name_roster_file,
        )
    elif page == "人员总览":
        render_person_overview(db_path)
    elif page == "人员详情":
        render_person_detail(db_path)
    else:
        render_audit_logs(db_path)


if __name__ == "__main__":
    main()
