#!/usr/bin/env python3
"""Streamlit 面板：项目与人员准确率统计、覆盖编辑、审计日志。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import plotly.express as px
import streamlit as st

from panel_db import (
    apply_override,
    deactivate_override,
    get_person_overall,
    get_person_project_series,
    get_project_group_detail,
    list_audit_logs,
    list_project_groups,
)
from panel_metrics import to_percent

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


def to_project_overview_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    data = []
    for r in rows:
        data.append(
            {
                "project_group_id": r.get("project_group_id"),
                "project_group_name": r.get("project_group_name") or "",
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


def render_project_detail(db_path: str, operator: str) -> None:
    st.subheader("项目详情")

    groups = list_project_groups(db_path=db_path)
    if not groups:
        st.warning("暂无项目数据")
        return

    group_map = {
        f"{g.get('project_group_name') or g.get('project_group_id')} | {g.get('project_group_id')}": g.get("project_group_id")
        for g in groups
    }
    selected = st.selectbox("选择项目", list(group_map.keys()))
    project_group_id = group_map[selected]

    c1, c2, c3 = st.columns(3)
    role_filter = c1.selectbox("角色过滤", ROLE_OPTIONS, index=0)
    vendor_filter = c2.selectbox("后缀过滤", VENDOR_OPTIONS, index=0)
    sheet_only = "全部"

    detail = get_project_group_detail(
        db_path=db_path,
        project_group_id=project_group_id,
        role_filter=role_filter,
        vendor_suffix=vendor_filter,
    )
    if not detail:
        st.warning("项目不存在")
        return

    group = detail.get("project_group", {})
    st.caption(
        f"项目: {group.get('project_group_name', '-')}"
        f" | group_id: {group.get('project_group_id', '-')}"
        f" | 最新run时间: {detail.get('latest_run_at', '-')}"
        f" | 子项目数: {detail.get('sheet_count', 0)}"
    )

    project_metrics = detail.get("project_metrics", [])
    _render_project_metric_cards(project_metrics)

    st.markdown("#### 项目级汇总")
    pm_df = pd.DataFrame(project_metrics)
    if not pm_df.empty:
        if "accuracy" in pm_df.columns:
            pm_df["accuracy"] = pm_df["accuracy"].apply(fmt_percent)
        if "weighted_accuracy" in pm_df.columns:
            pm_df["weighted_accuracy"] = pm_df["weighted_accuracy"].apply(fmt_percent)
    st.dataframe(pm_df, use_container_width=True, hide_index=True)

    sheets = detail.get("sheets", [])
    if sheets:
        sheet_options = ["全部"]
        for s in sheets:
            p = s.get("project", {})
            sheet_options.append(f"{p.get('sheet_title') or p.get('sheet_ref') or p.get('project_id')}")
        sheet_only = c3.selectbox("查看子项目", sheet_options, index=0, key=f"sheet_filter_{project_group_id}")

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
    st.set_page_config(page_title="项目数据面板", layout="wide")
    st.title("项目数据面板")

    with st.sidebar:
        st.header("配置")
        db_path = st.text_input("SQLite 路径", value=DEFAULT_DB_PATH)
        operator = st.text_input("操作人", value="panel")
        page = st.radio(
            "页面",
            ["项目总览", "项目详情", "人员总览", "人员详情", "审计日志"],
            index=0,
        )

    if page == "项目总览":
        render_project_overview(db_path)
    elif page == "项目详情":
        render_project_detail(db_path, operator)
    elif page == "人员总览":
        render_person_overview(db_path)
    elif page == "人员详情":
        render_person_detail(db_path)
    else:
        render_audit_logs(db_path)


if __name__ == "__main__":
    main()
