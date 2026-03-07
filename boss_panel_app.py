#!/usr/bin/env python3
"""老板版面板：项目总览、项目详情、人员总览、人员详情。"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import plotly.express as px
import streamlit as st

from panel_db import (
    get_boss_person_detail,
    get_boss_project_detail,
    list_boss_people,
    list_boss_project_cards,
)
from panel_metrics import to_percent


DEFAULT_DB_PATH = str(Path(__file__).with_name("metrics_panel.db").resolve())
FALLBACK_DB_PATH = str(Path(__file__).with_name("test_panel.db").resolve())
DEFAULT_ROSTER_PATH = str(Path(__file__).with_name("name_roster.txt").resolve())


def clamp_ratio(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return max(0.0, min(float(value), 1.0))


def fmt_percent(value: Optional[float]) -> str:
    val = to_percent(clamp_ratio(value))
    return val if val is not None else "-"


def fmt_num(value: Optional[float]) -> str:
    if value is None:
        return "-"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.2f}"


def fmt_ratio(pass_count: Optional[float], inspected_count: Optional[float]) -> str:
    if pass_count is None or inspected_count is None:
        return "-"
    return f"{fmt_num(pass_count)}/{fmt_num(inspected_count)}"


def fmt_date(value: Optional[str]) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    return text.split("T", 1)[0]


def _db_default() -> str:
    if Path(DEFAULT_DB_PATH).exists():
        return DEFAULT_DB_PATH
    return FALLBACK_DB_PATH


def _load_allowed_names(roster_path: str) -> Set[str]:
    path = Path(roster_path)
    if not path.exists():
        return set()
    names = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        name = line.strip()
        if name:
            names.add(name)
    return names


def _ensure_state() -> None:
    st.session_state.setdefault("boss_panel", "projects")
    st.session_state.setdefault("boss_project_id", "")
    st.session_state.setdefault("boss_person_name", "")
    st.session_state.setdefault("boss_project_table_version", 0)
    st.session_state.setdefault("boss_people_table_version", 0)


def _go_projects(project_group_id: str = "") -> None:
    st.session_state["boss_panel"] = "projects"
    st.session_state["boss_project_id"] = project_group_id
    if not project_group_id:
        st.session_state["boss_person_name"] = ""
        st.session_state["boss_project_table_version"] += 1
    st.rerun()


def _go_people(person_name: str = "") -> None:
    st.session_state["boss_panel"] = "people"
    st.session_state["boss_person_name"] = person_name
    if not person_name:
        st.session_state["boss_project_id"] = ""
        st.session_state["boss_people_table_version"] += 1
    st.rerun()


def render_project_overview(db_path: str) -> None:
    st.subheader("项目总览")
    keyword = st.text_input("项目搜索", value="")
    rows = list_boss_project_cards(db_path=db_path, project_keyword=keyword or None)
    st.caption(f"项目数: {len(rows)}")

    if not rows:
        st.warning("暂无项目数据。")
        return

    project_ids: List[str] = []
    table_rows: List[Dict[str, Any]] = []
    for row in rows:
        project_ids.append(str(row.get("project_group_id") or ""))
        table_rows.append(
            {
                "项目名称": str(row.get("project_name") or "-"),
                "批次": str(row.get("batch_no") or "-"),
                "日期": fmt_date(row.get("date")),
                "POC": str(row.get("poc_name") or "-"),
                "项目人数": fmt_num(row.get("person_count")),
                "整体加权准确率": fmt_percent(row.get("overall_weighted_accuracy")),
                "总产量": fmt_num(row.get("total_volume")),
            }
        )

    df = pd.DataFrame(table_rows)
    styler = df.style.set_properties(subset=["项目名称"], **{"font-weight": "bold"})
    event = st.dataframe(
        styler,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-cell",
        key=f"boss_project_table_{st.session_state['boss_project_table_version']}",
    )
    cells = (event.selection or {}).get("cells", []) if event else []
    if cells:
        row_idx, column_name = cells[0]
        if column_name == "项目名称" and 0 <= int(row_idx) < len(project_ids):
            _go_projects(project_ids[int(row_idx)])


def render_project_detail(db_path: str, project_group_id: str, allowed_names: Set[str]) -> None:
    detail = get_boss_project_detail(db_path=db_path, project_group_id=project_group_id)
    if not detail:
        st.warning("项目不存在。")
        return

    if st.button("返回项目总览"):
        _go_projects()

    st.subheader(f"项目详情: {detail.get('project_name') or '-'}")
    st.caption(
        f"批次: {detail.get('batch_no') or '-'} | 日期: {fmt_date(detail.get('date'))} | "
        f"POC: {detail.get('poc_name') or '-'}"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("项目人数", fmt_num(detail.get("person_count")))
    c2.metric("整体加权准确率", fmt_percent(detail.get("overall_weighted_accuracy")))
    c3.metric("总产量", fmt_num(detail.get("total_volume")))

    people = detail.get("people") or []
    if allowed_names:
        people = [row for row in people if str(row.get("person_name") or "").strip() in allowed_names]
    if not people:
        st.info("该项目暂无人员数据。")
        return

    table_rows: List[Dict[str, Any]] = []
    for row in people:
        table_rows.append(
            {
                "人员": str(row.get("person_name") or "-"),
                "角色": str(row.get("role") or "-"),
                "总产量": fmt_num(row.get("volume_total")),
                "通过数/被检数": fmt_ratio(row.get("pass_total"), row.get("inspected_total")),
                "加权准确率": fmt_percent(row.get("overall_weighted_accuracy")),
            }
        )

    st.markdown("#### 项目人员详情")
    st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)


def render_people_overview(db_path: str, allowed_names: Set[str]) -> None:
    st.subheader("人员名单")
    keyword = st.text_input("人员搜索", value="")
    rows = list_boss_people(db_path=db_path, person_keyword=keyword or None)
    if allowed_names:
        rows = [row for row in rows if str(row.get("person_name") or "").strip() in allowed_names]
    st.caption(f"人员数: {len(rows)}")

    if not rows:
        st.warning("暂无人员数据。")
        return

    person_names: List[str] = []
    table_rows: List[Dict[str, Any]] = []
    for row in rows:
        person_names.append(str(row.get("person_name") or ""))
        table_rows.append(
            {
                "人员姓名": str(row.get("person_name") or "-"),
                "角色": str(row.get("roles") or "-"),
                "参与项目数": fmt_num(row.get("project_count")),
                "总产量": fmt_num(row.get("volume_total")),
                "通过数/被检数": fmt_ratio(row.get("pass_total"), row.get("inspected_total")),
                "整体加权准确率": fmt_percent(row.get("overall_weighted_accuracy")),
            }
        )

    df = pd.DataFrame(table_rows)
    styler = df.style.set_properties(subset=["人员姓名"], **{"font-weight": "bold"})
    event = st.dataframe(
        styler,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-cell",
        key=f"boss_people_table_{st.session_state['boss_people_table_version']}",
    )
    cells = (event.selection or {}).get("cells", []) if event else []
    if cells:
        row_idx, column_name = cells[0]
        if column_name == "人员姓名" and 0 <= int(row_idx) < len(person_names):
            _go_people(person_names[int(row_idx)])


def render_person_detail(db_path: str, person_name: str, allowed_names: Set[str]) -> None:
    if allowed_names and person_name not in allowed_names:
        st.warning("该人员不在当前人员名单中。")
        return

    detail = get_boss_person_detail(db_path=db_path, person_name=person_name)
    if not detail:
        st.warning("人员不存在。")
        return

    if st.button("返回人员名单"):
        _go_people()

    st.subheader(f"人员详情: {detail.get('person_name') or '-'}")
    st.caption(f"角色: {detail.get('roles') or '-'}")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("参与项目数", fmt_num(detail.get("project_count")))
    c2.metric("总产量", fmt_num(detail.get("volume_total")))
    c3.metric("通过数/被检数", fmt_ratio(detail.get("pass_total"), detail.get("inspected_total")))
    c4.metric("整体加权准确率", fmt_percent(detail.get("overall_weighted_accuracy")))

    projects = detail.get("projects") or []
    if not projects:
        st.info("该人员暂无项目明细。")
        return

    ordered_names: List[str] = []
    table_rows: List[Dict[str, str]] = []
    chart_rows: List[Dict[str, Any]] = []

    for row in projects:
        project_name = str(row.get("project_name") or "-")
        if project_name not in ordered_names:
            ordered_names.append(project_name)

        table_rows.append(
            {
                "日期": fmt_date(row.get("date")),
                "项目": project_name,
                "角色": str(row.get("role") or "-"),
                "总产量": fmt_num(row.get("volume_total")),
                "通过数/被检数": fmt_ratio(row.get("pass_total"), row.get("inspected_total")),
                "加权准确率": fmt_percent(row.get("weighted_accuracy")),
            }
        )
        chart_rows.append(
            {
                "project_name": project_name,
                "role": str(row.get("role") or "-"),
                "weighted_accuracy": clamp_ratio(row.get("weighted_accuracy")),
                "date": fmt_date(row.get("date")),
                "ratio": fmt_ratio(row.get("pass_total"), row.get("inspected_total")),
            }
        )

    st.markdown("#### 所有项目详情")
    st.dataframe(pd.DataFrame(table_rows), use_container_width=True, hide_index=True)

    chart_df = pd.DataFrame(chart_rows)
    if not chart_df.empty and chart_df["weighted_accuracy"].notna().any():
        fig = px.line(
            chart_df,
            x="project_name",
            y="weighted_accuracy",
            color="role",
            markers=True,
            category_orders={"project_name": ordered_names},
            hover_data=["date", "ratio"],
            title=f"{detail.get('person_name') or '-'} 项目趋势",
        )
        fig.update_layout(xaxis_title="项目", yaxis_title="加权准确率")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("该人员暂无可绘制的加权准确率趋势。")


def main() -> None:
    st.set_page_config(page_title="Dola标评数据看板", layout="wide")
    _ensure_state()
    panel = st.session_state.get("boss_panel", "projects")
    current_project_id = st.session_state.get("boss_project_id", "")
    current_person_name = st.session_state.get("boss_person_name", "")

    with st.sidebar:
        st.header("配置")
        db_path = st.text_input("SQLite 路径", value=_db_default())
        roster_path = st.text_input("人员名单路径", value=DEFAULT_ROSTER_PATH)
        sidebar_panel = st.radio(
            "面板",
            ["项目面板", "人员面板"],
            index=0 if panel == "projects" else 1,
        )

    allowed_names = _load_allowed_names(roster_path)

    target_panel = "projects" if sidebar_panel == "项目面板" else "people"
    if target_panel != panel:
        if target_panel == "projects":
            _go_projects()
        else:
            _go_people()

    st.title("Dola标评数据看板")

    if panel == "projects":
        if current_project_id:
            render_project_detail(db_path=db_path, project_group_id=current_project_id, allowed_names=allowed_names)
        else:
            render_project_overview(db_path=db_path)
    else:
        if current_person_name:
            render_person_detail(db_path=db_path, person_name=current_person_name, allowed_names=allowed_names)
        else:
            render_people_overview(db_path=db_path, allowed_names=allowed_names)


if __name__ == "__main__":
    main()
