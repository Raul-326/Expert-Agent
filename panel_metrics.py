#!/usr/bin/env python3
"""面板指标计算逻辑：覆盖生效、聚合、加权、整体口径。"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
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


def recompute_weighted_accuracy(accuracy: Optional[float], difficulty_coef: Optional[float]) -> Optional[float]:
    if accuracy is None or difficulty_coef is None:
        return None
    return accuracy * difficulty_coef


def _normalize_base_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    out["volume"] = safe_float(out.get("volume"))
    out["inspected_count"] = safe_float(out.get("inspected_count"))
    out["pass_count"] = safe_float(out.get("pass_count"))
    out["accuracy"] = safe_float(out.get("accuracy"))
    out["weighted_accuracy"] = safe_float(out.get("weighted_accuracy"))
    out["difficulty_coef"] = safe_float(out.get("difficulty_coef"))
    return out


def _build_override_lookup(overrides: List[Dict[str, Any]]) -> Dict[tuple, Dict[str, Any]]:
    """按 (project_id, person_name, role, metric_key) 存活跃覆盖。"""
    lookup: Dict[tuple, Dict[str, Any]] = {}
    for o in overrides:
        if not o.get("is_active", 1):
            continue
        key = (
            o.get("project_id"),
            o.get("person_name"),
            o.get("role"),
            o.get("metric_key"),
        )
        lookup[key] = o
    return lookup


def _pick_override_value(
    lookup: Dict[tuple, Dict[str, Any]],
    project_id: str,
    person_name: Optional[str],
    role: Optional[str],
    metric_key: str,
) -> Optional[Any]:
    """覆盖优先级：精确(person+role) > person-only > role-only > project-only。"""
    candidates = [
        (project_id, person_name, role, metric_key),
        (project_id, person_name, None, metric_key),
        (project_id, None, role, metric_key),
        (project_id, None, None, metric_key),
    ]
    for c in candidates:
        if c in lookup:
            return lookup[c].get("override_value")
    return None


def compute_effective_project_metrics(
    base_person_metrics: List[Dict[str, Any]],
    overrides: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    根据基准数据+覆盖，生成项目有效数据。

    返回：
    {
      difficulty_coef,
      person_metrics: [...],
      project_metrics: [...]
    }
    """
    if not base_person_metrics:
        return {
            "difficulty_coef": None,
            "person_metrics": [],
            "project_metrics": [],
        }

    normalized = [_normalize_base_row(r) for r in base_person_metrics]
    project_id = normalized[0].get("project_id")

    lookup = _build_override_lookup(overrides)

    base_difficulty = normalized[0].get("difficulty_coef")
    ov_diff = _pick_override_value(lookup, project_id, None, None, "difficulty_coef")
    difficulty_coef = safe_float(ov_diff) if ov_diff is not None else base_difficulty

    effective_rows: List[Dict[str, Any]] = []
    editable_keys = ["volume", "inspected_count", "pass_count", "accuracy", "weighted_accuracy", "difficulty_coef"]

    for row in normalized:
        r = dict(row)
        person_name = r.get("person_name")
        role = r.get("role")

        # 全局难度优先
        if difficulty_coef is not None:
            r["difficulty_coef"] = difficulty_coef

        # 应用局部覆盖
        for key in editable_keys:
            ov = _pick_override_value(lookup, project_id, person_name, role, key)
            if ov is None:
                continue
            if key in {"volume", "inspected_count", "pass_count", "accuracy", "weighted_accuracy", "difficulty_coef"}:
                r[key] = safe_float(ov)
            else:
                r[key] = ov

        inspected = r.get("inspected_count")
        passed = r.get("pass_count")

        # 规则：有 inspected + pass 时 accuracy = pass/inspected；否则保留现值
        if inspected is not None and passed is not None:
            if inspected > 0:
                r["accuracy"] = passed / inspected
            else:
                r["accuracy"] = None

        r["weighted_accuracy"] = recompute_weighted_accuracy(r.get("accuracy"), r.get("difficulty_coef"))
        effective_rows.append(r)

    # 聚合项目级
    by_role: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "volume_total": 0.0,
        "inspected_total": 0.0,
        "pass_total": 0.0,
        "difficulty_coef": difficulty_coef,
    })

    for r in effective_rows:
        role = r.get("role") or "未知"
        if r.get("volume") is not None:
            by_role[role]["volume_total"] += r["volume"]
        if r.get("inspected_count") is not None:
            by_role[role]["inspected_total"] += r["inspected_count"]
        if r.get("pass_count") is not None:
            by_role[role]["pass_total"] += r["pass_count"]

    project_metrics: List[Dict[str, Any]] = []
    overall_inspected = 0.0
    overall_pass = 0.0
    overall_volume = 0.0

    for role, agg in by_role.items():
        inspected = agg["inspected_total"]
        passed = agg["pass_total"]
        accuracy = (passed / inspected) if inspected > 0 else None
        weighted = recompute_weighted_accuracy(accuracy, difficulty_coef)

        project_metrics.append({
            "metric_group": role,
            "volume_total": agg["volume_total"],
            "inspected_total": inspected,
            "pass_total": passed,
            "accuracy": accuracy,
            "weighted_accuracy": weighted,
            "difficulty_coef": difficulty_coef,
        })

        # 整体项目口径只汇总初标+质检
        if role in {"初标", "质检"}:
            overall_inspected += inspected
            overall_pass += passed
            overall_volume += agg["volume_total"]

    overall_accuracy = (overall_pass / overall_inspected) if overall_inspected > 0 else None
    overall_weighted = recompute_weighted_accuracy(overall_accuracy, difficulty_coef)

    project_metrics.append({
        "metric_group": "整体",
        "volume_total": overall_volume,
        "inspected_total": overall_inspected,
        "pass_total": overall_pass,
        "accuracy": overall_accuracy,
        "weighted_accuracy": overall_weighted,
        "difficulty_coef": difficulty_coef,
    })

    return {
        "difficulty_coef": difficulty_coef,
        "person_metrics": effective_rows,
        "project_metrics": project_metrics,
    }


def compute_effective_person_overall(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """个人全项目整体口径：按样本量加权（总通过/总抽检）。"""
    group: Dict[tuple, Dict[str, Any]] = defaultdict(lambda: {
        "volume_total": 0.0,
        "inspected_total": 0.0,
        "pass_total": 0.0,
        "projects": set(),
        "weighted_num": 0.0,
        "weighted_den": 0.0,
    })

    for row in rows:
        person = row.get("person_name")
        role = row.get("role")
        if not person or not role:
            continue

        key = (person, role)
        g = group[key]

        volume = safe_float(row.get("volume"))
        inspected = safe_float(row.get("inspected_count"))
        passed = safe_float(row.get("pass_count"))
        weighted_acc = safe_float(row.get("weighted_accuracy"))

        if volume is not None:
            g["volume_total"] += volume
        if inspected is not None:
            g["inspected_total"] += inspected
        if passed is not None:
            g["pass_total"] += passed

        project_id = row.get("project_id")
        if project_id:
            g["projects"].add(project_id)

        if inspected is not None and inspected > 0 and weighted_acc is not None:
            g["weighted_num"] += weighted_acc * inspected
            g["weighted_den"] += inspected

    out: List[Dict[str, Any]] = []
    for (person, role), g in group.items():
        inspected = g["inspected_total"]
        passed = g["pass_total"]
        overall_accuracy = (passed / inspected) if inspected > 0 else None
        weighted_overall = (g["weighted_num"] / g["weighted_den"]) if g["weighted_den"] > 0 else None

        out.append({
            "person_name": person,
            "role": role,
            "project_count": len(g["projects"]),
            "volume_total": g["volume_total"],
            "inspected_total": inspected,
            "pass_total": passed,
            "overall_accuracy": overall_accuracy,
            "overall_weighted_accuracy": weighted_overall,
        })

    out.sort(key=lambda x: (x["role"], -(x["overall_accuracy"] or -1)))
    return out


def to_percent(value: Optional[float]) -> Optional[str]:
    if value is None:
        return None
    return f"{value:.2%}"
