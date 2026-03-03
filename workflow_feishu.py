#!/usr/bin/env python3
"""
飞书表格数据标注准确率统计工具
自动识别列名并统计初标人、质检人、POC 的准确率和产量
"""

import pandas as pd
import numpy as np
import json
import os
import httpx
import sys
import argparse
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple, Dict, Any
from urllib.parse import urlparse, parse_qs

try:
    from panel_db import save_run_snapshot
except Exception:
    save_run_snapshot = None

try:
    from panel_metrics import compute_effective_project_metrics
except Exception:
    compute_effective_project_metrics = None

# ================= 配置区 =================
# 飞书 API 配置
FEISHU_APP_ID = os.environ.get("FEISHU_APP_ID", "cli_a9156f4577b99cbb")
FEISHU_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "t7z3OruUzeOayL9aLgSt7yNfI0mziHU1")
FEISHU_USER_ACCESS_TOKEN = os.environ.get("FEISHU_USER_ACCESS_TOKEN", "")

# ModelArk API 配置
ARK_API_KEY = os.environ.get("ARK_API_KEY", "8b65e642-beaa-4f37-9626-0b1e84cfdfc3")
ARK_BASE_URL = "https://ark-ap-southeast.byteintl.net/api/v3"
DEFAULT_MODEL = "ep-20260227030217-66fc4"

# 定义什么词汇代表"通过/正确" (支持忽略大小写)
PASS_LABELS = ['通过', 'pass', 'yes', '是', 'aligned', 'agree', '同意', 'true', '1', 'correct']
FAIL_LABELS = ['不通过', 'fail', 'no', '否', 'disagree', '不同意', 'false', 'incorrect']

# 标准列名
STANDARD_COLUMNS = ['初标人', '质检人', '质检结果', 'POC 姓名', '抽检结果']

# 规则库：Key 为标准列名，Value 为可能出现的各种别名（小写匹配）
RULE_BASE = {
    "初标人": [
        "初标人", "初标", "标注员", "标注人", "打标人", "annotator", "标注姓名",
        "evaluator", "sp", "sp name", "rater", "m1"
    ],
    "质检人": [
        "质检人", "质检", "质检员", "审核人", "reviewer", "qa", "一审人",
        "cc", "cc name", "qa name", "m2"
    ],
    "质检结果": [
        "质检结果", "质检状态", "审核结果", "review_status", "qa 结果", "一审结果",
        "cc verdict", "cc result"
    ],
    "POC 姓名": [
        "poc 姓名", "poc name", "抽检人", "poc audit", "二审人", "poc 专家",
        "终审人", "三审人"
    ],
    "抽检结果": ["抽检结果", "poc 结果", "audit result", "抽检状态", "二审结果", "poc verdict", "gsb"]
}

DIFFICULTY_MIN = 1.0
DIFFICULTY_MAX = 1.5
NAME_ROSTER_DEFAULT_PATH = Path(__file__).with_name("name_roster.txt")

# 常见中文拼音别名 -> 全名（可持续补充）
MANUAL_NAME_ALIAS = {
    "yihan": "王乙琀",
    "wangyihan": "王乙琀",
    "zeping": "励泽坪",
    "lizeping": "励泽坪",
    "jinfang": "涂瑾芳",
    "tujinfang": "涂瑾芳",
    "runhuan": "于润寰",
    "yurunhuan": "于润寰",
    "jiaji": "汪珈吉",
    "wangjiaji": "汪珈吉",
    "tianshu": "孟天舒",
    "mengtianshu": "孟天舒",
    "yiyi": "王依依",
    "wangyiyi": "王依依",
    "mingxuan": "李洺萱",
    "limingxuan": "李洺萱",
    "mingxin": "张明昕",
    "zhangmingxin": "张明昕",
}


def num_to_col(n: int) -> str:
    """列序号转 A1 表示法列名，1->A，27->AA"""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

# ================= 飞书 API =================
def get_tenant_access_token(app_id: str, app_secret: str) -> str:
    """获取飞书 tenant_access_token"""
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    resp = httpx.post(url, json={"app_id": app_id, "app_secret": app_secret})
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"获取 tenant_token 失败：{data.get('msg')}")
    return data["tenant_access_token"]


def get_user_access_token(user_access_token: Optional[str] = None) -> str:
    """获取 user_access_token（优先参数，其次环境变量 FEISHU_USER_ACCESS_TOKEN）。"""
    token = (user_access_token or FEISHU_USER_ACCESS_TOKEN or "").strip()
    if not token:
        raise Exception(
            "未提供 user_access_token。请通过 --user-access-token 传入，"
            "或设置环境变量 FEISHU_USER_ACCESS_TOKEN。"
        )
    return token


def resolve_feishu_access_token(auth_mode: str, user_access_token: Optional[str] = None) -> str:
    """根据鉴权模式获取访问 token。"""
    mode = (auth_mode or "user").strip().lower()
    if mode == "user":
        return get_user_access_token(user_access_token)
    if mode == "tenant":
        return get_tenant_access_token(FEISHU_APP_ID, FEISHU_APP_SECRET)
    raise Exception(f"不支持的鉴权模式: {auth_mode}")

def create_sheet(spreadsheet_token: str, sheet_name: str, token: str) -> str:
    """
    创建新的工作表
    Returns: sheet_id
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    # 方案1：旧接口（部分环境可用）
    url_v1 = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/sheets"
    resp = httpx.post(url_v1, headers=headers, json={"sheet": {"title": sheet_name}})
    try:
        data = resp.json()
        if data.get("code") == 0:
            return data["data"]["sheetId"]
    except Exception:
        data = None

    # 方案2：官方常用接口 sheets_batch_update
    url_v2 = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/sheets_batch_update"
    resp2 = httpx.post(
        url_v2,
        headers=headers,
        json={
            "requests": [
                {
                    "addSheet": {
                        "properties": {"title": sheet_name}
                    }
                }
            ]
        }
    )
    try:
        data2 = resp2.json()
    except Exception:
        raise Exception(
            f"创建工作表失败：HTTP {resp2.status_code}, 响应非JSON: {resp2.text[:200]}"
        )

    if data2.get("code") != 0:
        raise Exception(f"创建工作表失败：{data2.get('msg')}")

    # 兼容不同返回结构
    replies = data2.get("data", {}).get("replies", [])
    if replies:
        add_sheet = replies[0].get("addSheet", {})
        props = add_sheet.get("properties", {})
        sheet_id = props.get("sheetId") or add_sheet.get("sheetId")
        if sheet_id:
            return sheet_id

    # 兜底：重新查 metainfo，通过名称反查 sheet_id
    sheet_id = get_sheet_id_by_ref(spreadsheet_token, sheet_name, token)
    if sheet_id:
        return sheet_id

    raise Exception(f"创建工作表失败：无法从响应中解析 sheet_id，响应={json.dumps(data2, ensure_ascii=False)[:300]}")


def get_sheet_id_by_ref(spreadsheet_token: str, sheet_ref: str, token: str) -> Optional[str]:
    """按工作表名称或 sheet_id 查找 sheet_id，不存在返回 None"""
    meta_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    headers = {"Authorization": f"Bearer {token}"}
    resp = httpx.get(meta_url, headers=headers)
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"获取元信息失败：{data.get('msg')}")

    sheets = data.get("data", {}).get("sheets", [])
    for s in sheets:
        if s.get("title") == sheet_ref or s.get("sheetId") == sheet_ref:
            return s.get("sheetId")
    return None

def write_to_sheet(
    spreadsheet_token: str,
    sheet_id: str,
    values: list,
    token: str,
    start_row: int = 1
) -> bool:
    """
    写入数据到飞书表格
    """
    url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8"
    }
    if not values:
        raise Exception("写入数据失败：values 为空")

    max_col = max((len(row) for row in values), default=1)
    max_col = max(max_col, 1)
    end_col = num_to_col(max_col)
    start_row = max(int(start_row), 1)
    end_row = start_row + max(len(values), 1) - 1
    range_str = f"{sheet_id}!A{start_row}:{end_col}{end_row}"

    # 兼容不同请求体格式
    payloads = [
        {"valueRange": {"range": range_str, "values": values}},
        {"range": range_str, "values": values, "valueRange": {"majorDimension": "ROWS"}},
    ]

    last_err = None
    for payload in payloads:
        resp = httpx.put(url, headers=headers, json=payload)
        data = resp.json()
        if data.get("code") == 0:
            return True
        last_err = data.get("msg")

    raise Exception(f"写入数据失败：{last_err}")


def find_append_start_row(
    spreadsheet_token: str,
    sheet_id: str,
    token: str,
    max_rows: int = 5000,
    max_cols: int = 52
) -> int:
    """查找追加写入起始行（最后一个非空行 + 1）。"""
    headers = {"Authorization": f"Bearer {token}"}
    end_col = num_to_col(min(max_cols, 702))
    range_str = f"{sheet_id}!A1:{end_col}{max_rows}"
    data_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_str}"
    resp = httpx.get(data_url, headers=headers)
    data = resp.json()
    if data.get("code") != 0:
        raise Exception(f"读取追加位置失败：{data.get('msg')}")

    values = data.get("data", {}).get("valueRange", {}).get("values", [])

    def cell_non_empty(cell: Any) -> bool:
        if cell is None:
            return False
        try:
            if isinstance(cell, float) and np.isnan(cell):
                return False
        except Exception:
            pass
        return str(cell).strip() != ""

    last_non_empty = 0
    for idx, row in enumerate(values, start=1):
        if any(cell_non_empty(cell) for cell in row):
            last_non_empty = idx
    return last_non_empty + 1


def write_stats_back_to_feishu(
    spreadsheet_token: str,
    result_sheet_ref: str,
    annotators: pd.DataFrame,
    qas: pd.DataFrame,
    pocs: pd.DataFrame,
    token: str,
    difficulty: float = 1.0,
    sop_url: str = "",
    difficulty_report: str = "",
    append_mode: bool = False
) -> str:
    """
    将统计结果写回飞书表格。
    若目标工作表已存在，则覆盖 A1 起始区域；不存在则自动创建。
    """
    stats_data = prepare_stats_data(
        annotators,
        qas,
        pocs,
        difficulty=difficulty,
        sop_url=sop_url,
        difficulty_report=difficulty_report
    )

    sheet_id = get_sheet_id_by_ref(spreadsheet_token, result_sheet_ref, token)
    if sheet_id:
        print(f"检测到已有结果工作表（引用: {result_sheet_ref}），将覆盖写入。")
    else:
        sheet_id = create_sheet(spreadsheet_token, result_sheet_ref, token)
        print(f"已创建结果工作表：{result_sheet_ref}")

    start_row = 1
    if append_mode:
        start_row = find_append_start_row(spreadsheet_token, sheet_id, token)
        print(f"追加写入模式：从第 {start_row} 行开始写入。")

    write_to_sheet(spreadsheet_token, sheet_id, stats_data, token, start_row=start_row)
    return result_sheet_ref


def is_blank_cell(v: Any) -> bool:
    """判断单元格是否为空"""
    if v is None:
        return True
    if isinstance(v, float) and np.isnan(v):
        return True
    text = str(v).strip()
    return text == ""


def normalize_header_cell(v: Any, col_index: int) -> str:
    """规范化表头单元格并保证非空"""
    if is_blank_cell(v):
        return f"col_{col_index + 1}"
    text = str(v).strip().replace("\n", " ")
    return text if text else f"col_{col_index + 1}"


def score_header_row(row: list) -> float:
    """对候选表头行打分，分高者更可能是表头"""
    keywords = [
        "name", "prompt", "answer", "result", "verdict", "comment", "serial",
        "source", "ability", "poc", "cc", "sp", "id", "姓名", "结果", "质检", "初标"
    ]
    non_empty_cells = []
    for cell in row:
        if is_blank_cell(cell):
            continue
        non_empty_cells.append(str(cell).strip())

    if not non_empty_cells:
        return -1e9

    non_empty = len(non_empty_cells)
    short_cnt = sum(1 for x in non_empty_cells if len(x) <= 40)
    unique_cnt = len(set(non_empty_cells))
    keyword_hits = sum(
        1 for x in non_empty_cells if any(k in x.lower() for k in keywords)
    )
    url_cnt = sum(1 for x in non_empty_cells if re.search(r"https?://|www\\.", x.lower()))
    formula_cnt = sum(
        1 for x in non_empty_cells
        if x.startswith("=") or x.upper().startswith(("IF(", "LEFT(", "RIGHT(", "TEXT", "VLOOKUP(", "INDEX("))
    )
    long_cnt = sum(1 for x in non_empty_cells if len(x) > 120)

    return (
        non_empty * 2.0
        + short_cnt * 1.5
        + unique_cnt * 0.5
        + keyword_hits * 3.0
        - url_cnt * 4.0
        - formula_cnt * 2.0
        - long_cnt * 2.0
    )


def build_dataframe_from_values(values: list, header_row: Optional[int] = None) -> pd.DataFrame:
    """将飞书 values 构建为 DataFrame，支持自动识别表头行"""
    if not values:
        raise Exception("表格为空")

    max_cols = max(len(r) for r in values)
    norm_rows = []
    for r in values:
        row = list(r) + [None] * (max_cols - len(r))
        norm_rows.append(row)

    if header_row is None:
        search_upto = min(len(norm_rows), 30)
        best_idx = 0
        best_score = -1e18
        for i in range(search_upto):
            s = score_header_row(norm_rows[i])
            if s > best_score:
                best_score = s
                best_idx = i
        header_idx = best_idx
    else:
        # 传入为 1-based 行号
        header_idx = max(0, min(len(norm_rows) - 1, header_row - 1))

    raw_headers = norm_rows[header_idx]
    headers = []
    seen = {}
    for i, cell in enumerate(raw_headers):
        name = normalize_header_cell(cell, i)
        cnt = seen.get(name, 0)
        seen[name] = cnt + 1
        if cnt > 0:
            name = f"{name}__{cnt+1}"
        headers.append(name)

    body_rows = norm_rows[header_idx + 1:]
    # 丢弃全空行
    body_rows = [r for r in body_rows if any(not is_blank_cell(c) for c in r)]
    if not body_rows:
        df = pd.DataFrame(columns=headers)
        df.attrs["header_row"] = header_idx + 1
        return df

    df = pd.DataFrame(body_rows, columns=headers)
    df.attrs["header_row"] = header_idx + 1
    return df


def is_summary_like_sheet(df: pd.DataFrame) -> bool:
    """判断当前 sheet 是否更像统计结果页而非原始作业页"""
    if df is None or df.empty:
        return False

    cols = [str(c).lower() for c in df.columns]
    summary_kw = ["初标总产量", "被质检数", "质检通过数", "初标准确率", "质检准确率", "抽检通过数"]
    raw_kw = ["prompt", "reference", "response", "link", "serial", "sp name", "cc name", "poc name", "verdict", "source"]

    summary_col_hits = sum(1 for c in cols if any(k.lower() in c for k in summary_kw))
    raw_col_hits = sum(1 for c in cols if any(k in c for k in raw_kw))

    first_col = df.iloc[:, 0].dropna().astype(str).head(20)
    marker_hits = first_col.str.contains(r"统计|---|无\\s*POC|无质检|无初标", regex=True).sum()

    if summary_col_hits >= 2 and raw_col_hits == 0:
        return True
    if marker_hits >= 2 and raw_col_hits == 0:
        return True
    return False

def prepare_stats_data(
    annotators: pd.DataFrame,
    qas: pd.DataFrame,
    pocs: pd.DataFrame,
    difficulty: float = 1.0,
    sop_url: str = "",
    difficulty_report: str = ""
) -> list:
    """
    准备统计结果数据
    Returns: 用于写入的二维列表数据
    """
    all_rows = []

    # 表头
    all_rows.append(["=== 数据标注准确率统计报告 ==="])
    all_rows.append(["生成时间：" + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')])
    all_rows.append(["最终难度系数", f"{difficulty:.2f}"])
    if sop_url:
        all_rows.append(["SOP 文档", sop_url])
    all_rows.append([""])

    # 初标人统计
    all_rows.append(["--- 初标人统计 ---"])
    if not annotators.empty:
        annotator_headers = ["初标人", "初标总产量", "被质检数", "质检通过数", "初标准确率"]
        if "加权初标准确率" in annotators.columns:
            annotator_headers.append("加权初标准确率")
        all_rows.append(annotator_headers)
        for _, row in annotators.iterrows():
            item = [
                str(row.get("初标人", "")),
                str(int(row.get("初标总产量", 0))),
                str(int(row.get("被质检数", 0))),
                str(int(row.get("质检通过数", 0))),
                row.get("初标准确率", "")
            ]
            if "加权初标准确率" in annotators.columns:
                item.append(row.get("加权初标准确率", ""))
            all_rows.append(item)
    else:
        all_rows.append(["无初标数据"])

    all_rows.append([""])

    # 质检人统计
    all_rows.append(["--- 质检人统计 ---"])
    if not qas.empty:
        qa_headers = ["质检人", "质检总产量", "被抽检数", "抽检通过数", "质检准确率"]
        if "加权质检准确率" in qas.columns:
            qa_headers.append("加权质检准确率")
        all_rows.append(qa_headers)
        for _, row in qas.iterrows():
            item = [
                str(row.get("质检人", "")),
                str(int(row.get("质检总产量", 0))),
                str(int(row.get("被抽检数", 0))),
                str(int(row.get("抽检通过数", 0))),
                row.get("质检准确率", "")
            ]
            if "加权质检准确率" in qas.columns:
                item.append(row.get("加权质检准确率", ""))
            all_rows.append(item)
    else:
        all_rows.append(["无质检数据"])

    all_rows.append([""])

    # POC 统计
    all_rows.append(["--- POC 抽检统计 ---"])
    if not pocs.empty:
        all_rows.append(["POC 姓名", "抽检产量"])
        for _, row in pocs.iterrows():
            all_rows.append([
                str(row.get('POC 姓名', '')),
                str(int(row.get('抽检产量', 0)))
            ])
    else:
        all_rows.append(["无 POC 数据"])

    if difficulty_report:
        all_rows.append([""])
        all_rows.append(["--- 难度系数评估摘要（模型输出）---"])
        for line in difficulty_report.splitlines()[:40]:
            if line.strip():
                all_rows.append([line.strip()[:1000]])

    return all_rows

def read_feishu_sheet(
    spreadsheet_token: str,
    sheet_name: str = None,
    token: str = None,
    header_row: Optional[int] = None
) -> pd.DataFrame:
    """
    读取飞书表格数据

    Args:
        spreadsheet_token: 表格 token（URL 中的 Bv28sKowmhExvctX3HDl2XWCgoc 部分）
        sheet_name: 工作表名称，默认读取第一个
        token: 可选的访问令牌

    Returns:
        DataFrame: 表格数据
    """
    # 获取 token
    if token is None:
        token = get_user_access_token()

    headers = {"Authorization": f"Bearer {token}"}

    # 1. 获取元信息
    meta_url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
    resp = httpx.get(meta_url, headers=headers)
    meta = resp.json()

    if meta.get("code") != 0:
        raise Exception(f"获取元信息失败：{meta.get('msg')}")

    sheets = meta.get("data", {}).get("sheets", [])
    if not sheets:
        raise Exception("表格为空")

    # 选择工作表
    target_sheet = sheets[0]
    if sheet_name:
        found = None
        for s in sheets:
            if s.get("title") == sheet_name or s.get("sheetId") == sheet_name:
                found = s
                break
        if found is None:
            sheet_name_l = str(sheet_name).strip().lower()
            for s in sheets:
                if (
                    str(s.get("title", "")).strip().lower() == sheet_name_l
                    or str(s.get("sheetId", "")).strip().lower() == sheet_name_l
                ):
                    found = s
                    break
        if found is None:
            candidates = [f"{s.get('title')}({s.get('sheetId')})" for s in sheets[:20]]
            raise Exception(
                f"未找到工作表：{sheet_name}。可选工作表（前20个）: {candidates}"
            )
        target_sheet = found

    sheet_id = target_sheet.get("sheetId")
    sheet_title = target_sheet.get("title") or sheet_name or sheet_id
    column_count = target_sheet.get("columnCount", 100)
    row_count = target_sheet.get("rowCount", 1000)
    meta_data = meta.get("data", {}) or {}
    properties = meta_data.get("properties") or {}
    spreadsheet_title = (
        properties.get("title")
        or meta_data.get("title")
        or meta_data.get("spreadsheetTitle")
        or meta_data.get("name")
        or spreadsheet_token
    )

    end_col = num_to_col(min(column_count, 52))  # 最多到 AZ 列

    def fetch_values_range(start_row: int, end_row: int) -> dict:
        range_str_local = f"{sheet_id}!A{start_row}:{end_col}{end_row}"
        data_url_local = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_str_local}"
        resp_local = httpx.get(data_url_local, headers=headers)
        return resp_local.json()

    # 2. 读取数据（大表超 10MB 时自动分块）
    max_rows = min(row_count, 5000)
    data = fetch_values_range(1, max_rows)

    if data.get("code") == 0:
        values = data.get("data", {}).get("valueRange", {}).get("values", [])
    else:
        msg = str(data.get("msg") or "")
        if "data exceeded" not in msg.lower():
            raise Exception(f"读取数据失败：{data.get('msg')}")

        print("检测到单次读取超限，启用分块读取（按行）...")
        values = []
        start_row = 1
        chunk_size = min(800, max_rows)
        min_chunk = 50

        while start_row <= max_rows:
            cur_size = min(chunk_size, max_rows - start_row + 1)
            while True:
                end_row = start_row + cur_size - 1
                part = fetch_values_range(start_row, end_row)
                if part.get("code") == 0:
                    part_values = part.get("data", {}).get("valueRange", {}).get("values", [])
                    if part_values:
                        values.extend(part_values)
                    break

                part_msg = str(part.get("msg") or "")
                if "data exceeded" in part_msg.lower() and cur_size > min_chunk:
                    cur_size = max(min_chunk, cur_size // 2)
                    continue
                raise Exception(f"读取数据失败：{part_msg}")

            start_row += cur_size

    if not values:
        raise Exception("表格为空")

    df = build_dataframe_from_values(values, header_row=header_row)
    df.attrs["spreadsheet_token"] = spreadsheet_token
    df.attrs["spreadsheet_title"] = spreadsheet_title
    df.attrs["sheet_id"] = sheet_id
    df.attrs["sheet_title"] = sheet_title
    return df


def call_feishu_open_api(endpoint: str, token: str, params: dict = None) -> dict:
    """调用飞书开放接口并返回 data 字段"""
    url = f"https://open.feishu.cn/open-apis{endpoint}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = httpx.get(url, headers=headers, params=params)
    try:
        data = resp.json()
    except Exception:
        raise Exception(
            f"飞书 API 返回非 JSON（{endpoint}，HTTP {resp.status_code}）：{resp.text[:300]}"
        )
    if data.get("code") != 0:
        raise Exception(f"飞书 API 调用失败({endpoint})：{data.get('msg')}")
    return data.get("data", {})


def parse_feishu_url(url: str) -> Tuple[str, str]:
    """解析飞书 URL，返回 (类型, token)"""
    patterns = {
        "docx": r"(?:docx|docs)/([a-zA-Z0-9]+)",
        "wiki": r"wiki/([a-zA-Z0-9]+)",
        "sheet": r"sheets/([a-zA-Z0-9]+)",
    }
    for doc_type, pattern in patterns.items():
        match = re.search(pattern, url)
        if match:
            return doc_type, match.group(1)
    raise ValueError(f"无法识别的飞书 URL：{url}")


def extract_spreadsheet_token_from_url(url: str) -> str:
    """从飞书表格 URL 提取 spreadsheet token"""
    match = re.search(r"sheets/([a-zA-Z0-9]+)", url or "")
    if not match:
        raise ValueError(f"无法从 URL 提取表格 token：{url}")
    return match.group(1)


def extract_sheet_ref_from_url(url: str) -> Optional[str]:
    """从 URL query 提取 sheet 参数。"""
    if not url:
        return None
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        values = params.get("sheet") or []
        if values and str(values[0]).strip():
            return str(values[0]).strip()
    except Exception:
        return None
    return None


def resolve_spreadsheet_info_from_url(url: str, token: str) -> Tuple[str, Optional[str]]:
    """
    从 URL 解析 spreadsheet 信息：
    - sheets 链接：直接提取 token
    - wiki 链接：通过 wiki node 解析到 obj_token（必须是 sheet）
    返回: (spreadsheet_token, spreadsheet_title_or_none)
    """
    if not url:
        raise ValueError("URL 为空")

    sheet_match = re.search(r"sheets/([a-zA-Z0-9]+)", url)
    if sheet_match:
        return sheet_match.group(1), None

    doc_type, doc_token = parse_feishu_url(url)
    if doc_type != "wiki":
        raise ValueError(f"当前 URL 暂不支持解析为表格 token：{url}")

    node_data = call_feishu_open_api("/wiki/v2/spaces/get_node", token, params={"token": doc_token})
    node_data = node_data.get("node", node_data)
    obj_type = node_data.get("obj_type", "")
    obj_token = node_data.get("obj_token", "")
    if obj_type != "sheet" or not obj_token:
        raise ValueError(f"wiki 节点不是表格（obj_type={obj_type}）")
    return obj_token, (node_data.get("title") or None)


def resolve_spreadsheet_token_from_url(url: str, token: str) -> str:
    spreadsheet_token, _ = resolve_spreadsheet_info_from_url(url, token)
    return spreadsheet_token


def normalize_name_key(name: Any) -> str:
    """姓名键归一化（大小写、重音、空格、常见分隔符）"""
    if name is None:
        return ""
    text = str(name).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[\\s\\-\\.'\"`’·•]+", "", text)
    return text


def load_name_roster(path: str) -> list:
    """加载全名名单"""
    p = Path(path)
    if not p.exists():
        return []
    lines = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines()]
    return [ln for ln in lines if ln]


def build_name_alias_index(roster: list) -> Dict[str, str]:
    """
    构建别名->全名索引：
    - 全名精确（大小写/重音鲁棒）
    - 去后缀基名（例如 xxx_TMX -> xxx）
    - 手工拼音别名（如 yihan -> 王乙琀）
    仅保留“唯一映射”别名，避免歧义错配。
    """
    alias_to_candidates: Dict[str, set] = {}

    def put(alias: str, full_name: str):
        k = normalize_name_key(alias)
        if not k:
            return
        alias_to_candidates.setdefault(k, set()).add(full_name)

    roster_set = set(roster)
    for full in roster:
        put(full, full)
        if "_" in full:
            base = full.split("_", 1)[0].strip()
            if base:
                put(base, full)

    for alias, full in MANUAL_NAME_ALIAS.items():
        if full in roster_set:
            put(alias, full)

    index: Dict[str, str] = {}
    for k, candidates in alias_to_candidates.items():
        if len(candidates) == 1:
            index[k] = next(iter(candidates))
    return index


def resolve_full_name(name: Any, alias_index: Dict[str, str]) -> Any:
    """将别名映射为全名（支持逗号/斜杠分隔多姓名）"""
    if name is None:
        return name
    text = str(name).strip()
    if not text:
        return name

    parts = [p.strip() for p in re.split(r"[，,;/；|]+", text) if p.strip()]
    if len(parts) > 1:
        mapped = []
        for p in parts:
            k = normalize_name_key(p)
            mapped.append(alias_index.get(k, p))
        return " / ".join(mapped)

    k = normalize_name_key(text)
    return alias_index.get(k, text)


def apply_name_standardization(
    annotators: pd.DataFrame,
    qas: pd.DataFrame,
    pocs: pd.DataFrame,
    alias_index: Dict[str, str]
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """将统计结果中的人员字段统一替换为全名"""
    if annotators is not None and not annotators.empty and "初标人" in annotators.columns:
        annotators["初标人"] = annotators["初标人"].apply(lambda x: resolve_full_name(x, alias_index))
    if qas is not None and not qas.empty and "质检人" in qas.columns:
        qas["质检人"] = qas["质检人"].apply(lambda x: resolve_full_name(x, alias_index))
    if pocs is not None and not pocs.empty and "POC 姓名" in pocs.columns:
        pocs["POC 姓名"] = pocs["POC 姓名"].apply(lambda x: resolve_full_name(x, alias_index))
    return annotators, qas, pocs


def read_sop_content(sop_url: str, token: str) -> Tuple[str, str]:
    """读取 SOP 文档内容，当前支持 docx/docs 或 wiki->docx"""
    doc_type, doc_token = parse_feishu_url(sop_url)

    if doc_type == "wiki":
        node_data = call_feishu_open_api("/wiki/v2/spaces/get_node", token, params={"token": doc_token})
        node_data = node_data.get("node", node_data)
        obj_type = node_data.get("obj_type", "")
        obj_token = node_data.get("obj_token", "")
        title = node_data.get("title", "无标题")
        if obj_type != "docx" or not obj_token:
            raise Exception(f"SOP Wiki 节点不是可读取的 docx 文档：{title}({obj_type})")
        doc_token = obj_token

    if doc_type not in ["docx", "wiki"]:
        raise Exception("SOP 目前仅支持飞书 docx/docs 或 wiki(docx) 链接")

    data = call_feishu_open_api(f"/docx/v1/documents/{doc_token}/raw_content", token)
    content = data.get("content", "")
    title = data.get("document", {}).get("title", "无标题")
    if not content:
        raise Exception("SOP 文档内容为空")
    return title, content


def infer_column_type(series: pd.Series) -> str:
    """粗略推断列数据类型"""
    non_null = series.dropna()
    if non_null.empty:
        return "empty"
    numeric_ratio = pd.to_numeric(non_null, errors="coerce").notna().mean()
    if numeric_ratio > 0.9:
        return "number"
    if non_null.astype(str).str.len().mean() > 50:
        return "long_text"
    return "text"


def build_sheet_structure_summary(df: pd.DataFrame) -> str:
    """构建作业表结构摘要（列名、类型、示例）"""
    lines = [f"- 行数: {len(df)}", f"- 列数: {len(df.columns)}", "- 列结构:"]
    for idx, col in enumerate(df.columns):
        col_name = "" if col is None else str(col)
        s = df.iloc[:, idx]
        dtype = infer_column_type(s)
        examples = [str(x)[:80] for x in s.dropna().astype(str).head(3).tolist()]
        lines.append(
            f"  - {col_name or '<空列名>'}[col_{idx}]: type={dtype}, 示例={examples if examples else '[]'}"
        )
    return "\n".join(lines)


def sample_real_tasks(df: pd.DataFrame, sample_size: int = 50) -> Tuple[int, list]:
    """随机抽样真实作业题目，返回 (样本数, 样本列表)"""
    if df.empty:
        return 0, []

    col_names = [str(c).lower() for c in df.columns]
    keywords = [
        "prompt", "question", "题目", "问题", "reference", "answer",
        "response", "comment", "verdict", "result", "gt", "label"
    ]
    selected_idxs = []
    for i, col in enumerate(col_names):
        if any(k in col for k in keywords):
            selected_idxs.append(i)
    if not selected_idxs:
        selected_idxs = list(range(min(8, len(df.columns))))
    selected_idxs = selected_idxs[:12]

    n = min(sample_size, len(df))
    sampled = df.sample(n=n)
    records = []

    def is_missing(v: Any) -> bool:
        try:
            mask = pd.isna(v)
            if isinstance(mask, (bool, np.bool_)):
                return bool(mask)
            if isinstance(mask, np.ndarray):
                return bool(mask.all())
            return False
        except Exception:
            return False

    for _, row in sampled.iterrows():
        item = {}
        used_keys = set()
        for idx in selected_idxs:
            col = df.columns[idx]
            key = str(col) if col is not None else "<空列名>"
            if key in used_keys:
                key = f"{key}[col_{idx}]"
            used_keys.add(key)
            value = row.iloc[idx]
            if is_missing(value):
                continue
            text = str(value).strip()
            if not text:
                continue
            item[key] = text[:500]
        if item:
            records.append(item)
    return len(records), records


def call_modelark_text(prompt: str, api_key: str = None, model: str = None) -> str:
    """调用 ModelArk /responses，返回纯文本输出"""
    api_key = api_key or ARK_API_KEY
    model = model or DEFAULT_MODEL
    client = httpx.Client(timeout=120)
    response = client.post(
        f"{ARK_BASE_URL}/responses",
        json={
            "model": model,
            "input": [{"role": "user", "content": prompt}],
            "stream": False
        },
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    )
    response.raise_for_status()
    data = response.json()

    chunks = []
    for item in data.get("output", []):
        if item.get("type") != "message":
            continue
        for content_item in item.get("content", []):
            if content_item.get("type") == "output_text":
                chunks.append(content_item.get("text", ""))
    return "".join(chunks).strip()


def build_difficulty_eval_prompt(
    sop_title: str,
    sop_content: str,
    structure_summary: str,
    sample_records: list
) -> str:
    """构建难度系数评估提示词"""
    sample_json = json.dumps(sample_records, ensure_ascii=False, indent=2)
    sop_excerpt = sop_content[:20000]
    return f"""
你是大模型训练数据标注项目的质控专家。请基于以下输入，评估“最终难度系数”（1.00~1.50，步长0.01）。

你必须严格执行以下约束：
1. 先完成“强制准备工作”，再评分。
2. 必须基于提供的真实作业样本进行判断（已提供随机抽样样本，最多50条）。
3. 评估维度固定为4个：
   - 评分标准明确度
   - 决策复杂度
   - 专业知识要求
   - 主观判断占比
4. 最终难度系数 = 4个维度评分平均值，四舍五入保留2位小数，范围限制在1.00~1.50。

【SOP文档标题】
{sop_title}

【SOP文档内容】
{sop_excerpt}

【作业表结构分析输入】
{structure_summary}

【随机抽样真实作业题目（最多50条）】
{sample_json}

请先输出 Markdown 报告，必须包含以下区块：
1. 前置分析报告（SOP环节、表结构、SOP与表映射、专家工作内容、评估范围及依据）
2. 难度系数评估表（4个维度+最终难度系数）
3. 评分说明

然后在最后单独输出一行机器可解析结果，格式必须是：
@@DIFFICULTY_JSON@@{{"final_difficulty": 1.23, "dimension_scores": {{"评分标准明确度": 1.2, "决策复杂度": 1.3, "专业知识要求": 1.2, "主观判断占比": 1.2}}, "scope": "...", "reason_summary": "..."}}
"""


def extract_difficulty_score(model_output: str) -> float:
    """从模型输出中提取难度系数"""
    marker_match = re.search(r"@@DIFFICULTY_JSON@@\s*(\{.*\})", model_output, flags=re.S)
    if marker_match:
        try:
            payload = json.loads(marker_match.group(1))
            score = float(payload.get("final_difficulty"))
            return round(min(max(score, DIFFICULTY_MIN), DIFFICULTY_MAX), 2)
        except Exception:
            pass

    direct_match = re.search(r"最终难度系数[^0-9]*(1(?:\.\d{1,2})?)", model_output)
    if direct_match:
        score = float(direct_match.group(1))
        return round(min(max(score, DIFFICULTY_MIN), DIFFICULTY_MAX), 2)

    candidates = [float(x) for x in re.findall(r"\b1(?:\.\d{1,2})\b", model_output)]
    candidates = [x for x in candidates if DIFFICULTY_MIN <= x <= DIFFICULTY_MAX]
    if candidates:
        return round(candidates[-1], 2)

    raise Exception("无法从难度评估输出中解析最终难度系数")


def evaluate_difficulty_coefficient(sop_url: str, df: pd.DataFrame, token: str) -> Tuple[float, str]:
    """读取 SOP + 抽样作业内容，调用模型评估难度系数"""
    sop_title, sop_content = read_sop_content(sop_url, token)
    structure_summary = build_sheet_structure_summary(df)
    sample_count, sample_records = sample_real_tasks(df, sample_size=50)
    if sample_count == 0:
        raise Exception("作业表无有效样本，无法评估难度系数")

    prompt = build_difficulty_eval_prompt(
        sop_title=sop_title,
        sop_content=sop_content,
        structure_summary=structure_summary,
        sample_records=sample_records
    )
    model_output = call_modelark_text(prompt)
    difficulty = extract_difficulty_score(model_output)
    return difficulty, model_output


def parse_percent(value: Any) -> Optional[float]:
    """解析百分比文本为小数，例 '36.36%' -> 0.3636"""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in ["无质检数据", "无抽检数据"]:
        return None
    m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", text)
    if m:
        return float(m.group(1)) / 100.0
    try:
        num = float(text)
        return num if num <= 1 else num / 100.0
    except Exception:
        return None


def normalize_cell_value(value: Any) -> Any:
    """将 list/dict 等复杂单元格转为可统计文本；空值转 np.nan"""
    try:
        na_mask = pd.isna(value)
        if isinstance(na_mask, (bool, np.bool_)) and bool(na_mask):
            return np.nan
    except Exception:
        pass

    if isinstance(value, list):
        items = [str(v).strip() for v in value if str(v).strip()]
        return ", ".join(items) if items else np.nan

    if isinstance(value, tuple):
        items = [str(v).strip() for v in value if str(v).strip()]
        return ", ".join(items) if items else np.nan

    if isinstance(value, dict):
        for key in ["text", "name", "label", "value"]:
            v = value.get(key)
            if v is not None and str(v).strip():
                return str(v).strip()
        content = json.dumps(value, ensure_ascii=False)
        return content if content else np.nan

    text = str(value).strip()
    return text if text else np.nan


def column_quality(series: pd.Series) -> Dict[str, float]:
    """列质量指标"""
    s = series.dropna().astype(str).str.strip()
    s = s[s != ""]
    total = len(s)
    if total == 0:
        return {
            "count": 0.0,
            "unique_ratio": 1.0,
            "avg_len": 0.0,
            "url_ratio": 0.0,
            "numeric_ratio": 0.0,
        }
    unique_ratio = s.nunique() / total
    avg_len = float(s.str.len().mean())
    url_ratio = float(s.str.contains(r"https?://|www\\.", case=False, regex=True).mean())
    numeric_ratio = float(pd.to_numeric(s, errors="coerce").notna().mean())
    return {
        "count": float(total),
        "unique_ratio": float(unique_ratio),
        "avg_len": avg_len,
        "url_ratio": url_ratio,
        "numeric_ratio": numeric_ratio,
    }


def is_valid_actor_column(series: pd.Series) -> bool:
    """判断是否适合作为人员字段列"""
    q = column_quality(series)
    if q["count"] < 3:
        return False
    if q["url_ratio"] > 0.2:
        return False
    if q["avg_len"] > 40:
        return False
    if q["numeric_ratio"] > 0.8:
        return False
    if q["unique_ratio"] > 0.98 and q["count"] >= 20:
        return False
    return True


def sanitize_standard_columns(df: pd.DataFrame) -> pd.DataFrame:
    """对标准列做值归一化和质量拦截，避免错误列污染统计"""
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(normalize_cell_value)

    for actor_col in ["初标人", "质检人", "POC 姓名"]:
        if actor_col in df.columns:
            q = column_quality(df[actor_col])
            if q["count"] == 0:
                continue
            if is_valid_actor_column(df[actor_col]):
                continue
            print(f"[质量拦截] 列 {actor_col} 判定为低质量（可能是链接/长文本/高基数字段），已跳过该列统计。")
            df[actor_col] = np.nan
    return df


def _find_col_by_alias(col_map: Dict[str, Any], aliases: list) -> Optional[Any]:
    for a in aliases:
        k = str(a).strip().lower()
        if k in col_map:
            return col_map[k]
    return None


def _norm_col_key(name: Any) -> str:
    return str(name).strip().lower()


def _looks_like_same_column(df: pd.DataFrame, col: Any) -> bool:
    """判断某列是否像 same/not same 判定列（包含文本值或公式）。"""
    s = df[col].dropna().astype(str).str.strip().str.lower()
    if s.empty:
        return False

    text_hits = s.str.contains(r"\bsame\b|not\s*same|一致|不一致", regex=True).sum()
    if text_hits >= max(3, int(len(s) * 0.2)):
        return True

    formula_hits = s.str.contains(r'if\(|"same"|not\s*same', regex=True).sum()
    return formula_hits >= max(3, int(len(s) * 0.2))


def _find_same_column(df: pd.DataFrame, col_map: Dict[str, Any]) -> Optional[Any]:
    # 1) 强别名
    c = _find_col_by_alias(col_map, ["result", "same", "是否一致", "一致性", "一致判断"])
    if c is not None:
        return c

    # 2) 值分布/公式特征识别
    candidates = []
    for col in df.columns:
        if _looks_like_same_column(df, col):
            # 简单打分：命中文本越多越优先
            s = df[col].dropna().astype(str).str.strip().str.lower()
            score = int(s.str.contains(r"\bsame\b|not\s*same|一致|不一致", regex=True).sum())
            candidates.append((score, col))
    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]
    return None


def _find_poc_q_column(col_map: Dict[str, Any]) -> Optional[Any]:
    """
    自动识别 POC/Q 列（可为空）：
    仅使用明确语义别名，避免把商讨结果列误识别为 Q。
    """
    aliases = [
        "cc",
        "cc verdict",
        "cc result",
        "poc verdict",
        "poc result",
        "poc 结果",
        "抽检结果",
        "终审结果",
        "终审结论",
        "q",
        "q列",
    ]
    c = _find_col_by_alias(col_map, aliases)
    if c is not None:
        return c

    # 包含匹配（排除 __3 讨论列）
    for k, v in col_map.items():
        if k.endswith("__3"):
            continue
        if any(x in k for x in ["poc verdict", "poc result", "抽检结果", "cc verdict", "cc result"]):
            return v
    return None


def _is_aux_back_to_back_base(base: str) -> bool:
    """
    判断是否为“辅助判定字段”，默认不纳入两位初标结果一致性比较。
    例如 If DCG<3 常用于过程判断而非最终标签，不应影响“结果一致”判定。
    """
    b = str(base).strip().lower()
    b_nospace = re.sub(r"\s+", "", b)
    if "ifdcg<3" in b_nospace or "ifdcg" in b_nospace:
        return True
    return False


def _build_back_to_back_triplets(df: pd.DataFrame, col_map: Dict[str, Any]) -> list:
    """
    自动构建背靠背对比列组：
    annotator1(base) <-> annotator2(base__2) [可选] <-> discussion(base__3)
    - 允许没有 discussion(__3) 的表（例如仅两位初标，无商讨列）
    """
    excluded_bases = {
        "prompt",
        "response",
        "date",
        "name",
        "name-1",
        "name_1",
        "name 1",
        "name__2",
        "name-2",
        "name_2",
        "name 2",
        "discussion name",
        "comment",
        "comments",
        "result",
        "same",
        "cc",
    }

    index_map = {c: i for i, c in enumerate(df.columns)}
    triplets = []
    for col in df.columns:
        base = _norm_col_key(col)
        if base.endswith("__2") or base.endswith("__3"):
            continue
        if base in excluded_bases:
            continue
        c1 = col_map.get(base)
        c2 = col_map.get(f"{base}__2")
        c3 = col_map.get(f"{base}__3")
        if c1 is None or c2 is None:
            continue
        triplets.append(
            {
                "base": base,
                "a1": c1,
                "a2": c2,
                "disc": c3,  # 可能为 None
                "idx": index_map.get(c1, 0),
            }
        )

    preferred = [t for t in triplets if not _is_aux_back_to_back_base(t["base"])]
    if preferred:
        triplets = preferred

    triplets.sort(key=lambda x: x["idx"])
    return triplets


def resolve_back_to_back_columns(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    动态解析背靠背初标列：
    - 初标1姓名：name / name-1
    - 初标2姓名：name__2 / name-2
    - 初标结果对比列：自动匹配 base/base__2（如 PT, first_label, sec_label）
    - 一致性列：Result
    - 商讨结果：可选 base__3（若存在）
    - 质检列Q：CC / POC Verdict 等（可为空）
    """
    if df is None or df.empty:
        return None

    col_map = {str(c).strip().lower(): c for c in df.columns}

    c_name1 = _find_col_by_alias(
        col_map,
        ["name", "name-1", "name_1", "name 1", "annotator 1", "annotator1", "sp name-1", "sp name 1"],
    )
    c_name2 = _find_col_by_alias(
        col_map,
        ["name__2", "name-2", "name_2", "name 2", "annotator 2", "annotator2", "sp name__2", "sp name-2", "sp name 2"],
    )
    c_same = _find_same_column(df, col_map)
    c_poc = _find_poc_q_column(col_map)  # 可为空
    triplets = _build_back_to_back_triplets(df, col_map)

    # 至少需要：两位初标人 + 至少一组可对比列组（允许没有 __3）
    if c_name1 is None or c_name2 is None or not triplets:
        return None

    return {
        "name1": c_name1,
        "name2": c_name2,
        "triplets": triplets,
        "same": c_same,  # 可为空，届时通过 triplet 比较推导
        "poc": c_poc,
    }


def detect_back_to_back_schema(df: pd.DataFrame) -> bool:
    """先判定是否背靠背初标结构，命中则优先走背靠背规则。"""
    return resolve_back_to_back_columns(df) is not None


def normalize_compare_value(v: Any) -> str:
    """用于标签对比的归一化文本。"""
    if v is None:
        return ""
    text = str(v).strip().lower()
    text = re.sub(r"\\s+", " ", text)
    return text


def parse_zero_one_flag(v: Any) -> Optional[int]:
    """将质检标记解析为 0/1。"""
    if v is None:
        return None
    text = str(v).strip().lower()
    if not text:
        return None

    if text in {"1", "1.0", "true", "yes", "y", "pass", "通过"}:
        return 1
    if text in {"0", "0.0", "false", "no", "n", "fail", "不通过"}:
        return 0

    m = re.search(r"-?\\d+(?:\\.\\d+)?", text)
    if m:
        try:
            num = float(m.group(0))
            if abs(num - 1.0) < 1e-9:
                return 1
            if abs(num - 0.0) < 1e-9:
                return 0
        except Exception:
            return None
    return None


def is_same_flag(v: Any) -> bool:
    """判断 L 列是否表示 same。"""
    text = normalize_compare_value(v)
    return text in {"same", "一致", "相同", "一样", "1"}


def is_not_same_flag(v: Any) -> bool:
    """判断 L 列是否表示 not same。"""
    text = normalize_compare_value(v)
    return text in {"not same", "notsame", "不一致", "不相同", "不同", "0"}


def calculate_back_to_back_annotator_stats(df: pd.DataFrame, debug: bool = False) -> tuple:
    """
    背靠背初标专用统计：
    - D: name（初标1）
    - F/G: first_label/sec_label（初标1结果）
    - H: name__2（初标2）
    - J/K: first_label__2/sec_label__2（初标2结果）
    - 一致性：优先 same/result 列；若不存在则自动比较两位初标结果列组
    - 商讨结果：若存在 __3 列组，则在 not same 场景用于判定谁通过
    - Q/POC：可选，若为0则双方不通过；为空或1按规则继续
    """
    resolved = resolve_back_to_back_columns(df)
    if not resolved:
        raise Exception("当前表未识别为背靠背结构，无法使用背靠背统计规则")

    c_name1 = resolved["name1"]
    c_name2 = resolved["name2"]
    triplets = resolved["triplets"]
    c_same = resolved.get("same")
    c_poc = resolved["poc"]

    stats: Dict[str, Dict[str, float]] = {}

    def ensure_person(name: str):
        if name not in stats:
            stats[name] = {"初标总产量": 0.0, "被质检数": 0.0, "质检通过数": 0.0}

    debug_q_raw: Dict[str, int] = {}
    debug_l_raw: Dict[str, int] = {}
    debug_pass_same: Dict[str, int] = {}
    debug_pass_notsame: Dict[str, int] = {}
    dbg_rows_total = 0
    dbg_q0 = 0
    dbg_q1 = 0
    dbg_qnone = 0
    dbg_same_q1_or_empty = 0
    dbg_notsame_q_empty_or_1 = 0

    if debug:
        print("[B2B 调试] 解析列：")
        print("  name1 =", c_name1)
        print("  name2 =", c_name2)
        print("  same  =", c_same)
        print("  q/poc =", c_poc)
        print("  triplets =", [(t["base"], t["a1"], t["a2"], t["disc"]) for t in triplets])

    for _, row in df.iterrows():
        dbg_rows_total += 1
        name1 = normalize_cell_value(row.get(c_name1))
        name2 = normalize_cell_value(row.get(c_name2))
        a1_pair = tuple(normalize_compare_value(row.get(t["a1"])) for t in triplets)
        a2_pair = tuple(normalize_compare_value(row.get(t["a2"])) for t in triplets)

        if isinstance(name1, str):
            ensure_person(name1)
            stats[name1]["初标总产量"] += 1
            stats[name1]["被质检数"] += 1
        if isinstance(name2, str):
            ensure_person(name2)
            stats[name2]["初标总产量"] += 1
            stats[name2]["被质检数"] += 1

        same_flag = is_same_flag(row.get(c_same)) if c_same is not None else False
        not_same_flag = is_not_same_flag(row.get(c_same)) if c_same is not None else False
        # L 列有时是公式文本（如 IF(...)），无法直接读取结果。
        # 若 same 列为空/不可解析，则用两位初标结果列组比较推导。
        if not same_flag and not not_same_flag:
            non_empty = any(v != "" for v in a1_pair) or any(v != "" for v in a2_pair)
            if non_empty and a1_pair == a2_pair:
                same_flag = True
            else:
                not_same_flag = True
        poc_flag = parse_zero_one_flag(row.get(c_poc)) if c_poc is not None else None
        if debug:
            q_raw = str(normalize_cell_value(row.get(c_poc))) if c_poc is not None else "<MISSING>"
            l_raw = str(normalize_cell_value(row.get(c_same))) if c_same is not None else "<MISSING>"
            debug_q_raw[q_raw] = debug_q_raw.get(q_raw, 0) + 1
            debug_l_raw[l_raw] = debug_l_raw.get(l_raw, 0) + 1
            if poc_flag == 0:
                dbg_q0 += 1
            elif poc_flag == 1:
                dbg_q1 += 1
            else:
                dbg_qnone += 1

        # 规则补充：L=same 且 Q 为空或1，两个初标都通过
        if same_flag and (poc_flag is None or poc_flag == 1):
            if debug:
                dbg_same_q1_or_empty += 1
            if isinstance(name1, str):
                stats[name1]["质检通过数"] += 1
                if debug:
                    debug_pass_same[name1] = debug_pass_same.get(name1, 0) + 1
            if isinstance(name2, str):
                stats[name2]["质检通过数"] += 1
                if debug:
                    debug_pass_same[name2] = debug_pass_same.get(name2, 0) + 1
            continue

        # Q=0：两位初标都不通过
        if poc_flag == 0:
            continue

        # L=not same 且 Q 为空或1：若有商讨列，比较商讨结果与两位初标结果（顺序必须一致）
        if not_same_flag and (poc_flag is None or poc_flag == 1):
            if debug:
                dbg_notsame_q_empty_or_1 += 1
            discuss_triplets = [t for t in triplets if t.get("disc") is not None]
            if not discuss_triplets:
                continue

            discuss_pair = tuple(normalize_compare_value(row.get(t["disc"])) for t in discuss_triplets)
            a1_disc_pair = tuple(normalize_compare_value(row.get(t["a1"])) for t in discuss_triplets)
            a2_disc_pair = tuple(normalize_compare_value(row.get(t["a2"])) for t in discuss_triplets)

            if discuss_pair == a1_disc_pair and isinstance(name1, str):
                stats[name1]["质检通过数"] += 1
                if debug:
                    debug_pass_notsame[name1] = debug_pass_notsame.get(name1, 0) + 1
            if discuss_pair == a2_disc_pair and isinstance(name2, str):
                stats[name2]["质检通过数"] += 1
                if debug:
                    debug_pass_notsame[name2] = debug_pass_notsame.get(name2, 0) + 1

    rows = []
    for person, m in stats.items():
        inspected = m["被质检数"]
        passed = m["质检通过数"]
        acc_text = f"{(passed / inspected):.2%}" if inspected > 0 else "无质检数据"
        rows.append(
            {
                "初标人": person,
                "初标总产量": int(m["初标总产量"]),
                "被质检数": int(inspected),
                "质检通过数": int(passed),
                "初标准确率": acc_text,
            }
        )

    annotator_stats = pd.DataFrame(rows)
    if not annotator_stats.empty:
        annotator_stats = annotator_stats.sort_values(by=["初标总产量", "初标人"], ascending=[False, True]).reset_index(drop=True)

    if debug:
        print("[B2B 调试] 总行数:", dbg_rows_total)
        print("[B2B 调试] Q解析分布: q=0", dbg_q0, "q=1", dbg_q1, "q=空/其它", dbg_qnone)
        print("[B2B 调试] 命中规则: same&(q空或1)", dbg_same_q1_or_empty, "notsame&(q空或1)", dbg_notsame_q_empty_or_1)
        top_q = sorted(debug_q_raw.items(), key=lambda x: x[1], reverse=True)[:12]
        top_l = sorted(debug_l_raw.items(), key=lambda x: x[1], reverse=True)[:12]
        top_same = sorted(debug_pass_same.items(), key=lambda x: x[1], reverse=True)[:20]
        top_notsame = sorted(debug_pass_notsame.items(), key=lambda x: x[1], reverse=True)[:20]
        print("[B2B 调试] Q原值Top12:", top_q)
        print("[B2B 调试] L原值Top12:", top_l)
        print("[B2B 调试] 按人 same通过:", top_same)
        print("[B2B 调试] 按人 notsame通过:", top_notsame)

    # 该模式下无质检人统计与 POC 产量统计
    qa_stats = pd.DataFrame()
    poc_stats = pd.DataFrame()
    return annotator_stats, qa_stats, poc_stats


def apply_weighted_accuracy(df_stats: pd.DataFrame, base_col: str, weighted_col: str, difficulty: float) -> pd.DataFrame:
    """新增加权准确率列"""
    if df_stats.empty or base_col not in df_stats.columns:
        return df_stats

    def to_weighted_text(v):
        base = parse_percent(v)
        if base is None:
            return "无数据"
        return f"{(base * difficulty):.2%}"

    df_stats[weighted_col] = df_stats[base_col].apply(to_weighted_text)
    df_stats["最终难度系数"] = f"{difficulty:.2f}"
    return df_stats


def parse_number(value: Any) -> Optional[float]:
    """解析计数字段，兼容 int/float/字符串。"""
    if value is None:
        return None
    if isinstance(value, (int, float, np.integer, np.floating)):
        try:
            if np.isnan(value):
                return None
        except Exception:
            pass
        return float(value)

    text = str(value).strip().replace(",", "")
    if not text:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _manual_aggregate_project_rows(person_rows: list, difficulty: float) -> list:
    """在 panel_metrics 不可用时，兜底聚合项目级指标。"""
    by_role = {}
    for row in person_rows:
        role = row.get("role") or "未知"
        agg = by_role.setdefault(
            role,
            {"metric_group": role, "volume_total": 0.0, "inspected_total": 0.0, "pass_total": 0.0},
        )
        volume = row.get("volume")
        inspected = row.get("inspected_count")
        passed = row.get("pass_count")
        if volume is not None:
            agg["volume_total"] += float(volume)
        if inspected is not None:
            agg["inspected_total"] += float(inspected)
        if passed is not None:
            agg["pass_total"] += float(passed)

    project_rows = []
    overall_volume = 0.0
    overall_inspected = 0.0
    overall_pass = 0.0
    for role, agg in by_role.items():
        inspected = agg["inspected_total"]
        passed = agg["pass_total"]
        accuracy = (passed / inspected) if inspected > 0 else None
        weighted = (accuracy * difficulty) if accuracy is not None else None
        row = {
            "metric_group": role,
            "volume_total": agg["volume_total"],
            "inspected_total": inspected,
            "pass_total": passed,
            "accuracy": accuracy,
            "weighted_accuracy": weighted,
            "difficulty_coef": difficulty,
        }
        project_rows.append(row)

        if role in {"初标", "质检"}:
            overall_volume += agg["volume_total"]
            overall_inspected += inspected
            overall_pass += passed

    overall_acc = (overall_pass / overall_inspected) if overall_inspected > 0 else None
    project_rows.append(
        {
            "metric_group": "整体",
            "volume_total": overall_volume,
            "inspected_total": overall_inspected,
            "pass_total": overall_pass,
            "accuracy": overall_acc,
            "weighted_accuracy": (overall_acc * difficulty) if overall_acc is not None else None,
            "difficulty_coef": difficulty,
        }
    )
    return project_rows


def build_panel_snapshot(
    spreadsheet_token: str,
    sheet_ref: str,
    sheet_title: Optional[str],
    spreadsheet_title: Optional[str],
    result_spreadsheet_token: str,
    result_sheet_ref: str,
    project_display_name: Optional[str],
    annotators: pd.DataFrame,
    qas: pd.DataFrame,
    pocs: pd.DataFrame,
    difficulty: float,
    args: argparse.Namespace,
    mapping: dict,
) -> dict:
    """构建写入面板数据库的标准快照。"""
    run_id = str(uuid.uuid4())
    run_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    project_id = f"{spreadsheet_token}:{sheet_ref}"

    person_rows = []

    if annotators is not None and not annotators.empty:
        for _, row in annotators.iterrows():
            person_name = str(row.get("初标人", "")).strip()
            if not person_name:
                continue
            inspected = parse_number(row.get("被质检数"))
            passed = parse_number(row.get("质检通过数"))
            accuracy = parse_percent(row.get("初标准确率"))
            weighted = parse_percent(row.get("加权初标准确率"))
            if weighted is None and accuracy is not None:
                weighted = accuracy * difficulty
            person_rows.append(
                {
                    "project_id": project_id,
                    "person_name": person_name,
                    "role": "初标",
                    "volume": parse_number(row.get("初标总产量")),
                    "inspected_count": inspected,
                    "pass_count": passed,
                    "accuracy": accuracy,
                    "weighted_accuracy": weighted,
                    "difficulty_coef": difficulty,
                }
            )

    if qas is not None and not qas.empty:
        for _, row in qas.iterrows():
            person_name = str(row.get("质检人", "")).strip()
            if not person_name:
                continue
            inspected = parse_number(row.get("被抽检数"))
            passed = parse_number(row.get("抽检通过数"))
            accuracy = parse_percent(row.get("质检准确率"))
            weighted = parse_percent(row.get("加权质检准确率"))
            if weighted is None and accuracy is not None:
                weighted = accuracy * difficulty
            person_rows.append(
                {
                    "project_id": project_id,
                    "person_name": person_name,
                    "role": "质检",
                    "volume": parse_number(row.get("质检总产量")),
                    "inspected_count": inspected,
                    "pass_count": passed,
                    "accuracy": accuracy,
                    "weighted_accuracy": weighted,
                    "difficulty_coef": difficulty,
                }
            )

    if pocs is not None and not pocs.empty:
        for _, row in pocs.iterrows():
            person_name = str(row.get("POC 姓名", "")).strip()
            if not person_name:
                continue
            person_rows.append(
                {
                    "project_id": project_id,
                    "person_name": person_name,
                    "role": "POC",
                    "volume": parse_number(row.get("抽检产量")),
                    "inspected_count": None,
                    "pass_count": None,
                    "accuracy": None,
                    "weighted_accuracy": None,
                    "difficulty_coef": difficulty,
                }
            )

    project_rows = []
    if compute_effective_project_metrics is not None:
        try:
            computed = compute_effective_project_metrics(person_rows, overrides=[])
            project_rows = computed.get("project_metrics", [])
        except Exception:
            project_rows = []
    if not project_rows:
        project_rows = _manual_aggregate_project_rows(person_rows, difficulty)

    project_meta = {
        "project_id": project_id,
        "project_group_id": spreadsheet_token,
        "project_group_name": (spreadsheet_title or spreadsheet_token),
        "spreadsheet_token": spreadsheet_token,
        "sheet_ref": sheet_ref,
        "sheet_title": sheet_title or sheet_ref,
        "display_name": project_display_name or (sheet_title or sheet_ref),
        "result_spreadsheet_token": result_spreadsheet_token,
        "result_sheet_ref": result_sheet_ref,
    }

    run_meta = {
        "run_id": run_id,
        "run_at": run_at,
        "source_type": "workflow_feishu",
        "difficulty_coef": difficulty,
        "sheet_mapping": mapping,
        "operator": args.operator or "",
        "args": {
            "sheet": args.sheet,
            "header_row": args.header_row,
            "sop_url": args.sop_url or "",
            "result_sheet": args.result_sheet,
            "no_write_back": bool(args.no_write_back),
        },
    }

    return {
        "project_id": project_id,
        "run_id": run_id,
        "run_at": run_at,
        "difficulty_coef": difficulty,
        "project_meta": project_meta,
        "run_meta": run_meta,
        "annotators_df": annotators.to_dict(orient="records") if annotators is not None else [],
        "qas_df": qas.to_dict(orient="records") if qas is not None else [],
        "pocs_df": pocs.to_dict(orient="records") if pocs is not None else [],
        "person_metrics_base": person_rows,
        "project_metrics_base": project_rows,
        "project_summary_metrics": project_rows,
    }


def format_df_for_console(df: pd.DataFrame, title: str, max_rows: int = 40) -> str:
    """避免超长日志，控制台仅展示前 N 行，且单元格裁剪"""
    if df.empty:
        return f"{title}\n无数据"

    disp = df.head(max_rows).copy()
    for c in disp.columns:
        disp[c] = disp[c].apply(
            lambda x: (str(x)[:80] + "…") if len(str(x)) > 80 else str(x)
        )

    head_txt = disp.to_string()
    if len(df) <= max_rows:
        return f"{title}\n{head_txt}"
    return f"{title}\n{head_txt}\n...（共 {len(df)} 行，仅展示前 {max_rows} 行）"


def result_signal_score(series: pd.Series) -> float:
    """结果列信号强度，越高越像通过/不通过结果"""
    s = series.dropna().astype(str).str.lower().str.strip()
    s = s[s != ""]
    if s.empty:
        return 0.0
    pass_hit = s.apply(lambda x: any(p.lower() in x for p in PASS_LABELS)).mean()
    fail_hit = s.apply(lambda x: any(f.lower() in x for f in FAIL_LABELS)).mean()
    return float(pass_hit + fail_hit)


def infer_missing_mapping_by_data(df: pd.DataFrame, missing_standard: list) -> dict:
    """基于列值分布推断缺失映射"""
    inferred = {}
    if df is None or df.empty:
        return inferred

    cols = list(df.columns)

    # 人员列候选：短文本、低URL、非高数字列
    actor_candidates = []
    for c in cols:
        q = column_quality(df[c])
        if q["count"] < 3:
            continue
        if q["url_ratio"] > 0.15 or q["numeric_ratio"] > 0.8 or q["avg_len"] > 35:
            continue
        actor_candidates.append((c, q))

    def pick_actor(exclude_cols: set):
        best_col = None
        best_score = -1e9
        for c, q in actor_candidates:
            if c in exclude_cols:
                continue
            # 偏好重复较多（unique_ratio低一些）且文本较短
            score = (1.0 - q["unique_ratio"]) * 3.0 + (35 - q["avg_len"]) * 0.03 + q["count"] * 0.001
            if score > best_score:
                best_score = score
                best_col = c
        return best_col

    used = set()
    for std_col in ["初标人", "质检人"]:
        if std_col in missing_standard:
            c = pick_actor(used)
            if c:
                inferred[c] = std_col
                used.add(c)

    # POC 姓名仅在列名有明显关键词时才自动推断，避免把 Country 等字段误判为人员列
    if "POC 姓名" in missing_standard:
        poc_keywords = ["poc", "audit", "抽检", "终审", "三审"]
        poc_candidates = []
        for c in cols:
            if c in used:
                continue
            name_l = str(c).lower()
            if not any(k in name_l for k in poc_keywords):
                continue
            if is_valid_actor_column(df[c]):
                q = column_quality(df[c])
                score = (1.0 - q["unique_ratio"]) * 3.0 + (35 - q["avg_len"]) * 0.03 + q["count"] * 0.001
                poc_candidates.append((c, score))
        if poc_candidates:
            poc_candidates.sort(key=lambda x: x[1], reverse=True)
            inferred[poc_candidates[0][0]] = "POC 姓名"

    # 结果列候选：通过/不通过信号
    result_candidates = []
    for c in cols:
        if c in used:
            continue
        name_l = str(c).lower()
        if ("validation" in name_l or "check" in name_l) and "verdict" not in name_l:
            continue
        sig = result_signal_score(df[c])
        q = column_quality(df[c])
        if sig >= 0.15 and q["avg_len"] < 80:
            result_candidates.append((c, sig))
    result_candidates.sort(key=lambda x: x[1], reverse=True)

    for std_col in ["质检结果", "抽检结果"]:
        if std_col not in missing_standard:
            continue
        for c, _ in result_candidates:
            if c not in inferred:
                inferred[c] = std_col
                break

    return inferred


def is_mapping_plausible(actual_col: Any, std_col: str) -> bool:
    """过滤明显不合理的映射，避免 Validation 列误映射到结果列"""
    name_l = str(actual_col).lower().strip()
    if std_col in ["质检结果", "抽检结果"]:
        if ("validation" in name_l or "check" in name_l) and "verdict" not in name_l:
            return False
    return True


def set_mapping_with_priority(mapping: dict, actual_col: Any, std_col: str) -> None:
    """设置映射并移除同标准列的旧映射"""
    to_remove = [k for k, v in mapping.items() if v == std_col and k != actual_col]
    for k in to_remove:
        del mapping[k]
    mapping[actual_col] = std_col


def apply_high_confidence_overrides(actual_columns: list, mapping: dict) -> dict:
    """
    高置信规则覆盖：
    - CC Verdict 必须优先映射到 质检结果
    - POC Verdict 必须优先映射到 抽检结果
    - 避免被 GSB/Validation 抢占
    """
    cols_lower = [(c, str(c).lower().strip()) for c in actual_columns]

    def find_first(keyword: str):
        for raw, low in cols_lower:
            if keyword in low:
                return raw
        return None

    cc_verdict_col = find_first("cc verdict")
    if cc_verdict_col is not None:
        set_mapping_with_priority(mapping, cc_verdict_col, "质检结果")
        print(f"[优先级覆盖] 固定映射：'{cc_verdict_col}' -> '质检结果'")

    poc_verdict_col = find_first("poc verdict")
    if poc_verdict_col is not None:
        set_mapping_with_priority(mapping, poc_verdict_col, "抽检结果")
        print(f"[优先级覆盖] 固定映射：'{poc_verdict_col}' -> '抽检结果'")

    # 清理被误映射到结果列的 Validation/Check 列
    invalid_cols = []
    for actual, std in mapping.items():
        if std in ["质检结果", "抽检结果"] and not is_mapping_plausible(actual, std):
            invalid_cols.append(actual)
    for c in invalid_cols:
        print(f"[防误映射] 移除不合理结果列映射：'{c}' -> '{mapping[c]}'")
        del mapping[c]

    return mapping

# ================= 智能列名识别 =================
def intelligent_column_mapping(actual_columns: list, df: Optional[pd.DataFrame] = None, api_key: str = None) -> dict:
    """
    优先使用规则库匹配，匹配不到的交给 Kimi 识别。
    """
    mapping = {}
    unmapped_actual = []
    missing_standard = STANDARD_COLUMNS.copy()

    print(f"原始读取的列名：{actual_columns}")

    # 1. 第一阶段：规则库精准/包含匹配
    for col in actual_columns:
        col_lower = str(col).lower().strip()
        matched = False

        for std_name, aliases in RULE_BASE.items():
            if std_name in missing_standard:
                for alias in aliases:
                    if alias == col_lower or (len(alias) > 3 and alias in col_lower):
                        mapping[col] = std_name
                        missing_standard.remove(std_name)
                        print(f"[规则库命中] 识别到：'{col}' -> '{std_name}'")
                        matched = True
                        break
                    # 精确匹配优先
                    if alias == col_lower:
                        mapping[col] = std_name
                        missing_standard.remove(std_name)
                        print(f"[规则库精确匹配] 识别到：'{col}' -> '{std_name}'")
                        matched = True
                        break
                if matched:
                    break

        if not matched:
            unmapped_actual.append(col)

    # 2. 第二阶段：ModelArk LLM 兜底匹配
    if missing_standard and unmapped_actual:
        print(f"\n[启动 ModelArk 兜底] 尚缺失标准列：{missing_standard}")
        print(f"[启动 ModelArk 兜底] 未识别的实际列：{unmapped_actual}")

        try:
            client = httpx.Client(timeout=60)
            prompt = f"""
你是一个数据分析助手。我需要将一个 Excel 表格的实际列名映射到我的标准列名。

目前还需要匹配的标准列名有：{missing_standard}
表格中剩余未被识别的实际列名有：{unmapped_actual}

请根据语义，将实际列名映射到最合适的标准列名。
输出要求：
1. 仅输出纯 JSON 格式的字典，不要包含任何 Markdown 标记或其他说明文字。
2. JSON 的键为"实际列名"，值为"标准列名"。
3. 如果某个实际列名没有合适的标准列名对应，请不要包含在结果中。

示例输出：
{{"The Annotator Name": "初标人", "QA_Pass_or_Fail": "质检结果"}}
"""
            # 使用配置的 ModelArk API Key
            api_key = api_key or ARK_API_KEY

            # ModelArk 使用 /responses 接口
            response = client.post(
                f"{ARK_BASE_URL}/responses",
                json={
                    "model": DEFAULT_MODEL,
                    "input": [
                        {"role": "user", "content": prompt}
                    ],
                    "stream": False
                },
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json"
                }
            )

            result_data = response.json()

            # ModelArk 返回格式解析
            # output: [{type: "reasoning", ...}, {type: "message", content: [{type: "output_text", text: "..."}]}]
            result_text = ""
            if "output" in result_data:
                for item in result_data["output"]:
                    if item.get("type") == "message":
                        content_list = item.get("content", [])
                        for content_item in content_list:
                            if content_item.get("type") == "output_text":
                                result_text += content_item.get("text", "")

            result_text = result_text.strip()

            # 清理 markdown 标记
            if result_text.startswith("```json"):
                result_text = result_text[7:-3].strip()
            elif result_text.startswith("```"):
                result_text = result_text[3:-3].strip()

            llm_mapping = json.loads(result_text)

            for actual, std in llm_mapping.items():
                if (
                    actual in unmapped_actual
                    and std in missing_standard
                    and is_mapping_plausible(actual, std)
                ):
                    mapping[actual] = std
                    missing_standard.remove(std)
                    print(f"[ModelArk 命中] 识别到：'{actual}' -> '{std}'")
        except Exception as e:
            print(f"[ModelArk 调用失败] 错误信息：{e}")

    # 3. 第三阶段：基于列值分布兜底（适配非标准表头）
    if missing_standard and df is not None and not df.empty:
        inferred = infer_missing_mapping_by_data(df, missing_standard)
        for actual, std in inferred.items():
            if actual not in mapping and std in missing_standard and is_mapping_plausible(actual, std):
                mapping[actual] = std
                missing_standard.remove(std)
                print(f"[数据分布兜底] 识别到：'{actual}' -> '{std}'")

    mapping = apply_high_confidence_overrides(actual_columns, mapping)

    return mapping

# ================= 数据计算 =================
def calculate_accuracy_workflow(df: pd.DataFrame, column_mapping: dict) -> tuple:
    """
    根据映射字典重命名列，并计算准确率

    Returns:
        tuple: (annotator_stats, qa_stats, poc_stats)
    """
    # 1. 重命名列
    df = df.rename(columns=column_mapping)

    # 补齐缺失的标准列
    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    df = sanitize_standard_columns(df)

    # 2. 统计初标人表现
    annotator_stats = pd.DataFrame()
    if '初标人' in df.columns:
        # 统计初标总产量
        df_annotator_total = df.dropna(subset=['初标人'])
        annotator_volume = df_annotator_total.groupby('初标人').size().reset_index(name='初标总产量')

        # 统计被质检的数据
        df_annotator_qa = df.dropna(subset=['初标人', '质检结果'])
        annotator_qa_stats = pd.DataFrame()
        if not df_annotator_qa.empty:
            def qa_stats(x):
                qa_results = x['质检结果'].astype(str).str.lower().str.strip()
                pass_count = sum(1 for r in qa_results if any(p.lower() in r for p in PASS_LABELS))
                return pd.Series({
                    '被质检数': len(x),
                    '质检通过数': pass_count
                })

            annotator_qa_stats = df_annotator_qa.groupby('初标人').apply(
                qa_stats, include_groups=False
            ).reset_index()

        # 合并
        if not annotator_volume.empty:
            if not annotator_qa_stats.empty:
                annotator_stats = pd.merge(annotator_volume, annotator_qa_stats, on='初标人', how='left')
                annotator_stats = annotator_stats.fillna({'被质检数': 0, '质检通过数': 0})
            else:
                annotator_stats = annotator_volume.copy()
                annotator_stats['被质检数'] = 0
                annotator_stats['质检通过数'] = 0

            # 计算准确率
            annotator_stats['初标准确率'] = annotator_stats.apply(
                lambda row: f"{(row['质检通过数'] / row['被质检数']):.2%}" if row['被质检数'] > 0 else "无质检数据",
                axis=1
            )

    # 3. 统计质检人表现
    qa_stats = pd.DataFrame()
    if '质检人' in df.columns:
        df_qa_volume = df.dropna(subset=['质检人'])
        qa_volume = df_qa_volume.groupby('质检人').size().reset_index(name='质检总产量')

        df_qa_accuracy = df.dropna(subset=['质检人', '抽检结果'])
        qa_acc_stats = pd.DataFrame()
        if not df_qa_accuracy.empty:
            def poc_stats(x):
                poc_results = x['抽检结果'].astype(str).str.lower().str.strip()
                pass_count = sum(1 for r in poc_results if any(p.lower() in r for p in PASS_LABELS))
                return pd.Series({
                    '被抽检数': len(x),
                    '抽检通过数': pass_count
                })

            qa_acc_stats = df_qa_accuracy.groupby('质检人').apply(
                poc_stats, include_groups=False
            ).reset_index()

        if not qa_volume.empty:
            if not qa_acc_stats.empty:
                qa_stats = pd.merge(qa_volume, qa_acc_stats, on='质检人', how='left')
                qa_stats = qa_stats.fillna({'被抽检数': 0, '抽检通过数': 0})
            else:
                qa_stats = qa_volume.copy()
                qa_stats['被抽检数'] = 0
                qa_stats['抽检通过数'] = 0

            qa_stats['质检准确率'] = qa_stats.apply(
                lambda row: f"{(row['抽检通过数'] / row['被抽检数']):.2%}" if row['被抽检数'] > 0 else "无抽检数据",
                axis=1
            )

    # 4. 统计 POC 表现
    poc_stats = pd.DataFrame()
    if 'POC 姓名' in df.columns and df['POC 姓名'].notna().any():
        df_poc = df.dropna(subset=['POC 姓名', '抽检结果'])
        if not df_poc.empty:
            poc_stats = df_poc.groupby('POC 姓名').size().reset_index(name='抽检产量')

    return annotator_stats, qa_stats, poc_stats

# ================= 主流程 =================
def main():
    parser = argparse.ArgumentParser(description="飞书表格数据标注准确率统计工具")
    parser.add_argument("spreadsheet_token", nargs="?", help="飞书表格 URL 中的 token")
    parser.add_argument("--sheet", default="Sheet1", help="工作表名称，默认 Sheet1")
    parser.add_argument("--header-row", type=int, help="可选：指定表头所在行号（1-based）。不填则自动识别")
    parser.add_argument("--output", "-o", help="输出 Excel 文件路径")
    parser.add_argument("--url", help="飞书表格完整 URL（可选，替代直接提供 token）")
    parser.add_argument("--result-url", help="结果写入目标飞书表格 URL（可选，不填则写回源表）")
    parser.add_argument("--result-token", help="结果写入目标飞书表格 token（可选，不填则写回源表）")
    parser.add_argument("--name-roster-file", default=str(NAME_ROSTER_DEFAULT_PATH), help="姓名全名单文件路径（默认 name_roster.txt）")
    parser.add_argument("--sop-url", help="SOP 飞书文档 URL（docx/docs 或 wiki(docx)），用于评估最终难度系数")
    parser.add_argument("--difficulty-coef", type=float, help="手动指定最终难度系数（1.00~1.50），指定后不再自动评估")
    parser.add_argument("--result-sheet", default="产量&准确率统计", help="写回飞书的结果工作表名称或 sheet_id")
    parser.add_argument("--append-write-back", action="store_true", help="写回飞书时追加到空白行，不覆盖 A1")
    parser.add_argument("--no-write-back", action="store_true", help="仅计算/导出，不写回飞书表格")
    parser.add_argument("--auth-mode", choices=["user", "tenant"], default="user", help="飞书鉴权模式，默认 user")
    parser.add_argument("--user-access-token", help="飞书 user_access_token（不填则读 FEISHU_USER_ACCESS_TOKEN）")
    parser.add_argument("--debug-b2b", action="store_true", help="输出背靠背初标计算调试信息")
    parser.add_argument("--db-path", default="./metrics_panel.db", help="面板 SQLite 文件路径")
    parser.add_argument("--disable-panel-sync", action="store_true", help="禁用面板 SQLite 同步")
    parser.add_argument("--strict-sync", action="store_true", help="面板同步失败时阻断主流程")
    parser.add_argument("--project-display-name", help="项目展示名（用于面板）")
    parser.add_argument("--operator", help="操作人（用于审计字段）")

    args = parser.parse_args()

    # 解析 URL 或 token
    source_url = args.url or ""
    source_spreadsheet_title_from_url = None
    spreadsheet_token = args.spreadsheet_token
    if source_url and not spreadsheet_token:
        # sheets URL 可直接本地提取；wiki URL 需要拿到 token 后再解析
        m = re.search(r"sheets/([a-zA-Z0-9]+)", source_url)
        if m:
            spreadsheet_token = m.group(1)
    if source_url and args.sheet == "Sheet1":
        sheet_from_url = extract_sheet_ref_from_url(source_url)
        if sheet_from_url:
            args.sheet = sheet_from_url

    result_spreadsheet_token = args.result_token
    if args.result_url and not result_spreadsheet_token:
        m = re.search(r"sheets/([a-zA-Z0-9]+)", args.result_url)
        if m:
            result_spreadsheet_token = m.group(1)

    try:
        token = resolve_feishu_access_token(args.auth_mode, args.user_access_token)
        print(f"飞书鉴权模式：{args.auth_mode}")

        if source_url and not spreadsheet_token:
            spreadsheet_token, source_spreadsheet_title_from_url = resolve_spreadsheet_info_from_url(source_url, token)
        if not spreadsheet_token:
            print("错误：请提供飞书表格 URL 或 token")
            print("用法示例：python3 workflow_feishu.py 'Bv28sKowmhExvctX3HDl2XWCgoc'")
            print("或：python3 workflow_feishu.py --url 'https://xxx.feishu.cn/sheets/Bv28sKowmhExvctX3HDl2XWCgoc'")
            sys.exit(1)

        if args.result_url and not result_spreadsheet_token:
            result_spreadsheet_token = resolve_spreadsheet_token_from_url(args.result_url, token)
        if not result_spreadsheet_token:
            result_spreadsheet_token = spreadsheet_token

        print(f"正在读取飞书表格：{spreadsheet_token}")

        # 1. 读取飞书表格
        df = read_feishu_sheet(
            spreadsheet_token,
            args.sheet,
            token=token,
            header_row=args.header_row
        )
        print(f"读取成功，共 {len(df)} 行数据")
        if df.attrs.get("header_row"):
            print(f"表头行：第 {df.attrs.get('header_row')} 行")
        print(f"列名：{df.columns.tolist()}")

        if is_summary_like_sheet(df):
            raise Exception("当前源 sheet 更像统计结果页（非原始作业明细）。请切换到原始作业数据 sheet 再运行。")

        mapping = {}
        if detect_back_to_back_schema(df):
            print("\n--- 检测到背靠背初标结构：按专用规则计算 ---")
            annotators, qas, pocs = calculate_back_to_back_annotator_stats(df, debug=args.debug_b2b)
        else:
            # 2. 智能识别表头
            print("\n--- 开始智能表头识别 ---")
            mapping = intelligent_column_mapping(df.columns.tolist(), df=df)
            print(f"\n最终生成的列名映射字典：\n{json.dumps(mapping, indent=2, ensure_ascii=False)}")

            # 3. 计算指标
            print("\n--- 开始计算数据指标 ---")
            annotators, qas, pocs = calculate_accuracy_workflow(df, mapping)

        # 4. 评估最终难度系数
        difficulty = 1.0
        difficulty_report = ""
        if args.difficulty_coef is not None:
            if not (DIFFICULTY_MIN <= args.difficulty_coef <= DIFFICULTY_MAX):
                raise Exception(f"--difficulty-coef 超出范围，需在 {DIFFICULTY_MIN:.2f}~{DIFFICULTY_MAX:.2f} 之间")
            difficulty = round(args.difficulty_coef, 2)
            print(f"\n--- 使用手动难度系数: {difficulty:.2f} ---")
        elif args.sop_url:
            print("\n--- 开始评估最终难度系数（SOP + 随机50条样本）---")
            difficulty, difficulty_report = evaluate_difficulty_coefficient(args.sop_url, df, token)
            print(f"难度系数评估完成：{difficulty:.2f}")
        else:
            print("\n--- 未提供 SOP，默认难度系数 1.00 ---")

        # 5. 计算加权准确率
        annotators = apply_weighted_accuracy(annotators, "初标准确率", "加权初标准确率", difficulty)
        qas = apply_weighted_accuracy(qas, "质检准确率", "加权质检准确率", difficulty)

        # 5.1 人名标准化（别名/拼音 -> 全名）
        roster = load_name_roster(args.name_roster_file)
        if roster:
            alias_index = build_name_alias_index(roster)
            annotators, qas, pocs = apply_name_standardization(annotators, qas, pocs, alias_index)
            print(f"[姓名标准化] 已加载名单 {len(roster)} 人，别名索引 {len(alias_index)} 条。")
        else:
            print(f"[姓名标准化] 未找到名单文件或名单为空：{args.name_roster_file}")

        # 6. 打印结果
        print(f"\n==== 难度系数 ====\n{difficulty:.2f}")
        print(format_df_for_console(annotators, "==== 初标人统计 ===="))
        print(format_df_for_console(qas, "==== 质检人统计 ===="))
        print(format_df_for_console(pocs, "==== POC 抽检统计 ===="))

        # 7. 写回飞书
        if not args.no_write_back:
            print(
                f"\n--- 正在写回飞书工作表: {args.result_sheet} "
                f"(目标表: {result_spreadsheet_token}) ---"
            )
            sheet_name = write_stats_back_to_feishu(
                spreadsheet_token=result_spreadsheet_token,
                result_sheet_ref=args.result_sheet,
                annotators=annotators,
                qas=qas,
                pocs=pocs,
                token=token,
                difficulty=difficulty,
                sop_url=args.sop_url or "",
                difficulty_report=difficulty_report,
                append_mode=args.append_write_back
            )
            print(f"写回成功！请在飞书表格中查看工作表：{sheet_name}")

        # 8. 输出本地结果（可选）
        if args.output:
            print(f"\n--- 正在写入 Excel: {args.output} ---")
            # 准备统计数据
            stats_data = prepare_stats_data(
                annotators,
                qas,
                pocs,
                difficulty=difficulty,
                sop_url=args.sop_url or "",
                difficulty_report=difficulty_report
            )

            # 创建 Excel 文件
            with pd.ExcelWriter(args.output, engine='openpyxl') as writer:
                # 写入统计结果
                stats_df = pd.DataFrame(stats_data)
                stats_df.to_excel(writer, sheet_name='统计结果', index=False, header=False)

                # 写入初标人详细数据
                if not annotators.empty:
                    annotators.to_excel(writer, sheet_name='初标人详情', index=False)

                # 写入质检人详细数据
                if not qas.empty:
                    qas.to_excel(writer, sheet_name='质检人详情', index=False)

                # 写入 POC 详细数据
                if not pocs.empty:
                    pocs.to_excel(writer, sheet_name='POC 详情', index=False)

            print(f"写入成功！文件：{args.output}")
            print(f"提示：可将结果文件导入飞书表格，或直接在 Excel 中查看")

        # 9. 同步面板 SQLite
        if args.disable_panel_sync:
            print("[面板同步] 已禁用（--disable-panel-sync）")
        elif save_run_snapshot is None:
            sync_err = "panel_db 模块不可用，跳过同步"
            if args.strict_sync:
                raise Exception(sync_err)
            print(f"[面板同步警告] {sync_err}")
        else:
            snapshot = build_panel_snapshot(
                spreadsheet_token=spreadsheet_token,
                sheet_ref=(df.attrs.get("sheet_id") or args.sheet),
                sheet_title=df.attrs.get("sheet_title"),
                spreadsheet_title=(df.attrs.get("spreadsheet_title") or source_spreadsheet_title_from_url),
                result_spreadsheet_token=result_spreadsheet_token,
                result_sheet_ref=args.result_sheet,
                project_display_name=args.project_display_name,
                annotators=annotators,
                qas=qas,
                pocs=pocs,
                difficulty=difficulty,
                args=args,
                mapping=mapping,
            )
            try:
                run_id = save_run_snapshot(snapshot, db_path=args.db_path)
                print(f"[面板同步] 已写入数据库：{args.db_path}（run_id={run_id}）")
            except Exception as e:
                sync_err = f"写入失败：{e}"
                if args.strict_sync:
                    raise Exception(f"面板同步失败：{sync_err}") from e
                print(f"[面板同步警告] {sync_err}")

        return annotators, qas, pocs

    except Exception as e:
        print(f"处理失败：{e}")
        raise

if __name__ == "__main__":
    main()
