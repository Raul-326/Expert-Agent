import pandas as pd
from typing import List, Dict, Any, Optional
import sys
import os
import json
import re

# 将项目根目录加入 sys.path, 以便导入 workflow_feishu
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workflow_feishu import (
    resolve_feishu_access_token,
    resolve_spreadsheet_token_from_url,
    extract_sheet_ref_from_url,
    read_feishu_sheet,
    parse_feishu_url,
    read_sop_content,
    call_modelark_text
)

class DataFetcherAgent:
    def __init__(self, feishu_url: str, user_access_token: str):
        self.feishu_url = feishu_url
        self.user_access_token = user_access_token

    def fetch(self, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        接收飞书 URL 和 user_access_token。
        1. 自动判定 URL 类型：如果是 docx/wiki 文档，则通过 Ark 寻找其中的表格数据源。
        2. 如果是表格链接，直接读取。
        """
        # 0. 鉴权
        token = resolve_feishu_access_token(
            auth_mode="user",
            user_access_token=self.user_access_token
        )

        doc_type, _ = parse_feishu_url(self.feishu_url)
        target_url = self.feishu_url
        target_sheet = sheet_name

        # 1. 如果输入是文档或 Wiki，启动 Ark 语义寻踪
        if doc_type in ["docx", "wiki"] and doc_type != "sheet":
            print(f"[DataFetcher] 检测到文档链接({doc_type})，正在启动 Ark 识别数据源...")
            title, content = read_sop_content(self.feishu_url, token)
            
            # 让 Ark 找出文档中提到的作业表格 URL 和 Sheet 名
            found_source = self._find_data_source_with_ark(title, content)
            if found_source:
                target_url = found_source.get("url", target_url)
                target_sheet = target_sheet or found_source.get("sheet")
                print(f"[Ark 识别成功] 发现数据源 URL: {target_url}, Sheet: {target_sheet}")

        # 2. 解析最终的表格 Token
        spreadsheet_token = resolve_spreadsheet_token_from_url(target_url, token)
        if not target_sheet:
            target_sheet = extract_sheet_ref_from_url(target_url)

        # 3. 通过 read_feishu_sheet 读取表格
        df = read_feishu_sheet(
            spreadsheet_token=spreadsheet_token,
            sheet_name=target_sheet,
            token=token
        )

        # 4. 填充空值，转换为行数据字典列表返回
        df = df.fillna("")
        return df.to_dict(orient="records")

    def _find_data_source_with_ark(self, title: str, content: str) -> Optional[dict]:
        """调用 Ark 模型分析文档内容，提取表格数据源信息"""
        prompt = f"""
你是一个专业的数据寻踪助手。我有一份飞书文档的内容如下：
标题：{title}
正文：
{content[:5000]}  # 截断长文档以防止 Token 溢出

我的目标是找到这份文档中提到的“作业结果表格”或“标注明细表”的链接。
请根据文档上下文：
1. 找出最像作业结果数据的飞书表格(Spreadsheet)链接。
2. 找出提到的工作表(Sheet)名称（如果有）。

输出要求：
1. 仅输出纯 JSON 格式。
2. 字段包括："url" (字符串), "sheet" (字符串，没有则为 null)。
3. 如果文档中包含多个链接，请选择那个最像包含“标注”、“初标”、“核验”数据的表格。

示例输出：
{{"url": "https://feishu.cn/sheets/...", "sheet": "Sheet1"}}
"""
        try:
            res_text = call_modelark_text(prompt)
            # 简易清理 markdown 标记
            res_text = re.sub(r"```json\s*|\s*```", "", res_text).strip()
            result = json.loads(res_text)
            if result.get("url"):
                return result
        except Exception as e:
            print(f"[DataFetcher Ark 识别异常] {e}")
        return None
