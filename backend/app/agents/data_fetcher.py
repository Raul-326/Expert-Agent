import pandas as pd
from typing import List, Dict, Any, Optional
import sys
import os

# 将项目根目录加入 sys.path, 以便导入 workflow_feishu
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from workflow_feishu import (
    resolve_feishu_access_token,
    resolve_spreadsheet_token_from_url,
    extract_sheet_ref_from_url,
    read_feishu_sheet
)

class DataFetcherAgent:
    def __init__(self, feishu_url: str, user_access_token: str):
        self.feishu_url = feishu_url
        self.user_access_token = user_access_token

    def fetch(self, sheet_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        接收飞书 URL 和 user_access_token，运用现有 workflow_feishu.py 里的鉴权和 read_spreadsheet 逻辑，
        清洗并返回标准的行数据块（List of Dicts）。
        """
        # 1. 鉴权
        token = resolve_feishu_access_token(
            auth_mode="user",
            user_access_token=self.user_access_token
        )

        # 2. 解析表格 Token 和 sheet_name
        spreadsheet_token = resolve_spreadsheet_token_from_url(self.feishu_url, token)
        if not sheet_name:
            sheet_name = extract_sheet_ref_from_url(self.feishu_url)

        # 3. 通过 read_feishu_sheet 读取表格
        df = read_feishu_sheet(
            spreadsheet_token=spreadsheet_token,
            sheet_name=sheet_name,
            token=token
        )

        # 4. 填充空值，转换为行数据字典列表返回
        df = df.fillna("")
        return df.to_dict(orient="records")
