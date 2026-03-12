import unicodedata
import re
import os
import json
import httpx
from pathlib import Path
from typing import Dict, List, Set, Any, Optional

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
    "weiqi": "李蔚祺",
    "liweiqi": "李蔚祺",
    "wanqi": "曾琬棋",
    "zengwanqi": "曾琬棋",
    "hanyi": "韩毅",
    "zhangxuan": "张璇",
}

# Ark API 配置 (从环境变量或硬编码，与 workflow_feishu 保持一致)
ARK_API_KEY = os.environ.get("ARK_API_KEY", "")
ARK_BASE_URL = "https://ark-ap-southeast.byteintl.net/api/v3"
DEFAULT_MODEL = "ep-20260227030217-66fc4"

class PersonnelManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PersonnelManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        # 获取当前文件所在目录，从而定位到项目根目录的 roster 文件
        base_dir = Path(__file__).resolve().parent.parent.parent.parent
        self.roster_path = base_dir / "name_roster.txt"
        self.roster: List[str] = []
        self.roster_set: Set[str] = set()
        self.alias_index: Dict[str, str] = {}
        self.load_all()
        self._initialized = True

    def normalize_name_key(self, name: Any) -> str:
        if name is None: return ""
        text = str(name).strip().lower()
        text = unicodedata.normalize("NFKD", text)
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        # 去除非字母数字字符
        text = re.sub(r"[\s\-\.'\"`’·•_]+", "", text)
        return text

    def load_all(self):
        if self.roster_path.exists():
            lines = [ln.strip() for ln in self.roster_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
            self.roster = lines
            self.roster_set = set(lines)
            
            # 构建索引
            alias_to_candidates = {}
            def put(alias: str, full_name: str):
                k = self.normalize_name_key(alias)
                if k: alias_to_candidates.setdefault(k, set()).add(full_name)

            for full in self.roster:
                put(full, full)
                # 处理 xxx_TMX 这种情况
                if "_" in full:
                    base = full.split("_", 1)[0].strip()
                    if base: put(base, full)

            # 手工映射
            for alias, full in MANUAL_NAME_ALIAS.items():
                if full in self.roster_set:
                    put(alias, full)

            self.alias_index = {k: next(iter(v)) for k, v in alias_to_candidates.items() if len(v) == 1}

    def resolve_name(self, name: str) -> Optional[str]:
        """将名称映射为全名，如果不在名单中则返回 None (或进行过滤标记)"""
        if not name: return None
        
        # 1. 尝试直接全名匹配
        if name in self.roster_set:
            return name
        
        # 2. 尝试别名索引
        norm = self.normalize_name_key(name)
        full_name = self.alias_index.get(norm)
        if full_name:
            return full_name
            
        # 3. 语义兜底：让 Ark 帮忙识别（针对昵称、拼写变体等）
        ark_resolution = self._resolve_name_with_ark(name)
        if ark_resolution:
            return ark_resolution
            
        return None

    def _resolve_name_with_ark(self, name: str) -> Optional[str]:
        """调用 Ark 模型进行语义人名匹配"""
        if not ARK_API_KEY:
            return None
            
        # 限制名单范围以节省 Prompt 长度
        roster_str = ", ".join(self.roster)
        
        prompt = f"""
你是一个专业的人事数据助手。我有一个标准职员名单：[{roster_str}]

现在我在一份作业表格中看到了一个人名/昵称："{name}"

请判断这个名字是否指向名单中的某位成员。
规则：
1. 如果确定指向某人（即使是拼音、昵称或简称），仅输出该成员的“全名”。
2. 如果无法确定或明显不在名单内，输出 "None"。
3. 不要输出任何解释说明，不要带 Markdown 格式。

示例输出：
王乙琀
None
"""
        
        try:
            with httpx.Client(timeout=10) as client:
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
                        "Authorization": f"Bearer {ARK_API_KEY}",
                        "Content-Type": "application/json"
                    }
                )
                
                result_data = response.json()
                result_text = ""
                if "output" in result_data:
                    for item in result_data["output"]:
                        if item.get("type") == "message":
                            content_list = item.get("content", [])
                            for content_item in content_list:
                                if content_item.get("type") == "output_text":
                                    result_text += content_item.get("text", "")
                
                resolved = result_text.strip()
                if resolved in self.roster_set:
                    print(f"[Ark 语义匹配成功] {name} -> {resolved}")
                    return resolved
        except Exception as e:
            print(f"[Ark 语义匹配失败] 错误信息：{e}")
            
        return None

    def is_team_member(self, name: str) -> bool:
        return name in self.roster_set or self.normalize_name_key(name) in self.alias_index

personnel_manager = PersonnelManager()
