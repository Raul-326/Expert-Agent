import unicodedata
import re
import os
import psycopg2
from pathlib import Path
from typing import Dict, List, Set, Any

# ================= 配置 =================
NAME_ROSTER_PATH = "/Users/bytedance/code/Expert Agent/name_roster.txt"
POSTGRES_URL = "postgresql://agent_user:agent_password@localhost:5432/expert_agent"

# 常见中文拼音别名 -> 全名
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

def normalize_name_key(name: Any) -> str:
    if name is None: return ""
    text = str(name).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[\s\-\.'\"`’·•]+", "", text)
    return text

def load_roster(path: str) -> List[str]:
    p = Path(path)
    if not p.exists(): return []
    return [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]

def build_alias_index(roster: List[str]) -> Dict[str, str]:
    alias_to_candidates = {}
    
    def put(alias: str, full_name: str):
        k = normalize_name_key(alias)
        if k: alias_to_candidates.setdefault(k, set()).add(full_name)

    roster_set = set(roster)
    for full in roster:
        put(full, full)
        if "_" in full:
            base = full.split("_", 1)[0].strip()
            if base: put(base, full)

    for alias, full in MANUAL_NAME_ALIAS.items():
        if full in roster_set: put(alias, full)

    index = {}
    for k, candidates in alias_to_candidates.items():
        if len(candidates) == 1:
            index[k] = next(iter(candidates))
    return index

def run_cleanup():
    print("🚀 开始人名映射与团队成员过滤（PostgreSQL 层）...")
    roster = load_roster(NAME_ROSTER_PATH)
    if not roster:
        print("❌ 未加载到名单")
        return
    
    index = build_alias_index(roster)
    roster_set = set(roster)
    
    conn = psycopg2.connect(POSTGRES_URL)
    cur = conn.cursor()

    # 1. 获取所有人员
    cur.execute("SELECT id, person_name FROM person_metrics_base")
    rows = cur.fetchall()
    
    to_delete = []
    to_update = []
    
    stats_updated = 0
    stats_deleted = 0

    for pid, name in rows:
        norm = normalize_name_key(name)
        full_name = index.get(norm)
        
        # 如果能映射到名单中的全名
        if full_name:
            if full_name != name:
                to_update.append((full_name, pid))
                stats_updated += 1
        # 如果即使映射完依然不在名单中，则标记删除
        elif name not in roster_set:
            to_delete.append((pid,))
            stats_deleted += 1

    print(f"📊 待处理：更新人员姓名 {stats_updated} 条，删除非名单人员 {stats_deleted} 条。")

    if to_update:
        cur.executemany("UPDATE person_metrics_base SET person_name=%s WHERE id=%s", to_update)
    
    if to_delete:
        cur.executemany("DELETE FROM person_metrics_base WHERE id=%s", to_delete)

    conn.commit()
    print(f"✅ 执行完成！")
    conn.close()

if __name__ == "__main__":
    run_cleanup()
