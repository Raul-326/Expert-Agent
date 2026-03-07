#!/usr/bin/env python3
"""飞书 user token 本地管理：读取、检测、自动刷新并落盘。"""

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import httpx


DEFAULT_HOST = os.environ.get("FEISHU_OPEN_HOST", "https://fsopen.bytedance.net").rstrip("/")
DEFAULT_APP_ID = os.environ.get("FEISHU_APP_ID", "")
DEFAULT_APP_SECRET = os.environ.get("FEISHU_APP_SECRET", "")
DEFAULT_TOKEN_FILE = os.environ.get("FEISHU_USER_TOKEN_FILE", "./tokens.json")


def _parse_shell_kv_line(line: str) -> Tuple[Optional[str], Optional[str]]:
    line = (line or "").strip()
    if not line or line.startswith("#") or "=" not in line:
        return None, None
    key, value = line.split("=", 1)
    key = key.strip()
    value = value.strip()
    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
        value = value[1:-1]
    return key, value


def load_tokens(path: str) -> Dict[str, Any]:
    p = Path(path).expanduser()
    if not p.exists():
        return {}
    raw = p.read_text(encoding="utf-8").strip()
    if not raw:
        return {}
    if p.suffix.lower() == ".json":
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}

    env_data: Dict[str, str] = {}
    for line in raw.splitlines():
        key, value = _parse_shell_kv_line(line)
        if key:
            env_data[key] = value or ""
    out: Dict[str, Any] = {
        "access_token": env_data.get("FEISHU_USER_ACCESS_TOKEN", ""),
        "refresh_token": env_data.get("FEISHU_REFRESH_TOKEN", ""),
    }
    if env_data.get("FEISHU_ACCESS_EXPIRE_AT"):
        try:
            out["expire_at"] = int(float(env_data["FEISHU_ACCESS_EXPIRE_AT"]))
        except Exception:
            pass
    return out


def save_tokens(path: str, token_data: Dict[str, Any]) -> None:
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)
    access_token = str(token_data.get("access_token", "") or "")
    refresh_token = str(token_data.get("refresh_token", "") or "")
    expires_in = int(token_data.get("expires_in", 0) or 0)
    updated_at = int(token_data.get("updated_at", int(time.time())) or int(time.time()))
    if expires_in > 0:
        expire_at = updated_at + expires_in - 60
    else:
        expire_at = int(token_data.get("expire_at", updated_at) or updated_at)

    if p.suffix.lower() == ".json":
        payload = {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_in": expires_in,
            "updated_at": updated_at,
            "expire_at": expire_at,
        }
        p.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return

    content = (
        f'FEISHU_USER_ACCESS_TOKEN="{access_token}"\n'
        f'FEISHU_REFRESH_TOKEN="{refresh_token}"\n'
        f'FEISHU_ACCESS_EXPIRE_AT="{expire_at}"\n'
    )
    p.write_text(content, encoding="utf-8")


def is_expiring(token_data: Dict[str, Any], threshold_sec: int = 120) -> bool:
    now_ts = int(time.time())
    expire_at = token_data.get("expire_at")
    if expire_at is not None:
        try:
            return int(expire_at) - now_ts <= threshold_sec
        except Exception:
            return True
    updated_at = token_data.get("updated_at")
    expires_in = token_data.get("expires_in")
    if updated_at is not None and expires_in is not None:
        try:
            return int(updated_at) + int(expires_in) - now_ts <= threshold_sec
        except Exception:
            return True
    return True


def refresh_tokens(host: str, app_id: str, app_secret: str, refresh_token: str) -> Dict[str, Any]:
    url = f"{host}/open-apis/authen/v1/refresh_access_token"
    payload = {
        "grant_type": "refresh_token",
        "app_id": app_id,
        "app_secret": app_secret,
        "refresh_token": refresh_token,
    }
    resp = httpx.post(url, json=payload, timeout=20.0)
    data = resp.json()
    if data.get("code") != 0:
        raise RuntimeError(f"刷新失败: code={data.get('code')} msg={data.get('msg')}")
    payload_data = data.get("data") if isinstance(data.get("data"), dict) else data
    payload_data["updated_at"] = int(time.time())
    return payload_data


def cmd_status(args: argparse.Namespace) -> int:
    data = load_tokens(args.token_file)
    if not data:
        print("EMPTY")
        return 1
    access = bool(str(data.get("access_token", "")).strip())
    refresh = bool(str(data.get("refresh_token", "")).strip())
    expiring = is_expiring(data, args.threshold_sec)
    print(json.dumps({"has_access_token": access, "has_refresh_token": refresh, "expiring": expiring}, ensure_ascii=False))
    return 0


def cmd_refresh(args: argparse.Namespace) -> int:
    data = load_tokens(args.token_file)
    refresh_token = (args.refresh_token or data.get("refresh_token") or "").strip()
    if not refresh_token:
        print("缺少 refresh_token")
        return 1
    new_data = refresh_tokens(args.host, args.app_id, args.app_secret, refresh_token)
    merged = dict(data)
    merged.update(
        {
            "access_token": new_data.get("access_token", ""),
            "refresh_token": new_data.get("refresh_token") or refresh_token,
            "expires_in": new_data.get("expires_in", 0),
            "updated_at": new_data.get("updated_at", int(time.time())),
        }
    )
    save_tokens(args.token_file, merged)
    print("OK")
    return 0


def cmd_get(args: argparse.Namespace) -> int:
    data = load_tokens(args.token_file)
    if not data:
        print("")
        return 1
    if args.auto_refresh and is_expiring(data, args.threshold_sec):
        refresh_token = str(data.get("refresh_token", "") or "").strip()
        if refresh_token:
            new_data = refresh_tokens(args.host, args.app_id, args.app_secret, refresh_token)
            data.update(
                {
                    "access_token": new_data.get("access_token", ""),
                    "refresh_token": new_data.get("refresh_token") or refresh_token,
                    "expires_in": new_data.get("expires_in", 0),
                    "updated_at": new_data.get("updated_at", int(time.time())),
                }
            )
            save_tokens(args.token_file, data)
    print(str(data.get("access_token", "") or ""))
    return 0 if str(data.get("access_token", "") or "").strip() else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="飞书 user token 管理脚本")
    p.add_argument("--host", default=DEFAULT_HOST, help="飞书开放平台域名")
    p.add_argument("--app-id", default=DEFAULT_APP_ID, help="应用 app_id")
    p.add_argument("--app-secret", default=DEFAULT_APP_SECRET, help="应用 app_secret")
    p.add_argument("--token-file", default=DEFAULT_TOKEN_FILE, help="token 文件路径（json 或 env 样式）")
    p.add_argument("--threshold-sec", type=int, default=120, help="视为即将过期的阈值秒数")

    sub = p.add_subparsers(dest="cmd", required=True)

    sp_status = sub.add_parser("status", help="检查 token 状态")
    sp_status.set_defaults(func=cmd_status)

    sp_refresh = sub.add_parser("refresh", help="强制刷新并落盘")
    sp_refresh.add_argument("--refresh-token", default="", help="可选：覆盖文件中的 refresh_token")
    sp_refresh.set_defaults(func=cmd_refresh)

    sp_get = sub.add_parser("get-access-token", help="获取 access_token（可自动刷新）")
    sp_get.add_argument("--auto-refresh", action="store_true", help="若临近过期则自动刷新")
    sp_get.set_defaults(func=cmd_get)
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    need_creds = args.cmd == "refresh" or (args.cmd == "get-access-token" and bool(getattr(args, "auto_refresh", False)))
    if need_creds and (not args.app_id or not args.app_secret):
        print("缺少 app_id 或 app_secret（可用 --app-id/--app-secret 或环境变量 FEISHU_APP_ID/FEISHU_APP_SECRET）")
        return 1
    try:
        return int(args.func(args))
    except Exception as e:
        print(str(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
