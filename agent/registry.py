from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass
class SkillDef:
    name: str
    version: str
    handler: Callable[..., Dict[str, Any]]


_REGISTRY: Dict[str, SkillDef] = {}


def register_skill(name: str, version: str, handler: Callable[..., Dict[str, Any]]) -> None:
    _REGISTRY[name] = SkillDef(name=name, version=version, handler=handler)


def get_skill(name: str) -> SkillDef:
    if name not in _REGISTRY:
        raise KeyError(f"skill 未注册: {name}")
    return _REGISTRY[name]


def list_skills() -> Dict[str, SkillDef]:
    return dict(_REGISTRY)
