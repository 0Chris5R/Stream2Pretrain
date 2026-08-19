"""Canonical serialization and identity helpers."""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel


def canonical_json(value: Any) -> bytes:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=False)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=_json_default,
    ).encode("utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", exclude_none=False)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value)
    raise TypeError(f"cannot canonically serialize {type(value).__name__}")


def sha256(value: bytes | str | Any) -> str:
    if isinstance(value, str):
        payload = value.encode("utf-8")
    elif isinstance(value, bytes):
        payload = value
    else:
        payload = canonical_json(value)
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def stable_id(namespace: str, *parts: str) -> str:
    payload = "\0".join(parts)
    return f"{namespace}:{uuid.uuid5(uuid.NAMESPACE_URL, payload)}"


def normalize_identifier(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9._:-]+", "-", value).strip("-")
    return normalized[:160] or "unknown"


def model_family(model: str) -> str:
    lowered = model.lower()
    families = {
        "qwen": "qwen",
        "gpt-oss": "gpt-oss",
        "nemotron": "nemotron",
        "llama": "llama",
        "mistral": "mistral",
        "deepseek": "deepseek",
    }
    return next((family for marker, family in families.items() if marker in lowered), lowered)


__all__ = ["canonical_json", "model_family", "normalize_identifier", "sha256", "stable_id"]
