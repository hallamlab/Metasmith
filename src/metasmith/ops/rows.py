from __future__ import annotations

import json


def scalar(v):
    if not isinstance(v, str): return v
    s = v.strip()
    if not s: return v
    try:
        parsed = json.loads(s)
    except ValueError:
        return v
    if isinstance(parsed, (dict, list)): return v
    return parsed


def column_of(field: dict) -> str:
    return str(field.get("column") or "").strip()


def entries(row: dict) -> list[dict]:
    raw = row.get("values")
    if not isinstance(raw, list):
        return [{"key": "", "value": row.get("value") or "", "column": ""}]
    out: list[dict] = []
    for e in raw:
        if not isinstance(e, dict):
            continue
        out.append({
            "key": str(e.get("key") or "").strip(),
            "value": e.get("value") or "",
            "column": column_of(e),
        })
    return out


def render_value(ents: list[dict]) -> str:
    if not ents:
        return ""
    if len(ents) == 1 and not ents[0]["key"]:
        return ents[0]["value"]
    return json.dumps({e["key"]: scalar(e["value"]) for e in ents})
