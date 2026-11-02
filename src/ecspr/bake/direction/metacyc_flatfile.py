from __future__ import annotations

from pathlib import Path
from typing import Iterator

ENCODING = "latin-1"


def iter_records(path: Path) -> Iterator[dict]:
    rec: dict[str, list] = {}
    last_key: str | None = None

    def flush():
        nonlocal rec, last_key
        if rec:
            yield_rec, rec, last_key = rec, {}, None
            return yield_rec
        rec, last_key = {}, None
        return None

    with open(path, encoding=ENCODING) as fh:
        for raw in fh:
            line = raw.rstrip("\n").rstrip("\r")
            if not line or line.startswith("#"):
                continue
            if line == "//":
                out = flush()
                if out is not None:
                    yield out
                continue
            if line.startswith("^"):
                continue
            if line.startswith("/"):
                if last_key and rec.get(last_key):
                    rec[last_key][-1] += " " + line[1:].strip()
                continue
            key, sep, val = line.partition(" - ")
            if not sep:
                continue
            key = key.strip()
            rec.setdefault(key, []).append(val.strip())
            last_key = key
        out = flush()
        if out is not None:
            yield out


def load_reactions(path: Path) -> list[dict]:
    out = []
    for rec in iter_records(path):
        ids = rec.get("UNIQUE-ID")
        if not ids:
            continue
        d = rec.get("REACTION-DIRECTION") or []
        if len(d) > 1:
            raise ValueError(
                f"{ids[0]}: {len(d)} REACTION-DIRECTION values {d} -- MetaCyc writes at "
                f"most one, and choosing between them is not this reader's call")
        out.append(dict(
            unique_id=ids[0],
            direction=(d[0] if d else None),
            left=list(rec.get("LEFT") or []),
            right=list(rec.get("RIGHT") or []),
        ))
    return out
