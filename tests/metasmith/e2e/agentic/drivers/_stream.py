from __future__ import annotations

import json
import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass
class StreamSummary:
    tokens_in: int
    tokens_out: int
    final_text: str
    raw_events: int
    tokens_cached: int = 0
    tokens_cache_creation: int = 0


def run_streaming(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    transcript_path: Path,
    timeout_s: float | None = None,
) -> tuple[int, float]:
    transcript_path.parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    merged_env = {**os.environ, **env}
    with transcript_path.open("w") as fp:
        try:
            proc = subprocess.run(
                argv,
                cwd=str(cwd),
                env=merged_env,
                stdout=fp,
                stderr=subprocess.STDOUT,
                timeout=timeout_s,
                check=False,
            )
            exit_code = proc.returncode
        except subprocess.TimeoutExpired:
            exit_code = 124
    return exit_code, time.monotonic() - start


def iter_jsonl(path: Path) -> Iterable[dict]:
    if not path.exists():
        return
    with path.open() as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                yield obj


def parse_opencode_stream(path: Path) -> StreamSummary:
    tokens_in = 0
    tokens_out = 0
    final_text = ""
    raw = 0
    for ev in iter_jsonl(path):
        raw += 1
        if "input_tokens" in ev or "output_tokens" in ev:
            tokens_in += int(ev.get("input_tokens") or 0)
            tokens_out += int(ev.get("output_tokens") or 0)
        usage = ev.get("usage")
        if isinstance(usage, dict):
            tokens_in += int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
            tokens_out += int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
        role = ev.get("role") or (ev.get("message") or {}).get("role")
        if role == "assistant":
            content = ev.get("content") or ev.get("text")
            if isinstance(content, str) and content:
                final_text = content
            elif isinstance(content, list):
                parts = [c.get("text", "") for c in content if isinstance(c, dict)]
                joined = "".join(parts)
                if joined:
                    final_text = joined
        if ev.get("event") == "message.assistant":
            text = ev.get("text")
            if isinstance(text, str) and text:
                final_text = text
    return StreamSummary(tokens_in, tokens_out, final_text, raw)


def _claude_usage(ev: dict) -> dict | None:
    u = ev.get("usage")
    if isinstance(u, dict):
        return u
    msg = ev.get("message")
    if isinstance(msg, dict):
        u = msg.get("usage")
        if isinstance(u, dict):
            return u
    return None


def _claude_usage_counts(u: dict) -> tuple[int, int, int, int]:
    return (
        int(u.get("input_tokens") or 0),
        int(u.get("output_tokens") or 0),
        int(u.get("cache_read_input_tokens") or 0),
        int(u.get("cache_creation_input_tokens") or 0),
    )


def parse_claude_stream(path: Path) -> StreamSummary:
    final_text = ""
    raw = 0
    summed = [0, 0, 0, 0]
    result_counts: tuple[int, int, int, int] | None = None
    for ev in iter_jsonl(path):
        raw += 1
        etype = ev.get("type")
        usage = _claude_usage(ev)
        if etype == "result":
            if usage is not None:
                counts = _claude_usage_counts(usage)
                if any(counts):
                    result_counts = counts
            r = ev.get("result")
            if isinstance(r, str) and r:
                final_text = r
        else:
            if usage is not None:
                for i, v in enumerate(_claude_usage_counts(usage)):
                    summed[i] += v
            if etype == "assistant":
                msg = ev.get("message") or {}
                content = msg.get("content")
                if isinstance(content, list):
                    parts = [c.get("text", "") for c in content
                             if isinstance(c, dict) and c.get("type") == "text"]
                    joined = "".join(parts)
                    if joined:
                        final_text = joined
    tin, tout, tcache, tcreate = result_counts if result_counts is not None else tuple(summed)
    return StreamSummary(
        tokens_in=tin,
        tokens_out=tout,
        final_text=final_text,
        raw_events=raw,
        tokens_cached=tcache,
        tokens_cache_creation=tcreate,
    )
