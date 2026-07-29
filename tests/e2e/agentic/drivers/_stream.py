"""Shared subprocess + JSON-event helpers used by both drivers.

Both opencode (``--format json``) and claude (``--output-format=stream-json``)
emit newline-delimited JSON event streams on stdout. The exact field
names differ but the shapes both let us extract:

    * cumulative token usage (in / out)
    * the final assistant text

This module keeps the parsers tolerant: unknown event shapes are logged
and ignored rather than crashing the run.
"""
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
    # Cache-related counts. Populated by parse_claude_stream; the opencode
    # parser leaves them at 0 (opencode's json stream does not expose a
    # cache-read / cache-creation split in a comparable shape).
    tokens_cached: int = 0            # usage.cache_read_input_tokens
    tokens_cache_creation: int = 0    # usage.cache_creation_input_tokens


def run_streaming(
    argv: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    transcript_path: Path,
    timeout_s: float | None = None,
) -> tuple[int, float]:
    """Run a process, tee stdout (line-by-line) to transcript_path.

    Returns ``(exit_code, duration_s)``. Caller parses the transcript
    afterwards — keeping I/O and parsing separate makes the parser
    independently testable.
    """
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
    """Yield parsed JSON objects from a newline-delimited file.

    Skips blank lines and unparseable lines silently — neither driver
    promises a strict JSONL stream when an error message bypasses the
    formatter.
    """
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
    """Parse an opencode ``--format json`` event log.

    The schema is documented at https://deepwiki.com/sst/opencode CLI
    page. We look for events of these shapes (others are ignored):

        {"type": "usage", "input_tokens": N, "output_tokens": M, ...}
        {"type": "message", "role": "assistant", "content": "..."}
        {"event": "message.assistant", "text": "..."}         # newer
    """
    tokens_in = 0
    tokens_out = 0
    final_text = ""
    raw = 0
    for ev in iter_jsonl(path):
        raw += 1
        # accept multiple plausible field shapes — defensive against version drift
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
    """Pull the usage object off a claude event.

    Real Claude Code stream-json nests usage under ``message`` on
    per-assistant-message events (``{"type":"assistant","message":{...,"usage":{...}}}``)
    but places it at the top level on the final ``result`` event. Accept
    either shape (top-level first, so a synthetic top-level usage on an
    assistant event still parses).
    """
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
    """(input, output, cache_read, cache_creation) from a usage object."""
    return (
        int(u.get("input_tokens") or 0),
        int(u.get("output_tokens") or 0),
        int(u.get("cache_read_input_tokens") or 0),
        int(u.get("cache_creation_input_tokens") or 0),
    )


def parse_claude_stream(path: Path) -> StreamSummary:
    """Parse a claude ``--output-format=stream-json`` event log.

    Field shapes observed in practice:

        {"type": "assistant", "message": {"content": [...], "usage": {...}}}
        {"type": "result", "usage": {"input_tokens": N, "output_tokens": M,
                                     "cache_read_input_tokens": R,
                                     "cache_creation_input_tokens": C}}

    ACCOUNTING DECISION (single authoritative source, no double-count):
    Claude Code emits ``usage`` on every per-assistant-message event AND a
    cumulative ``usage`` on the terminal ``type=="result"`` event. Those
    two overlap — summing all of them double-counts (badly so with prompt
    caching, where each turn re-reports the growing cache_read footprint).

    So the ``result`` event's usage is AUTHORITATIVE whenever it reports a
    non-zero total: it already aggregates the whole invocation. Only when
    there is no result usage (older CLI builds, a truncated stream) do we
    fall back to summing the per-message usage events. This keeps the loop
    budget aligned with the true billable footprint for the iteration.
    """
    final_text = ""
    raw = 0
    summed = [0, 0, 0, 0]          # per-assistant-message fallback
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
