"""Unit tests for driver argv assembly + event-stream parsing.

These tests do NOT spawn `claude` or `opencode`; they exercise the
parsing logic against canned event logs and the factory routing. Real
subprocess invocation is exercised by the live smoke scenario.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.e2e_agentic.drivers._stream import (
    iter_jsonl,
    parse_claude_stream,
    parse_opencode_stream,
)
from tests.e2e_agentic.drivers.factory import make_driver
from tests.e2e_agentic.drivers.claude import ClaudeDriver
from tests.e2e_agentic.drivers.opencode import OpencodeDriver


def _write_jsonl(path: Path, events: list[dict]) -> Path:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return path


def test_iter_jsonl_skips_blank_and_garbage(tmp_path: Path) -> None:
    p = tmp_path / "stream.jsonl"
    p.write_text(
        json.dumps({"a": 1}) + "\n"
        "\n"
        "this is not json\n"
        + json.dumps({"b": 2}) + "\n"
        + json.dumps([1, 2, 3]) + "\n"   # array, not dict -> filtered
    )
    out = list(iter_jsonl(p))
    assert out == [{"a": 1}, {"b": 2}]


def test_iter_jsonl_missing_file(tmp_path: Path) -> None:
    assert list(iter_jsonl(tmp_path / "absent.jsonl")) == []


# ---------------------------------------------------------------------------
# opencode parser
# ---------------------------------------------------------------------------


def test_parse_opencode_usage_and_final_text(tmp_path: Path) -> None:
    p = _write_jsonl(tmp_path / "opencode.jsonl", [
        {"type": "session.start"},
        {"type": "message", "role": "assistant", "content": "hello"},
        {"type": "usage", "input_tokens": 120, "output_tokens": 40},
        {"type": "message", "role": "assistant", "content": "all done"},
        {"type": "usage", "input_tokens": 30, "output_tokens": 10},
    ])
    s = parse_opencode_stream(p)
    assert s.tokens_in == 150
    assert s.tokens_out == 50
    assert s.final_text == "all done"
    assert s.raw_events == 5


def test_parse_opencode_nested_usage_and_content_list(tmp_path: Path) -> None:
    p = _write_jsonl(tmp_path / "opencode.jsonl", [
        {"role": "assistant", "content": [{"type": "text", "text": "part-a "},
                                          {"type": "text", "text": "part-b"}]},
        {"usage": {"input_tokens": 7, "output_tokens": 3}},
        {"event": "message.assistant", "text": "final"},
    ])
    s = parse_opencode_stream(p)
    assert s.final_text == "final"
    assert s.tokens_in == 7 and s.tokens_out == 3


def test_parse_opencode_ignores_unknown_events(tmp_path: Path) -> None:
    p = _write_jsonl(tmp_path / "opencode.jsonl", [
        {"type": "weird_event", "payload": [1, 2, 3]},
        {"type": "tool_call", "name": "Bash", "args": {}},
    ])
    s = parse_opencode_stream(p)
    assert s.tokens_in == 0 and s.tokens_out == 0
    assert s.final_text == ""


# ---------------------------------------------------------------------------
# claude parser
# ---------------------------------------------------------------------------


def test_parse_claude_stream_typical(tmp_path: Path) -> None:
    p = _write_jsonl(tmp_path / "claude.jsonl", [
        {"type": "system", "subtype": "init"},
        {"type": "assistant",
         "message": {"role": "assistant",
                     "content": [{"type": "text", "text": "Working on it..."}]},
         "usage": {"input_tokens": 80, "output_tokens": 20}},
        {"type": "assistant",
         "message": {"role": "assistant",
                     "content": [{"type": "text", "text": "Done!"}]},
         "usage": {"input_tokens": 30, "output_tokens": 5}},
        {"type": "result", "usage": {"input_tokens": 0, "output_tokens": 0}},
    ])
    s = parse_claude_stream(p)
    assert s.final_text == "Done!"
    assert s.tokens_in == 110
    assert s.tokens_out == 25


def test_parse_claude_result_text_fallback(tmp_path: Path) -> None:
    p = _write_jsonl(tmp_path / "claude.jsonl", [
        {"type": "result", "result": "the-final-line",
         "usage": {"input_tokens": 1, "output_tokens": 2}},
    ])
    s = parse_claude_stream(p)
    assert s.final_text == "the-final-line"


# ---------------------------------------------------------------------------
# factory
# ---------------------------------------------------------------------------


def test_factory_defaults_to_deepseek_for_opencode() -> None:
    d = make_driver("opencode")
    assert isinstance(d, OpencodeDriver)
    assert d.model == "openrouter/deepseek/deepseek-v4-flash"


def test_factory_defaults_to_haiku_for_claude() -> None:
    d = make_driver("claude")
    assert isinstance(d, ClaudeDriver)
    assert d.model == "haiku"


def test_factory_honors_explicit_model() -> None:
    d = make_driver("claude", model="sonnet")
    assert d.model == "sonnet"


def test_factory_rejects_unknown_driver() -> None:
    with pytest.raises(ValueError, match="unknown agent driver"):
        make_driver("aider")
