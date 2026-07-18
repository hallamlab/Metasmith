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


def test_parse_claude_stream_result_is_authoritative_for_cache(tmp_path: Path) -> None:
    """Representative real-shaped stream: usage nested under ``message`` on
    each assistant event, and a cumulative usage on the terminal ``result``
    event. The result event is authoritative — the four counts must equal
    the result's usage, NOT the sum of result + per-message usage (which
    would double-count, badly so for cache_read).
    """
    p = _write_jsonl(tmp_path / "claude.jsonl", [
        {"type": "system", "subtype": "init"},
        {"type": "assistant",
         "message": {"role": "assistant",
                     "content": [{"type": "text", "text": "first"}],
                     "usage": {"input_tokens": 100, "output_tokens": 20,
                               "cache_read_input_tokens": 5_000,
                               "cache_creation_input_tokens": 800}}},
        {"type": "assistant",
         "message": {"role": "assistant",
                     "content": [{"type": "text", "text": "done"}],
                     "usage": {"input_tokens": 10, "output_tokens": 40,
                               "cache_read_input_tokens": 6_000,
                               "cache_creation_input_tokens": 0}}},
        {"type": "result", "result": "done",
         "usage": {"input_tokens": 110, "output_tokens": 60,
                   "cache_read_input_tokens": 11_000,
                   "cache_creation_input_tokens": 800}},
    ])
    s = parse_claude_stream(p)
    assert s.final_text == "done"
    # authoritative == the result event's usage, not the per-message sum
    assert s.tokens_in == 110
    assert s.tokens_out == 60
    assert s.tokens_cached == 11_000
    assert s.tokens_cache_creation == 800


def test_parse_claude_stream_falls_back_to_message_sum(tmp_path: Path) -> None:
    """When the result event carries no usage (older CLI / truncated stream),
    accounting falls back to summing the per-assistant-message usage — still
    four-way, still each event counted once."""
    p = _write_jsonl(tmp_path / "claude.jsonl", [
        {"type": "assistant",
         "message": {"content": [{"type": "text", "text": "a"}],
                     "usage": {"input_tokens": 100, "output_tokens": 20,
                               "cache_read_input_tokens": 5_000,
                               "cache_creation_input_tokens": 800}}},
        {"type": "assistant",
         "message": {"content": [{"type": "text", "text": "b"}],
                     "usage": {"input_tokens": 10, "output_tokens": 40,
                               "cache_read_input_tokens": 6_000,
                               "cache_creation_input_tokens": 0}}},
        {"type": "result", "result": "b"},  # no usage
    ])
    s = parse_claude_stream(p)
    assert s.tokens_in == 110
    assert s.tokens_out == 60
    assert s.tokens_cached == 11_000
    assert s.tokens_cache_creation == 800


def test_iterresult_tokens_total_includes_cache() -> None:
    """The loop stop-condition total is the full billable footprint:
    input + output + cache_read + cache_creation."""
    from tests.e2e_agentic.drivers.base import IterResult

    r = IterResult(
        exit_code=0, tokens_in=100, tokens_out=50,
        tokens_cached=2_000, tokens_cache_creation=300,
        final_text="", transcript_path=None, duration_s=0.0,
    )
    assert r.tokens_total == 2_450


def test_budget_record_tracks_split_and_charges_total() -> None:
    """TokenBudget.record folds the four-way split for reporting and charges
    the billable total against the limit (stop semantics unchanged)."""
    from tests.e2e_agentic.drivers.base import IterResult
    from tests.e2e_agentic.harness.budget import TokenBudget

    b = TokenBudget(limit=10_000)
    b.record(IterResult(
        exit_code=0, tokens_in=100, tokens_out=50,
        tokens_cached=2_000, tokens_cache_creation=300,
        final_text="", transcript_path=None, duration_s=0.0,
    ))
    assert b.tokens_in == 100
    assert b.tokens_out == 50
    assert b.tokens_cached == 2_000
    assert b.tokens_cache_creation == 300
    assert b.used == 2_450  # billable total drives the stop condition


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
