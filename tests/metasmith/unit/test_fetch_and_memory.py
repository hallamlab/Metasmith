"""A fetched reference is checked against its published md5, and a task's memory reaches a transform in GB."""

from __future__ import annotations

import hashlib
import subprocess

import pytest

from metasmith.bootstrap import _memory_gb
from metasmith.models.libraries import FetchCommand

PAYLOAD = b"reference bytes\n"


def _run(tmp_path, script: str) -> int:
    shim = tmp_path / "bin"
    shim.mkdir(exist_ok=True)
    (shim / "payload").write_bytes(PAYLOAD)
    wget = shim / "wget"
    wget.write_text(f'#!/bin/bash\nfor a; do [ "$prev" = -O ] && cat {shim}/payload > "$a"; prev=$a; done; true\n')
    wget.chmod(0o755)
    return subprocess.run(
        ["bash", "-euo", "pipefail", "-c", script], cwd=tmp_path,
        env={"PATH": f"{shim}:/usr/bin:/bin"},
    ).returncode


def test_a_fetch_whose_md5_matches_is_kept(tmp_path):
    md5 = hashlib.md5(PAYLOAD).hexdigest()
    assert _run(tmp_path, FetchCommand("https://x/part_aa", "part_aa", md5=md5)) == 0
    assert (tmp_path / "part_aa").read_bytes() == PAYLOAD


def test_a_fetch_whose_md5_differs_fails_and_leaves_nothing(tmp_path):
    assert _run(tmp_path, FetchCommand("https://x/part_aa", "part_aa", md5="0" * 32)) == 1
    assert not (tmp_path / "part_aa").exists()
    assert not (tmp_path / "part_aa.part").exists()


@pytest.mark.parametrize("text, gb", [
    ("180 GB", 180), ("1.5 GB", 1), ("1 TB", 1024), ("512 MB", 1), ("750 GB", 750), ("64", 64),
])
def test_task_memory_is_read_in_gb(text, gb):
    assert _memory_gb(text) == gb


def test_unreadable_task_memory_is_dropped():
    assert _memory_gb("lots") is None
