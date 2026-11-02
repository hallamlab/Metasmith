"""The dev overlay lands as a tree and a tarball cut from one archive."""

from __future__ import annotations

import tarfile
from pathlib import Path

from metasmith.agents import Agent
from metasmith.env import Runtime
from metasmith.models.remote import Source


def _package(root: Path, version: str) -> Path:
    pkg = root / "metasmith"
    (pkg / "models").mkdir(parents=True)
    (pkg / "__pycache__").mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "version.txt").write_text(version)
    (pkg / "models" / "a.py").write_text("x = 1\n")
    (pkg / "__pycache__" / "a.cpython-312.pyc").write_bytes(b"\0")
    return pkg


def _tree(path: Path) -> set[str]:
    return {str(p.relative_to(path)) for p in path.rglob("*") if p.is_file()}


def test_the_tree_and_its_tarball_hold_the_same_files(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    agent = Agent(home=Source.FromLocal(home), runtime=Runtime.APPTAINER, container="docker://x/y:1")
    report = agent.DeployDevOverlay(_package(tmp_path / "src1", "1"))

    tree = home / "dev" / "metasmith"
    with tarfile.open(home / "dev" / "metasmith.tar") as tar:
        archived = {str(Path(m.name).relative_to("metasmith")) for m in tar.getmembers() if m.isfile()}
    assert _tree(tree) == archived == {"__init__.py", "version.txt", "models/a.py"}
    assert report["files"] == 3 and report["stage_key"]


def test_a_second_overlay_replaces_the_first(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    agent = Agent(home=Source.FromLocal(home), runtime=Runtime.APPTAINER, container="docker://x/y:1")
    agent.DeployDevOverlay(_package(tmp_path / "src1", "1"))
    agent.DeployDevOverlay(_package(tmp_path / "src2", "2"))
    assert (home / "dev" / "metasmith" / "version.txt").read_text() == "2"
    assert not (home / "dev" / "metasmith.next").exists()
    assert not (home / "dev" / "metasmith.tar.part").exists()


def test_a_hardlinked_source_file_counts_once_per_name(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    pkg = _package(tmp_path / "src1", "1")
    (pkg / "models" / "b.py").hardlink_to(pkg / "models" / "a.py")
    agent = Agent(home=Source.FromLocal(home), runtime=Runtime.APPTAINER, container="docker://x/y:1")
    report = agent.DeployDevOverlay(pkg)
    assert report["files"] == 4
    assert "models/b.py" in _tree(home / "dev" / "metasmith")


def test_node_modules_stays_out_of_the_overlay(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    pkg = _package(tmp_path / "src1", "1")
    (pkg / "frontend" / "node_modules" / "x").mkdir(parents=True)
    (pkg / "frontend" / "node_modules" / "x" / "index.js").write_text("")
    agent = Agent(home=Source.FromLocal(home), runtime=Runtime.APPTAINER, container="docker://x/y:1")
    agent.DeployDevOverlay(pkg)
    assert not (home / "dev" / "metasmith" / "frontend" / "node_modules").exists()
