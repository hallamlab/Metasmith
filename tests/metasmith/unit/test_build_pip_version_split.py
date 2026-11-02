from __future__ import annotations

import importlib

from metasmith.testing import docker_builder


def test_build_pip_package_strips_local_segment_before_writing(tmp_path, monkeypatch):
    fake_repo = tmp_path / "repo"
    (fake_repo / "src/metasmith").mkdir(parents=True)
    version_file = fake_repo / "src/metasmith/version.txt"
    version_file.write_text("0.0.0")

    seen: dict[str, str] = {}

    def _fake_write_build_hash(root):
        seen["version_txt"] = version_file.read_text()
        (root / "build_hash.txt").write_text("deadbee")
        return "deadbee"

    class _FakeRun:
        def __init__(self):
            self.returncode = 0
            self.stderr = ""

    monkeypatch.setattr(docker_builder, "REPO_ROOT", fake_repo)
    monkeypatch.setattr(docker_builder, "write_build_hash", _fake_write_build_hash)
    monkeypatch.setattr(docker_builder.subprocess, "run", lambda *a, **kw: _FakeRun())
    monkeypatch.setattr(docker_builder.shutil, "rmtree", lambda *a, **kw: None)

    docker_builder.build_pip_package(version="0.18.2+1fb0eec")

    assert seen["version_txt"] == "0.18.2", (
        f"version.txt must contain bare semver before write_build_hash runs, "
        f"got [{seen['version_txt']}] — setup.py would compose this with "
        f"the build hash and produce a double-`+` PEP 440 InvalidVersion."
    )


def test_build_pip_package_bare_semver_passes_through(tmp_path, monkeypatch):
    fake_repo = tmp_path / "repo"
    (fake_repo / "src/metasmith").mkdir(parents=True)
    version_file = fake_repo / "src/metasmith/version.txt"
    version_file.write_text("0.0.0")

    seen: dict[str, str] = {}

    def _fake_write_build_hash(root):
        seen["version_txt"] = version_file.read_text()
        (root / "build_hash.txt").write_text("deadbee")
        return "deadbee"

    class _FakeRun:
        def __init__(self):
            self.returncode = 0
            self.stderr = ""

    monkeypatch.setattr(docker_builder, "REPO_ROOT", fake_repo)
    monkeypatch.setattr(docker_builder, "write_build_hash", _fake_write_build_hash)
    monkeypatch.setattr(docker_builder.subprocess, "run", lambda *a, **kw: _FakeRun())
    monkeypatch.setattr(docker_builder.shutil, "rmtree", lambda *a, **kw: None)

    docker_builder.build_pip_package(version="0.19.0")
    assert seen["version_txt"] == "0.19.0"
