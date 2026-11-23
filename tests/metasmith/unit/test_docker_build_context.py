import subprocess

from metasmith._build_hash import compute_build_hash
from metasmith.testing import docker_builder


def _tree_status():
    return subprocess.run(
        ["git", "status", "--porcelain", "--ignored", "src"],
        cwd=docker_builder.REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout


def test_the_staged_context_hashes_like_the_checkout_and_leaves_it_untouched(tmp_path, monkeypatch):
    lib = tmp_path / "lib"
    lib.mkdir()
    (lib / "tini").write_text("")
    monkeypatch.setattr(docker_builder, "LIB", lib)
    before = _tree_status()

    docker_builder.stage_build_context(tmp_path / "stage")

    assert compute_build_hash(tmp_path / "stage/src/metasmith") == compute_build_hash(docker_builder.PACKAGE)
    for target in docker_builder.RELAY_TARGETS:
        assert docker_builder._relay_path(tmp_path / "stage", target).exists()
    assert _tree_status() == before
