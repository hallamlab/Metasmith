import os
import shutil
import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def docker_available():
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not on PATH")
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=60)
    except subprocess.TimeoutExpired:
        pytest.fail("docker info did not answer within 60s")
    if result.returncode != 0:
        pytest.skip(f"Docker daemon not available: {result.stderr.strip()[:200]}")


@pytest.fixture(scope="session")
def docker_image(docker_available):
    from metasmith.testing.docker_builder import build_docker_image

    return build_docker_image()


@pytest.fixture(scope="session")
def nxf_container(docker_image, tmp_path_factory):
    # One container per worker, exec'd into for every Nextflow run: creating a container costs
    # this daemon 11-17s, an exec well under one. It mounts the worker's whole tmp tree at the
    # same path, so a test's paths mean the same thing on both sides. A killed worker never
    # tears down, so the container's own lifetime is bounded and --rm removes it.
    base = tmp_path_factory.getbasetemp()
    cid = subprocess.run(
        ["docker", "run", "-d", "--rm", "-u", f"{os.getuid()}:{os.getgid()}",
         "-v", f"{base}:{base}", "--entrypoint", "sleep", docker_image, "10800"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()
    yield cid
    subprocess.run(["docker", "kill", cid], capture_output=True)


@pytest.fixture
def local_agent_home(tmp_path):
    home = tmp_path / "agent_home"
    for subdir in ["runs", "data", "lib", "relay"]:
        (home / subdir).mkdir(parents=True)
    return home


def promote_stub_tasks(work_dir: Path, cache_root: Path) -> None:
    """What bootstrap does after the protocol, for every stub task that ran.

    A `-stub` task never calls bootstrap, so nothing writes the per-member
    `.command.cache` record that `record_run` reads -- and without records
    there are no trace events, so `CollectResults` builds an empty library.
    Promotion moved inside the task with the member cache; before that a
    host-side pass walked the workspace and this was not needed.
    """
    from metasmith.caching.promote import CACHE_RECORD_FILE, StepCacheMeta, promote_members
    from metasmith.models.lineage import LinPayload

    metas = {}
    for mp in sorted(work_dir.glob("workflow.step_*.meta")):
        raw = dict(
            l.partition(" ")[::2] for l in mp.read_text().splitlines() if l.strip()
        )
        order = int(mp.stem.rsplit("_", 1)[1])
        metas[order] = StepCacheMeta.from_raw(order, raw)
    roots = [p for p in (work_dir / "nxf_work", work_dir / "work") if p.is_dir()]
    for meta_file in sorted(p for r in roots for p in r.rglob(".command.metadata")):
        task_dir = meta_file.parent
        if (task_dir / CACHE_RECORD_FILE).exists():
            continue
        raw = dict(
            l.partition(" ")[::2] for l in meta_file.read_text().splitlines() if l.strip()
        )
        if "lin" not in raw or "transform_key" not in raw:
            continue
        order = next(
            (o for o, m in metas.items() if m.transform_key == raw["transform_key"]),
            None,
        )
        if order is None:
            continue
        entries = LinPayload.from_json(raw["lin"]).entries
        promote_members(
            cwd=task_dir, entries=entries, meta=metas[order],
            cache_root=cache_root, successes=[True] * len(entries),
        )
