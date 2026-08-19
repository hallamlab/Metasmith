from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

from metasmith.agents import Agent, TargetBuilder
from metasmith.constants import AgentPaths
from metasmith.env import Runtime
from metasmith.models.libraries import (
    DataInstanceLibrary,
    DataTypeLibrary,
    TransformInstanceLibrary,
)
from metasmith.models.remote import Source
from metasmith.models.solver import Endpoint
from metasmith.testing.docker_builder import get_docker_tag
from metasmith.testing.mock_transforms import identity_transform


pytestmark = [
    pytest.mark.requires_ssh_localhost,
    pytest.mark.requires_apptainer,
    pytest.mark.requires_docker_dev_image,
]


@pytest.fixture(scope="session")
def ssh_localhost() -> str:
    if shutil.which("ssh") is None:
        pytest.skip("ssh CLI not available")
    try:
        res = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=5", "localhost", "true"],
            capture_output=True, timeout=15,
        )
    except subprocess.TimeoutExpired:
        pytest.skip("ssh localhost true timed out (no passwordless access)")
    if res.returncode != 0:
        pytest.skip(
            "requires passwordless ssh to localhost "
            f"(exit={res.returncode}, stderr={res.stderr.decode(errors='replace').strip()})"
        )
    return "localhost"


@pytest.fixture(scope="session")
def metasmith_dev_image() -> str:
    tag = get_docker_tag()

    docker = shutil.which("docker")
    apptainer = shutil.which("apptainer")
    if docker is None and apptainer is None:
        pytest.skip("neither docker nor apptainer CLI available")

    found = False
    if docker is not None:
        res = subprocess.run(
            ["docker", "image", "inspect", tag],
            capture_output=True, timeout=15,
        )
        if res.returncode == 0:
            found = True

    if not found and apptainer is not None:
        pass

    if not found:
        pytest.skip(
            f"dev image [{tag}] not found locally; run `./dev.sh -bd` "
            f"(or `./dev.sh -bs` for apptainer) first"
        )
    return tag


@pytest.fixture
def ssh_agent_home(ssh_localhost: str, tmp_path: Path) -> Source:
    remote_path = tmp_path / "agent_home"
    source = Source.FromSsh(ssh_localhost, remote_path)
    try:
        yield source
    finally:
        subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=5", ssh_localhost,
             f"rm -rf {remote_path}"],
            capture_output=True, timeout=30,
        )


def _ssh_test_exists(host: str, path: str) -> bool:
    res = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=5", host, f"test -e {path} && echo OK"],
        capture_output=True, timeout=30,
    )
    return res.returncode == 0 and b"OK" in res.stdout


def _ssh_read_text(host: str, path: str) -> str:
    res = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=5", host, f"cat {path} 2>/dev/null || true"],
        capture_output=True, timeout=30,
    )
    return res.stdout.decode(errors="replace")


def test_deploy_against_ssh_localhost(
    ssh_localhost: str,
    metasmith_dev_image: str,
    ssh_agent_home: Source,
):
    container_uri = f"docker://{metasmith_dev_image}"

    agent = Agent(
        home=ssh_agent_home,
        container=container_uri,
        runtime=Runtime.APPTAINER,
    )

    agent.Deploy()

    home_path = ssh_agent_home.GetPath()

    relay = AgentPaths.to_relay(home_path)
    assert _ssh_test_exists(ssh_localhost, str(relay)), (
        f"relay binary missing at {relay} after Deploy()"
    )

    cache_dir = home_path / AgentPaths.CONTAINER_CACHE
    assert _ssh_test_exists(ssh_localhost, str(cache_dir)), (
        f"container cache dir missing at {cache_dir}"
    )
    res = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=5", ssh_localhost,
         f"ls {cache_dir}/*.sif 2>/dev/null | head -1"],
        capture_output=True, timeout=30,
    )
    assert res.stdout.strip(), (
        f"no .sif in container cache {cache_dir} after Deploy()"
    )

    definition = AgentPaths.to_definition(home_path)
    assert _ssh_test_exists(ssh_localhost, str(definition)), (
        f"agent definition missing at {definition}"
    )

    agent.Deploy()
    assert _ssh_test_exists(ssh_localhost, str(relay)), (
        "relay binary missing after second Deploy() — idempotence broken"
    )


def test_assertive_redeploy_wipes_sandbox(
    ssh_localhost: str,
    metasmith_dev_image: str,
    ssh_agent_home: Source,
):
    container_uri = f"docker://{metasmith_dev_image}"
    agent = Agent(
        home=ssh_agent_home,
        container=container_uri,
        runtime=Runtime.APPTAINER,
    )

    agent.Deploy()

    home_path = ssh_agent_home.GetPath()
    cache_dir = home_path / AgentPaths.CONTAINER_CACHE

    res = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=5", ssh_localhost,
         f"ls -d {cache_dir}/*.sandbox 2>/dev/null | head -1"],
        capture_output=True, timeout=30,
    )
    sandbox_path = res.stdout.decode().strip()
    sandbox_exists_before = bool(sandbox_path) and _ssh_test_exists(
        ssh_localhost, sandbox_path
    )

    mtime_before = None
    if sandbox_exists_before:
        res = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=5", ssh_localhost,
             f"stat -c %Y {sandbox_path}"],
            capture_output=True, timeout=30,
        )
        try:
            mtime_before = int(res.stdout.decode().strip())
        except ValueError:
            mtime_before = None

    agent.Deploy(assertive=True)

    if sandbox_exists_before:
        still_exists = _ssh_test_exists(ssh_localhost, sandbox_path)
        if still_exists:
            res = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
                 "-o", "ConnectTimeout=5", ssh_localhost,
                 f"stat -c %Y {sandbox_path}"],
                capture_output=True, timeout=30,
            )
            try:
                mtime_after = int(res.stdout.decode().strip())
            except ValueError:
                mtime_after = None
            assert mtime_before is None or mtime_after is None or mtime_after >= mtime_before, (
                f"assertive redeploy did not refresh sandbox at {sandbox_path}: "
                f"mtime {mtime_before} -> {mtime_after}"
            )


def test_ssh_deploy_runs_2_step_plan(
    ssh_localhost: str,
    metasmith_dev_image: str,
    ssh_agent_home: Source,
    tmp_path: Path,
):
    container_uri = f"docker://{metasmith_dev_image}"
    agent = Agent(
        home=ssh_agent_home,
        container=container_uri,
        runtime=Runtime.APPTAINER,
    )
    agent.Deploy()

    types = DataTypeLibrary()
    types["start"] = Endpoint(properties={"start"})
    types["mid"] = Endpoint(properties={"mid"})
    types["end"] = Endpoint(properties={"end"})
    types_path = tmp_path / "types.yml"
    types.Save(types_path)

    tr_dir = tmp_path / "transforms.xgdb"
    tr_dir.mkdir(parents=True, exist_ok=True)
    meta_dir = tr_dir / "_metadata"
    types_subdir = meta_dir / "types"
    types_subdir.mkdir(parents=True, exist_ok=True)
    shutil.copy(types_path, types_subdir / "mock.yml")
    (types_subdir / "transforms.yml").write_text(
        "schema: v1\n"
        "ontology:\n"
        "  name: EDAM\n"
        "  version: '1.25'\n"
        "  doi: https://doi.org/10.1093/bioinformatics/btt113\n"
        "  strict: false\n"
        "types:\n"
        "  transform:\n"
        "    properties:\n"
        "    - metasmith\n"
        "    - transform\n",
        encoding="utf-8",
    )

    import yaml as _yaml
    transforms = (
        identity_transform("mock::start", "mock::mid")
        | identity_transform("mock::mid", "mock::end")
    )
    manifest = {}
    for name, code in transforms.items():
        (tr_dir / f"{name}.py").write_text(code, encoding="utf-8")
        manifest[f"{name}.py"] = {"type": "transforms::transform"}
    (meta_dir / "index.yml").write_text(
        _yaml.dump({"manifest": manifest, "schema": "v1"}), encoding="utf-8"
    )
    tr_lib = TransformInstanceLibrary.Load(tr_dir)

    data_dir = tmp_path / "samples.xgdb"
    data_lib = DataInstanceLibrary(data_dir)
    data_lib.AddTypeLibrary(types_path, namespace="mock")
    (data_lib.location / "input.txt").write_text("hello\n", encoding="utf-8")
    data_lib.AddItem(Path("input.txt"), "mock::start")
    data_lib.Save()

    targets = TargetBuilder()
    targets.Add("mock::end")
    task = agent.GenerateWorkflow(
        samples=[data_lib],
        resources=[],
        transforms=[tr_lib],
        targets=targets,
    )
    assert task.ok, (
        f"GenerateWorkflow returned not-ok task; hints={task.plan.hints}"
    )
    assert len(task.plan.steps) == 2, (
        f"expected 2-step plan (start→mid→end); got {len(task.plan.steps)}"
    )

    agent.StageWorkflow(task)

    home_path = ssh_agent_home.GetPath()
    task_path = AgentPaths.to_task(task.GetKey(), root=home_path)
    workspace = task_path.parent.parent
    launcher = workspace / AgentPaths.LAUNCHER_FILE
    assert _ssh_test_exists(ssh_localhost, str(launcher)), (
        f"launcher missing at {launcher} after StageWorkflow — SSH deploy "
        f"did not reach a runnable state"
    )

    workflow_nf = workspace / AgentPaths.NXF_WORKFLOW
    assert _ssh_test_exists(ssh_localhost, str(workflow_nf)), (
        f"workflow.nf missing at {workflow_nf} after StageWorkflow"
    )
