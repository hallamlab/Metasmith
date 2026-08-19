import inspect
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from metasmith import agents as agents_mod
from metasmith.constants import AgentPaths


def _agents_source() -> str:
    pkg = Path(agents_mod.__file__).parent
    return "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(pkg.rglob("*.py"))
    )


def test_launcher_template_writes_valid_yaml_not_touch():
    src = _agents_source()
    assert "echo '{}' > {AgentPaths.NXF_PARAMS}".replace("{}", "{{}}") in src, (
        "launcher source no longer contains the `echo '{}' > NXF_PARAMS` fallback"
    )
    assert "touch {AgentPaths.NXF_PARAMS}" not in src, (
        "launcher source still has the broken `touch NXF_PARAMS` fallback "
        "that produces a 0-byte file Nextflow can't parse"
    )


@pytest.fixture(scope="module")
def nextflow_bin():
    env_nxf = Path(sys.executable).parent / "nextflow"
    if env_nxf.exists() and os.access(env_nxf, os.X_OK):
        return [str(env_nxf)]
    path = shutil.which("nextflow")
    if path:
        return [path]
    repo_nxf = Path(__file__).resolve().parents[3] / "lib" / "nextflow"
    if repo_nxf.exists() and os.access(repo_nxf, os.X_OK):
        return [str(repo_nxf)]
    pytest.skip("no nextflow binary available")


@pytest.mark.slow
def test_zero_byte_params_file_breaks_nextflow(tmp_path, nextflow_bin):
    (tmp_path / "main.nf").write_text("workflow { log.info \"params: ${params}\" }\n")
    empty_params = tmp_path / "workflow.params.yml"
    empty_params.touch()
    assert empty_params.stat().st_size == 0

    result = subprocess.run(
        nextflow_bin + ["run", "main.nf", "-params-file", str(empty_params)],
        cwd=tmp_path, capture_output=True, text=True, timeout=180,
        env={**os.environ, "NXF_OFFLINE": "TRUE", "NXF_ANSI_LOG": "false"},
    )
    combined = (result.stdout or "") + (result.stderr or "")
    assert "Cannot parse params file" in combined, (
        f"expected Nextflow to reject 0-byte params; got stdout=[{result.stdout}] "
        f"stderr=[{result.stderr}]"
    )


@pytest.mark.slow
def test_empty_mapping_params_file_passes_nextflow(tmp_path, nextflow_bin):
    (tmp_path / "main.nf").write_text("workflow { log.info \"params: ${params}\" }\n")
    params_path = tmp_path / AgentPaths.NXF_PARAMS

    snippet = f"[ -e {AgentPaths.NXF_PARAMS} ] || echo '{{}}' > {AgentPaths.NXF_PARAMS}"
    subprocess.run(["bash", "-c", snippet], cwd=tmp_path, check=True)
    assert params_path.exists()
    assert params_path.read_text().strip() == "{}"

    result = subprocess.run(
        nextflow_bin + ["run", "main.nf", "-params-file", str(params_path)],
        cwd=tmp_path, capture_output=True, text=True, timeout=180,
        env={**os.environ, "NXF_OFFLINE": "TRUE", "NXF_ANSI_LOG": "false"},
    )
    combined = (result.stdout or "") + (result.stderr or "")
    assert result.returncode == 0, (
        f"Nextflow rejected the {{}} fallback: stdout=[{result.stdout}] "
        f"stderr=[{result.stderr}]"
    )
    assert "Cannot parse params file" not in combined
    assert "params: [:]" in combined or "params:[:]" in combined
