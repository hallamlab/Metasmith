"""Regression test for issue 2: the launcher's params-YAML fallback must
produce a Nextflow-parseable file.

Pre-fix, agents.py:980 wrote `touch workflow.params.yml` when the file
didn't exist; Nextflow 26.04.1 rejects 0-byte params files with
"Cannot parse params file" and aborts before any process runs.

Post-fix, the fallback writes `{}` (an empty mapping), which Nextflow
parses as `params = [:]`.
"""

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
    # The whole package, not one module. `inspect.getsource` on a package
    # returns its __init__ -- pure re-exports since the split -- and the second
    # assertion below pins an *absence*, so it would have passed against a
    # string with no launcher in it at all. Globbing keeps it honest.
    pkg = Path(agents_mod.__file__).parent
    return "\n".join(
        p.read_text(encoding="utf-8") for p in sorted(pkg.rglob("*.py"))
    )


def test_launcher_template_writes_valid_yaml_not_touch():
    """Static check: the launcher source emits an echo redirect for
    NXF_PARAMS, never a `touch`. This pins the source-code shape so a
    future edit can't silently revert to the broken form.

    Looks at the unevaluated f-string in the source (the launcher line
    interpolates `{AgentPaths.NXF_PARAMS}`), so the assertions match the
    literal source text rather than the runtime-expanded form.
    """
    src = _agents_source()
    # Post-fix: `echo '{{}}' > {AgentPaths.NXF_PARAMS}` in source
    # (`{{}}` is an f-string-escaped literal `{}`).
    assert "echo '{}' > {AgentPaths.NXF_PARAMS}".replace("{}", "{{}}") in src, (
        "launcher source no longer contains the `echo '{}' > NXF_PARAMS` fallback"
    )
    # Pre-fix line must be gone — the literal `touch {AgentPaths.NXF_PARAMS}`
    # is what produced the 0-byte file that broke Nextflow.
    assert "touch {AgentPaths.NXF_PARAMS}" not in src, (
        "launcher source still has the broken `touch NXF_PARAMS` fallback "
        "that produces a 0-byte file Nextflow can't parse"
    )


@pytest.fixture(scope="module")
def nextflow_bin():
    """Locate a nextflow binary, or skip.

    Order matters: the interpreter running the tests is checked before PATH,
    because pytest is routinely invoked by absolute path (`.../envs/X/bin/python
    -m pytest`), which leaves that env's bin/ off PATH even though it is the
    environment under test. Falling through to a hardcoded env name instead is
    what made this fixture fail on any machine that did not happen to have an
    env by that name.
    """
    env_nxf = Path(sys.executable).parent / "nextflow"
    if env_nxf.exists() and os.access(env_nxf, os.X_OK):
        return [str(env_nxf)]
    path = shutil.which("nextflow")
    if path:
        return [path]
    # Common project layout: lib/nextflow downloaded by docker_builder.ensure_lib_prerequisites
    repo_nxf = Path(__file__).resolve().parents[3] / "lib" / "nextflow"
    if repo_nxf.exists() and os.access(repo_nxf, os.X_OK):
        return [str(repo_nxf)]
    pytest.skip("no nextflow binary available")


@pytest.mark.slow
def test_zero_byte_params_file_breaks_nextflow(tmp_path, nextflow_bin):
    """Sanity reproduction: confirm the legacy broken form still breaks.
    Pin the failure mode so we can attribute future regressions correctly."""
    (tmp_path / "main.nf").write_text("workflow { log.info \"params: ${params}\" }\n")
    empty_params = tmp_path / "workflow.params.yml"
    empty_params.touch()  # 0 bytes — the legacy `touch` fallback
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
    """Run the *exact* fallback command from agents.py:980 in a clean
    workspace, then feed the resulting file to Nextflow. The full chain
    (fallback shell snippet -> on-disk YAML -> Nextflow params load) must
    succeed."""
    (tmp_path / "main.nf").write_text("workflow { log.info \"params: ${params}\" }\n")
    params_path = tmp_path / AgentPaths.NXF_PARAMS

    # Reproduce the exact fallback shell snippet from agents.py.
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
    # And params actually loaded as an empty mapping.
    assert "params: [:]" in combined or "params:[:]" in combined
