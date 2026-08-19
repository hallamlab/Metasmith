import subprocess
from pathlib import Path

from metasmith.models.paths import inject_array_child_guard


_ARRAY_SH = (
    "#!/bin/bash -ue\n"
    "array=( /host/runs/K/nxf_work/ed/aaa /host/runs/K/nxf_work/04/bbb )\n"
    "export nxf_array_task_dir=${array[SLURM_ARRAY_TASK_ID]}\n"
    "bash $nxf_array_task_dir/.command.run 2>&1 > $nxf_array_task_dir/.command.log\n"
)

_NON_ARRAY = (
    "#!/bin/bash -ue\n"
    "cd /host/work\n"
    "bash .command.run\n"
)


def test_guard_injected_before_child_invocation() -> None:
    out = inject_array_child_guard(_ARRAY_SH)
    assert out != _ARRAY_SH
    assert "# msm-phantom-guard" in out
    assert "trap msm_phantom_guard EXIT" in out
    assert '$nxf_array_task_dir/.command.begin' in out
    assert '$nxf_array_task_dir/.exitcode' in out
    assert out.index("trap msm_phantom_guard EXIT") < out.index(
        "bash $nxf_array_task_dir/.command.run"
    )
    assert out.index("export nxf_array_task_dir=") < out.index(
        "# msm-phantom-guard"
    )


def test_guard_is_idempotent() -> None:
    once = inject_array_child_guard(_ARRAY_SH)
    twice = inject_array_child_guard(once)
    assert once == twice


def test_non_array_script_unchanged() -> None:
    assert inject_array_child_guard(_NON_ARRAY) == _NON_ARRAY


def _run_dispatcher(tmp_path: Path, child_body: str) -> tuple[Path, int]:
    work = tmp_path / "child"
    work.mkdir()
    (work / ".command.run").write_text("#!/bin/bash\n" + child_body)
    sh = (
        "#!/bin/bash -ue\n"
        f"array=( {work} )\n"
        "export nxf_array_task_dir=${array[SLURM_ARRAY_TASK_ID]}\n"
        "bash $nxf_array_task_dir/.command.run 2>&1 > $nxf_array_task_dir/.command.log\n"
    )
    guarded = inject_array_child_guard(sh)
    script = tmp_path / ".command.sh"
    script.write_text(guarded)
    script.chmod(0o755)
    env = {"SLURM_ARRAY_TASK_ID": "0", "PATH": "/usr/bin:/bin"}
    proc = subprocess.run(["/bin/bash", str(script)], env=env, capture_output=True)
    return work, proc.returncode


def test_guard_fills_files_when_child_dies_before_begin(tmp_path: Path) -> None:
    work, _ = _run_dispatcher(tmp_path, "exit 126\n")
    assert (work / ".command.begin").exists(), "guard did not create .command.begin"
    assert (work / ".exitcode").exists(), "guard did not create .exitcode"
    assert (work / ".exitcode").read_text().strip() == "126"


def test_guard_does_not_clobber_a_real_exitcode(tmp_path: Path) -> None:
    child = (
        ': > "$nxf_array_task_dir/.command.begin"\n'
        'echo 0 > "$nxf_array_task_dir/.exitcode"\n'
        "exit 0\n"
    )
    work, rc = _run_dispatcher(tmp_path, child)
    assert rc == 0
    assert (work / ".command.begin").exists()
    assert (work / ".exitcode").read_text().strip() == "0", "guard clobbered a real exitcode"
