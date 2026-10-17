"""A binner sized out of its memory fails as an OOM, so the retry ladder asks for more memory.

SemiBin2 refuses before starting when its pair matrix cannot fit, and COMEBin fails when its
Leiden sweep was killed mid-way. Both name 137, and the step ends with the code its protocol
named rather than the generic 1.
"""

from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from metasmith.bootstrap import FailureExitCode
from metasmith.models.libraries import ExecutionResult, TransformInstanceLibrary


@dataclass
class _Path:
    local: Path

    @property
    def container(self) -> Path:
        return self.local


@dataclass
class _Context:
    inputs: dict
    params: dict
    on_exec: callable = None
    commands: list[str] = field(default_factory=list)

    def Input(self, dep):
        return _Path(self.inputs[dep.key])

    def Output(self, dep, **kw):
        return _Path(Path(f"out_{dep.key}_{kw.get('i', 0)}"))

    def LocalShell(self, cmd: str, **kw) -> int:
        return subprocess.run(["bash", "-c", f"set -eo pipefail\n{cmd}"]).returncode

    def ExecWithEnv(self, env=None, cmd="", **kw):
        self.commands.append(cmd)
        if self.on_exec:
            self.on_exec()


def _fasta(path: Path, n: int, length: int) -> Path:
    path.write_text("".join(f">c{i}\n{'A' * length}\n" for i in range(n)))
    return path


def _binner(root: Path, name: str):
    return TransformInstanceLibrary.Load(root / "transforms" / "metagenomics").GetTransform(f"binning/{name}.py")


def _inputs(transform, fasta: Path, tmp_path: Path) -> dict:
    asm = transform.protocol.__globals__["asm"]
    return {
        dep.key: fasta if dep.key == asm.key else tmp_path / f"in_{dep.key}"
        for dep in transform.model.requires
    }


@pytest.fixture
def work(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_semibin2_refuses_a_matrix_bigger_than_its_memory(metasmith_libraries_root, work):
    transform = _binner(metasmith_libraries_root, "semibin2")
    fasta = _fasta(work / "asm.fa", 40, 3000)
    ctx = _Context(_inputs(transform, fasta, work), {"memory": 8 * 30**2 / 1024**3})
    result = transform.protocol(ctx)
    assert (result.success, result.exit_code) == (False, 137)
    assert ctx.commands == []


def test_semibin2_falls_back_to_long_contigs_when_they_fit(metasmith_libraries_root, work):
    transform = _binner(metasmith_libraries_root, "semibin2")
    asm = work / "asm.fa"
    asm.write_text(_fasta(work / "a.fa", 40, 1500).read_text() + _fasta(work / "b.fa", 10, 3000).read_text())
    ctx = _Context(_inputs(transform, asm, work), {"memory": 8 * 30**2 / 1024**3})
    try:
        transform.protocol(ctx)
    except Exception:
        pass
    assert ctx.commands and "--min-len 2500" in ctx.commands[0]


def test_comebin_fails_as_an_oom_when_its_sweep_was_killed(metasmith_libraries_root, work):
    transform = _binner(metasmith_libraries_root, "comebin")
    fasta = _fasta(work / "asm.fa", 4, 2000)

    def killed_mid_sweep():
        state = work / "comebin_out" / "comebin_res" / "leiden_sweep.state"
        state.parent.mkdir(parents=True, exist_ok=True)
        state.write_text("running\n")

    ctx = _Context(_inputs(transform, fasta, work), {"cpus": 2}, on_exec=killed_mid_sweep)
    result = transform.protocol(ctx)
    assert (result.success, result.exit_code) == (False, 137)


class _Failed:
    def FailureExitCode(self):
        return 1


def test_a_protocols_named_code_is_the_steps_exit_code():
    results = [ExecutionResult(manifest=[], success=False, exit_code=137)]
    assert FailureExitCode(results, _Failed()) == 137


def test_without_a_named_code_the_commands_answer():
    assert FailureExitCode([ExecutionResult(manifest=[], success=False)], _Failed()) == 1
