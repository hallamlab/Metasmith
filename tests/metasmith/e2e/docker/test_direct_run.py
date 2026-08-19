import pytest
from pathlib import Path

from metasmith.coms.cli import main as cli_main
from metasmith.models.direct_run import RunTransform
from metasmith.testing.mock_transforms import alignment_transform

from .conftest import create_transform_library


def _alignment_inputs(samples_lib) -> tuple[Path, Path]:
    reads = None
    asm = None
    for p, name in samples_lib.manifest.items():
        abs_path = samples_lib.location / p if not p.is_absolute() else p
        if name == "mock::reads" and reads is None:
            reads = abs_path
        if name == "mock::assembly" and asm is None:
            asm = abs_path
        if reads is not None and asm is not None:
            break
    assert reads is not None and asm is not None
    return reads, asm


class TestRunTransformApi:
    def test_smoke_success(self, mock_samples, mock_types, temp_dir):
        tr_lib = create_transform_library(
            temp_dir / "tr_smoke", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_smoke"

        result = RunTransform(
            transform_lib=tr_lib.location,
            transform="alignment.py",
            inputs=[
                ("mock::reads", reads),
                ("mock::assembly", asm),
            ],
            work_dir=work,
        )
        assert result.success
        assert (work / "aligned.bam").exists()

    def test_unmatched_input_type_errors(self, mock_samples, mock_types, temp_dir):
        tr_lib = create_transform_library(
            temp_dir / "tr_bad", mock_types, alignment_transform(),
        )
        reads, _ = _alignment_inputs(mock_samples)
        work = temp_dir / "work_bad"

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                transform_lib=tr_lib.location,
                transform="alignment.py",
                inputs=[
                    ("mock::reads", reads),
                    ("mock::bam", reads),
                ],
                work_dir=work,
            )
        msg = str(excinfo.value)
        assert "mock::bam" in msg or "assembly" in msg.lower()


class TestRunTransformCli:
    def test_cli_smoke(self, mock_samples, mock_types, temp_dir, monkeypatch):
        tr_lib = create_transform_library(
            temp_dir / "tr_cli", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_cli"

        argv = [
            "metasmith", "run",
            "-l", str(tr_lib.location),
            "-t", "alignment.py",
            "-i", f"mock::reads={reads}",
            "-i", f"mock::assembly={asm}",
            "-w", str(work),
        ]
        monkeypatch.setattr("sys.argv", argv)

        with pytest.raises(SystemExit) as excinfo:
            cli_main()
        assert excinfo.value.code == 0
        assert (work / "aligned.bam").exists()
