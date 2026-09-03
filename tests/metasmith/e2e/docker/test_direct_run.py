import pytest
from pathlib import Path

from metasmith.agents import Agent
from metasmith.coms.cli import main as cli_main
from metasmith.env import Runtime
from metasmith.models.direct_run import RunTransform
from metasmith.models.remote import Source
from metasmith.testing.mock_transforms import alignment_transform

from .conftest import create_transform_library


@pytest.fixture
def agent_home(temp_dir) -> Path:
    home = temp_dir / "msm_home"
    (home / "lib").mkdir(parents=True)
    Agent(
        home=Source.FromLocal(home),
        runtime=Runtime.DOCKER,
    ).Save(home / "lib" / "agent.yml")
    return home


def _context_output_transform() -> dict[str, str]:
    # The shared mocks write their products by hand, so they never reach
    # _get_output_paths -> output_file_name -> member_token. That is the path
    # a real transform takes, and the path that broke unnoticed.
    return {
        "writer": '''
from pathlib import Path
from metasmith.models.libraries import (
    TransformInstanceLibrary,
    TransformInstance,
    ExecutionContext,
    ExecutionResult,
)
from metasmith.models.solver import Transform

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
reads = model.AddRequirement(lib.GetType("mock::reads"))
out = model.AddProduct(lib.GetType("mock::bam"))

def protocol(context: ExecutionContext):
    out_path = context.Output(out)
    out_path.local.write_text("written to the context-named path")
    return ExecutionResult(
        manifest=[{out: out_path.local}],
        success=out_path.local.exists(),
    )

TransformInstance(protocol=protocol, model=model, group_by=reads)
'''
    }


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
    def test_smoke_success(self, mock_samples, mock_types, temp_dir, agent_home):
        tr_lib = create_transform_library(
            temp_dir / "tr_smoke", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_smoke"

        result = RunTransform(
            transform=tr_lib.location / "alignment.py",
            inputs=[
                ("reads", reads),
                ("asm", asm),
            ],
            work_dir=work,
            agent_home=agent_home,
        )
        assert result.success
        assert (work / "aligned.bam").exists()

    def test_unknown_name_lists_the_bindable_ones(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_bad", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                transform=tr_lib.location / "alignment.py",
                inputs=[
                    ("reads", reads),
                    ("assembly", asm),
                ],
                work_dir=temp_dir / "work_bad",
                agent_home=agent_home,
            )
        msg = str(excinfo.value)
        assert "assembly" in msg
        assert "reads" in msg and "asm" in msg

    def test_a_product_is_not_bindable(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_out", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                transform=tr_lib.location / "alignment.py",
                inputs=[("reads", reads), ("asm", asm), ("out", asm)],
                work_dir=temp_dir / "work_out",
                agent_home=agent_home,
            )
        assert "produced by" in str(excinfo.value)

    def test_missing_input_is_named(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_missing", mock_types, alignment_transform(),
        )
        reads, _ = _alignment_inputs(mock_samples)

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                transform=tr_lib.location / "alignment.py",
                inputs=[("reads", reads)],
                work_dir=temp_dir / "work_missing",
                agent_home=agent_home,
            )
        assert "asm" in str(excinfo.value)


    def test_a_context_named_output_is_produced(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_ctx", mock_types, _context_output_transform(),
        )
        reads, _ = _alignment_inputs(mock_samples)
        work = temp_dir / "work_ctx"

        result = RunTransform(
            transform=tr_lib.location / "writer.py",
            inputs=[("reads", reads)],
            work_dir=work,
            agent_home=agent_home,
        )
        assert result.success
        produced = [p for p in work.glob("1-1-1.*") if p.is_file()]
        assert len(produced) == 1, sorted(p.name for p in work.iterdir())
        assert produced[0].read_text() == "written to the context-named path"


class TestRunTransformAgent:
    def test_no_agent_is_refused(self, mock_samples, mock_types, temp_dir, monkeypatch):
        monkeypatch.delenv("AGENT_HOME", raising=False)
        tr_lib = create_transform_library(
            temp_dir / "tr_noagent", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                transform=tr_lib.location / "alignment.py",
                inputs=[("reads", reads), ("asm", asm)],
                work_dir=temp_dir / "work_noagent",
            )
        assert "AGENT_HOME" in str(excinfo.value)

    def test_undeployed_home_is_refused(
        self, mock_samples, mock_types, temp_dir,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_bare", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        bare = temp_dir / "not_an_agent"
        bare.mkdir()

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                transform=tr_lib.location / "alignment.py",
                inputs=[("reads", reads), ("asm", asm)],
                work_dir=temp_dir / "work_bare",
                agent_home=bare,
            )
        assert "not a deployed agent" in str(excinfo.value)

    def test_agent_home_from_env(
        self, mock_samples, mock_types, temp_dir, agent_home, monkeypatch,
    ):
        monkeypatch.setenv("AGENT_HOME", str(agent_home))
        tr_lib = create_transform_library(
            temp_dir / "tr_env", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_env"

        result = RunTransform(
            transform=tr_lib.location / "alignment.py",
            inputs=[("reads", reads), ("asm", asm)],
            work_dir=work,
        )
        assert result.success
        assert (work / "aligned.bam").exists()


class TestRunTransformLibraryResolution:
    def test_uncompiled_directory_says_so(self, temp_dir, agent_home):
        loose = temp_dir / "loose"
        loose.mkdir()
        (loose / "alignment.py").write_text(alignment_transform()["alignment"])

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                transform=loose / "alignment.py",
                inputs=[],
                work_dir=temp_dir / "work_loose",
                agent_home=agent_home,
            )
        msg = str(excinfo.value)
        assert "metasmith build" in msg
        assert "alignment.py" in msg


class TestRunTransformCli:
    def test_cli_smoke(self, mock_samples, mock_types, temp_dir, agent_home, monkeypatch):
        tr_lib = create_transform_library(
            temp_dir / "tr_cli", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_cli"

        argv = [
            "metasmith", "run",
            str(tr_lib.location / "alignment.py"),
            "-i", f"reads={reads}",
            "-i", f"asm={asm}",
            "-w", str(work),
            "--agent-home", str(agent_home),
        ]
        monkeypatch.setattr("sys.argv", argv)

        with pytest.raises(SystemExit) as excinfo:
            cli_main()
        assert excinfo.value.code == 0
        assert (work / "aligned.bam").exists()
