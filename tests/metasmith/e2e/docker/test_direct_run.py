import pytest
from pathlib import Path

from metasmith.agents import Agent
from metasmith.coms.cli import main as cli_main
from metasmith.env import Runtime
from metasmith.models.direct_run import RunTransform
from metasmith.models.remote import Source
from metasmith.testing.mock_transforms import (
    alignment_transform,
    params_transform,
    provenance_transform,
)

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
    # Items are named by their manifest key, not by where they sit on disk.
    reads = None
    asm = None
    for p, name in samples_lib.manifest.items():
        if name == "mock::reads" and reads is None:
            reads = p
        if name == "mock::assembly" and asm is None:
            asm = p
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
            data_library=mock_samples,
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
                data_library=mock_samples,
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
                data_library=mock_samples,
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
                data_library=mock_samples,
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
            data_library=mock_samples,
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
                data_library=mock_samples,
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
                data_library=mock_samples,
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
            data_library=mock_samples,
            transform=tr_lib.location / "alignment.py",
            inputs=[("reads", reads), ("asm", asm)],
            work_dir=work,
        )
        assert result.success
        assert (work / "aligned.bam").exists()


class TestRunTransformLibraryResolution:
    def test_uncompiled_directory_says_so(self, mock_samples, temp_dir, agent_home):
        loose = temp_dir / "loose"
        loose.mkdir()
        (loose / "alignment.py").write_text(alignment_transform()["alignment"])

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                data_library=mock_samples,
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
            "-d", str(mock_samples.location),
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


def _provenance_transform() -> dict[str, str]:
    # Two slots of distinguishable types, where the library records one as the
    # other's parent. The protocol asks which metadata each read came from --
    # the question a collecting transform exists to answer, and the one that
    # needs both a slot channel and a per-item ancestry to answer correctly.
    return {
        "pairs": '''
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
label = model.AddRequirement(lib.GetType("mock::sample_metadata"))
item = model.AddRequirement(lib.GetType("mock::reads"), parents={label})
out = model.AddProduct(lib.GetType("mock::merged"))

def protocol(context: ExecutionContext):
    lines = []
    for p in context.InputGroup(item):
        src = context.SourceOf(p, label)
        name = src.local.parent.name if src is not None else "UNPAIRED"
        lines.append(name + "\\t" + p.local.parent.name)
    out_path = context.Output(out)
    out_path.local.write_text("\\n".join(sorted(lines)) + "\\n")
    return ExecutionResult(manifest=[{out: out_path.local}], success=True)

TransformInstance(protocol=protocol, model=model, group_by=label)
'''
    }


class TestRunTransformProvenance:
    def test_two_labels_two_items_pair_by_ancestry(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_prov", mock_types, _provenance_transform(),
        )
        work = temp_dir / "work_prov"

        result = RunTransform(
            data_library=mock_samples,
            transform=tr_lib.location / "pairs.py",
            inputs=[
                ("label", Path("sample_00/metadata.json")),
                ("label", Path("sample_01/metadata.json")),
                ("item", Path("sample_00/reads.fq")),
                ("item", Path("sample_01/reads.fq")),
            ],
            work_dir=work,
            agent_home=agent_home,
        )
        assert result.success
        produced = [p for p in work.glob("1-1-1.*") if p.is_file()]
        assert len(produced) == 1, sorted(p.name for p in work.iterdir())
        assert produced[0].read_text().split() == [
            "sample_00", "sample_00", "sample_01", "sample_01",
        ]


class TestRunTransformBinding:
    def test_a_filesystem_path_is_refused(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_path", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)

        with pytest.raises(ValueError) as excinfo:
            RunTransform(
                data_library=mock_samples,
                transform=tr_lib.location / "alignment.py",
                inputs=[("reads", temp_dir / "elsewhere.fq"), ("asm", asm)],
                work_dir=temp_dir / "work_path",
                agent_home=agent_home,
            )
        msg = str(excinfo.value)
        assert "filesystem path" in msg
        assert "metasmith data import" in msg

    def test_an_absolute_path_inside_the_library_is_the_same_item(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_abs", mock_types, alignment_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_abs"

        result = RunTransform(
            data_library=mock_samples,
            transform=tr_lib.location / "alignment.py",
            inputs=[
                ("reads", mock_samples.location / reads),
                ("asm", asm),
            ],
            work_dir=work,
            agent_home=agent_home,
        )
        assert result.success
        assert (work / "aligned.bam").exists()


# A direct run's machine is the caller's to state, and the default is the smallest
# legal one rather than a useful one.
class TestRunTransformParams:
    def test_defaults_are_one(self, mock_samples, mock_types, temp_dir, agent_home):
        tr_lib = create_transform_library(
            temp_dir / "tr_params_default", mock_types, params_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_params_default"

        result = RunTransform(
            data_library=mock_samples,
            transform=tr_lib.location / "params_echo.py",
            inputs=[("reads", reads), ("asm", asm)],
            work_dir=work,
            agent_home=agent_home,
        )
        assert result.success
        assert (work / "aligned.bam").read_text() == "cpus=1 memory=1 attempt=1"

    def test_cli_flags_reach_the_protocol(
        self, mock_samples, mock_types, temp_dir, agent_home, monkeypatch,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_params_cli", mock_types, params_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_params_cli"

        argv = [
            "metasmith", "run",
            str(tr_lib.location / "params_echo.py"),
            "-d", str(mock_samples.location),
            "-i", f"reads={reads}",
            "-i", f"asm={asm}",
            "-w", str(work),
            "--agent-home", str(agent_home),
            "--cpus", "12", "--memory", "48", "--attempt", "3",
        ]
        monkeypatch.setattr("sys.argv", argv)

        with pytest.raises(SystemExit) as excinfo:
            cli_main()
        assert excinfo.value.code == 0
        assert (work / "aligned.bam").read_text() == "cpus=12 memory=48 attempt=3"


# A direct run is one coherent sample, so every supplied input is an ancestor of every
# other and `SourceOf` has to answer rather than raise. Without this the whole class of
# collecting transforms -- anything that recovers a sample label from its inputs -- is
# unrunnable outside Nextflow.
class TestRunTransformSiblingProvenance:
    def test_source_of_resolves_a_sibling_slot(
        self, mock_samples, mock_types, temp_dir, agent_home,
    ):
        tr_lib = create_transform_library(
            temp_dir / "tr_sibling", mock_types, provenance_transform(),
        )
        reads, asm = _alignment_inputs(mock_samples)
        work = temp_dir / "work_sibling"

        result = RunTransform(
            data_library=mock_samples,
            transform=tr_lib.location / "provenance_echo.py",
            inputs=[("reads", reads), ("asm", asm)],
            work_dir=work,
            agent_home=agent_home,
        )
        assert result.success
        assert (work / "aligned.bam").read_text() == Path(reads).name

