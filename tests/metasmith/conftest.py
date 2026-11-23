import os
import sys
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

# FULL_VERSION is read at import, and the docker lane's image is tagged with it; stamp first
# so the host and the image agree about which source they came from.
from metasmith._build_hash import write_build_hash
write_build_hash()

from metasmith.constants import AgentPaths
from metasmith.models.solver import Transform, Endpoint
from metasmith.testing.virtual_runtime import VirtualE2ERuntime

from ._groups import group_of
from .support import create_transform_library, write_mock_samples, write_mock_types


_TESTS_ROOT = Path(__file__).resolve().parent

_DIR_MARKERS: list[tuple[str, list[str]]] = [
    ("unit", ["fast"]),
    ("flow", ["fast"]),
    ("solver", ["fast"]),
    ("cache", ["fast"]),
    ("gui", ["fast", "gui"]),
    ("bootstrap", ["fast"]),
    ("perf", ["slow"]),
    ("deploy", ["slow"]),
    ("e2e/virtual", ["e2e_virtual"]),
    ("e2e/docker", ["e2e_docker", "slow", "requires_docker"]),
    ("e2e/nextflow", ["e2e_nextflow", "requires_nextflow"]),
    ("e2e/agentic/_harness", ["fast"]),
    ("e2e/agentic", ["e2e_agentic", "slow"]),
    ("audit", ["fast"]),
    ("lifecycle", ["lifecycle", "slow"]),
]


def pytest_collection_modifyitems(config, items):
    unclaimed: list[str] = []
    ungrouped: set[str] = set()
    for item in items:
        rel = Path(item.fspath).resolve().relative_to(_TESTS_ROOT)
        rel_str = rel.as_posix()
        if group_of(rel_str) is None:
            ungrouped.add(rel_str)
        for dir_prefix, markers in sorted(_DIR_MARKERS, key=lambda kv: -len(kv[0])):
            if rel_str.startswith(dir_prefix + "/"):
                for m in markers:
                    # Markers add up, so without this a test marked slow in a
                    # fast directory stays in the `-m fast` loop.
                    if m == "fast" and item.get_closest_marker("slow"):
                        continue
                    item.add_marker(getattr(pytest.mark, m))
                break
        else:
            unclaimed.append(rel_str)

    if unclaimed:
        listing = "\n  ".join(sorted(set(unclaimed)))
        raise pytest.UsageError(
            "these test files sit under no axis in tests/conftest.py::_DIR_MARKERS, "
            "so they carry no marker and run in no gate:\n  "
            f"{listing}\n"
            "Move each into the directory naming what it is (unit / flow / cache / "
            "gui / bootstrap / deploy / e2e/*), or add a row to _DIR_MARKERS."
        )
    if ungrouped:
        listing = "\n  ".join(sorted(ungrouped))
        raise pytest.UsageError(
            "these test files belong to no group in tests/metasmith/_groups.py, so no "
            f"merge check selects them:\n  {listing}\n"
            "Add each to OVERRIDES under the feature it pins."
        )


def _configure_agent_paths(monkeypatch, home: Path) -> None:
    monkeypatch.setattr(AgentPaths, "HOME_ROOT", home)
    monkeypatch.setattr(AgentPaths, "WORK_ROOT", home / "_ws")


@pytest.fixture
def mock_types(tmp_path) -> Path:
    return write_mock_types(tmp_path)


@pytest.fixture
def mock_samples(tmp_path, mock_types):
    return write_mock_samples(tmp_path, mock_types)


@pytest.fixture
def simple_workflow_task(mock_samples, mock_types, tmp_path):
    from metasmith.models.workflow import Case, WorkflowPlan, WorkflowTask
    from metasmith.models.solver import Transform
    from metasmith.testing.mock_transforms import identity_transform

    transforms = identity_transform("mock::assembly", "mock::bam")
    tr_lib = create_transform_library(tmp_path / "simple_tr", mock_types, transforms)

    given = [[sv] for sv in mock_samples.AsSamples("mock::assembly")]
    target_model = Transform()
    target_model.AddRequirement(properties={"bam"})
    target_names = ["bam"]

    plan = WorkflowPlan.Generate(
        cases=Case.ByShape(given, target=target_model, target_names=target_names),
        transforms=[tr_lib],
    )

    assert isinstance(plan, WorkflowPlan)
    return WorkflowTask(
        ok=True,
        plan=plan,
        data_libraries=[mock_samples],
        transform_libraries=[tr_lib],
    )


@pytest.fixture
def virtual_runtime(tmp_path, monkeypatch):
    runtime = VirtualE2ERuntime(
        tmp_path / "virtual_rt", host="virt-host", force_bounce=False
    )
    runtime.setup(monkeypatch)
    _configure_agent_paths(monkeypatch, runtime.home)
    return runtime


@pytest.fixture
def virtual_runtime_bounce(tmp_path, monkeypatch):
    runtime = VirtualE2ERuntime(
        tmp_path / "virtual_rt_bounce", host="virt-host", force_bounce=True
    )
    runtime.setup(monkeypatch)
    _configure_agent_paths(monkeypatch, runtime.home)
    return runtime


@pytest.fixture(scope="session")
def metasmith_libraries_root() -> Path:
    env = os.environ.get("METASMITH_LIBRARIES_ROOT")
    if env:
        p = Path(env).expanduser().resolve()
        if p.exists():
            return p
        pytest.skip(
            f"METASMITH_LIBRARIES_ROOT={env!r} does not exist; "
            "unset it or point at a library root"
        )
    root = Path(__file__).resolve().parents[2] / "src" / "metasmith_libraries"
    if not (root / "transforms" / "logistics" / "_metadata" / "index.yml").exists():
        pytest.skip(
            f"the standard library at {root} is not compiled — run `dev/libraries.sh -bm`"
        )
    return root


@pytest.fixture
def make_transform():
    def _make_transform():
        return Transform()
    return _make_transform


@pytest.fixture
def make_endpoint():
    def _make_endpoint(properties):
        return Endpoint(properties=properties)
    return _make_endpoint
