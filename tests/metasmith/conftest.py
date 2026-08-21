import os
import sys
from pathlib import Path

import pytest

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from metasmith.constants import AgentPaths
from metasmith.models.solver import Transform, Endpoint
from metasmith.testing.virtual_runtime import VirtualE2ERuntime


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
    ("e2e/agentic/_harness", ["fast"]),
    ("e2e/agentic", ["e2e_agentic", "slow"]),
    ("audit", ["fast"]),
    ("lifecycle", ["lifecycle", "slow"]),
]


def pytest_addoption(parser):
    parser.addoption(
        "--solver", action="store", default="auto",
        choices=["auto", "python", "rust"],
        help="solver implementation for this session (default: auto)",
    )
    parser.addoption(
        "--python-solver", action="store_true", default=False,
        help="also run the tests that need the python solver (see the"
             " `python_solver` marker); off by default, including in the"
             " release suite",
    )


@pytest.fixture(scope="session", autouse=True)
def _solver_selection(request):
    from metasmith.models.solver_backend import (
        PythonSolver, RustSolver, _set_solver_class,
    )
    choice = request.config.getoption("--solver")
    if choice == "auto":
        yield
        return
    if choice == "rust" and not RustSolver.Available():
        pytest.fail(
            "--solver=rust, but no msm_solver advertising `solve` is staged for"
            " this platform (./dev.sh -bel). Refusing to run the python solver"
            " under a rust label."
        )
    previous = _set_solver_class(PythonSolver if choice == "python" else RustSolver)
    yield
    _set_solver_class(previous)


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--python-solver"):
        skip_python_solver = pytest.mark.skip(
            reason="needs the python solver; pass --python-solver to run it"
        )
        for item in items:
            if "python_solver" in item.keywords:
                item.add_marker(skip_python_solver)

    unclaimed: list[str] = []
    for item in items:
        rel = Path(item.fspath).resolve().relative_to(_TESTS_ROOT)
        rel_str = rel.as_posix()
        for dir_prefix, markers in sorted(_DIR_MARKERS, key=lambda kv: -len(kv[0])):
            if rel_str.startswith(dir_prefix + "/"):
                for m in markers:
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


def _configure_agent_paths(monkeypatch, home: Path) -> None:
    monkeypatch.setattr(AgentPaths, "HOME_ROOT", home)
    monkeypatch.setattr(AgentPaths, "WORK_ROOT", home / "_ws")


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
