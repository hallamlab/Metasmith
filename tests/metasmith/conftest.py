"""Shared pytest fixtures + auto-marker for the metasmith test suite.

Markers are auto-applied by directory (see `_DIR_MARKERS`). This is the contract
behind the `tests/<axis>/` layout: the directory is the declaration of intent,
the marker is just the pytest-visible projection.

Override by adding explicit `@pytest.mark.<name>` on a test — auto-markers
are additive, not exclusive.
"""

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

# (relative_dir_under_tests, [markers_to_apply])
_DIR_MARKERS: list[tuple[str, list[str]]] = [
    ("unit", ["fast"]),
    ("flow", ["fast"]),
    # Solver correctness -- which transforms get chosen, and whether the plan
    # that comes back is sound. Separate from `flow`, which asks what the
    # runtime then does with a plan it is handed.
    ("solver", ["fast"]),
    ("cache", ["fast"]),
    ("gui", ["fast", "gui"]),
    # bootstrap was `slow` because one 10k-scale class lived in its biggest
    # file. Everything else in the axis is sub-millisecond, so ~100 tests sat
    # out the daily loop to contain six. The scale tests moved to `perf`.
    ("bootstrap", ["fast"]),
    ("perf", ["slow"]),
    ("deploy", ["slow"]),
    ("e2e/virtual", ["e2e_virtual"]),
    ("e2e/docker", ["e2e_docker", "slow", "requires_docker"]),
    ("e2e/agentic/_harness", ["fast"]),
    ("e2e/agentic", ["e2e_agentic", "slow"]),
    ("audit", ["fast"]),
]


def pytest_addoption(parser):
    """`--solver=` -- which implementation the whole session's solves run on.

    This is what lets one axis be run both ways, which it has to be: the
    failure mode here is a *green* run that silently used the other
    implementation, and no assertion in `tests/solver` notices it. `rust` fails
    the session outright when no usable binary is staged, rather than quietly
    doing what `auto` would have done -- asking for a thing and getting
    something else is the shape of the bug, not the workaround for it.
    """
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
    # The python solver is on its way out. The engine is what ships and what the
    # suite is asking about, so tests that need the python implementation --
    # parity sweeps that use it as the engine's reference, and the few that
    # trace its internals -- are opt-in rather than part of any routine run,
    # release included. Reach for `--python-solver` when there is reason to
    # suspect the engine, which is the one case the comparison still answers.
    #
    # Skipped rather than deselected: a gate that vanishes without saying so is
    # how this suite lost four files once already.
    if not config.getoption("--python-solver"):
        skip_python_solver = pytest.mark.skip(
            reason="needs the python solver; pass --python-solver to run it"
        )
        for item in items:
            if "python_solver" in item.keywords:
                item.add_marker(skip_python_solver)

    # A file matching no prefix gets NO marker and therefore runs in no gate.
    # That is silent by construction -- the tests collect, pass locally, and are
    # simply never selected again -- and it had already swallowed four files
    # before anyone noticed. Directory-as-declaration is only a contract if
    # violating it is loud, so an unclaimed item fails collection.
    unclaimed: list[str] = []
    for item in items:
        rel = Path(item.fspath).resolve().relative_to(_TESTS_ROOT)
        rel_str = rel.as_posix()
        # Longest-prefix-first so e2e/agentic/_harness beats e2e/agentic
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
    """Project-wide VirtualE2ERuntime fixture.

    Defined here (not in tests/e2e/virtual/conftest.py) so cache / flow /
    audit tests can request it without cross-tree fixture imports. The
    e2e/virtual conftest re-imports this name to keep the local file as a
    standalone module-level reference for that subtree.
    """
    runtime = VirtualE2ERuntime(
        tmp_path / "virtual_rt", host="virt-host", force_bounce=False
    )
    runtime.setup(monkeypatch)
    _configure_agent_paths(monkeypatch, runtime.home)
    return runtime


@pytest.fixture
def virtual_runtime_bounce(tmp_path, monkeypatch):
    """VirtualE2ERuntime with the bounce path forced on."""
    runtime = VirtualE2ERuntime(
        tmp_path / "virtual_rt_bounce", host="virt-host", force_bounce=True
    )
    runtime.setup(monkeypatch)
    _configure_agent_paths(monkeypatch, runtime.home)
    return runtime


@pytest.fixture(scope="session")
def metasmith_libraries_root() -> Path:
    """The standard library, which lives in this repo.

    Lives here rather than in one axis's conftest because three axes want the
    real standard library: `flow` solves every shipped template, `solver`
    fingerprints them, and `perf` benchmarks them.

    It used to resolve a sibling `metasmith-libraries/main` checkout, which is
    now archived — and the anchor was one level short of where that sibling
    actually sat, so this had been quietly skipping all three axes since the
    monorepo layout landed. ``METASMITH_LIBRARIES_ROOT`` still overrides, for
    testing against a library that is not this one.
    """
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
    # `_metadata/` is compiled, not tracked, so a fresh checkout has none and
    # every solve below would raise on a missing index. Say which command
    # fixes it rather than failing three axes with an assertion from the loader.
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
