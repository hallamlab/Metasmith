"""Preserve-behavior tests for `bootstrap._parse_path`'s symlink
handling (`src/metasmith/bootstrap.py:80-90`).

These two cases pass today and must keep passing through the path
overhaul. They sit in `tests/integration/` (not in `tests/path_overhaul/`)
because they exercise legitimate current behavior, not bug shapes.

Companion to:
- `tests/integration/repro_139_batched_lineage.py::test_parse_path_*`
  for the `/ws/<tail>` rewrite (inbox #139, case-1)
- `tests/path_overhaul/test_parse_path_apptainer_relative.py` for the
  apptainer `../ws/<tail>` bug (case-3, fails today)
"""
from pathlib import Path


def test_parse_path_symlink_into_home(tmp_path: Path) -> None:
    """A symlink whose target resolves under `HOME_ROOT` (case-2 of
    `_parse_path`) is rerouted to point into the agent's host home
    directory. The local view remains under `HOME_ROOT`; the external
    view sits under `agent_home`.
    """
    from metasmith.bootstrap import _parse_path
    from metasmith.constants import AgentPaths

    # Create a real symlink whose target string contains `HOME_ROOT`.
    # The case-2 rewrite swaps `HOME_ROOT` for `agent_home` in the
    # readlink output.
    link = tmp_path / "input.fa"
    link.symlink_to(AgentPaths.HOME_ROOT / "runs/TESTKEY/inputs/some.fa")

    parsed = _parse_path(
        link,
        agent_home="/host/scratch/agent",
        external_cwd=tmp_path,
        task_key="TESTKEY",
    )

    # External view: agent_home + the tail beneath HOME_ROOT.
    assert parsed.external == Path("/host/scratch/agent/runs/TESTKEY/inputs/some.fa")
    # Local view: still under HOME_ROOT (container-portable form).
    assert parsed.local == AgentPaths.HOME_ROOT / "runs/TESTKEY/inputs/some.fa"


def test_parse_path_symlink_foreign(tmp_path: Path) -> None:
    """A symlink whose target sits outside `agent_home` (e.g. a
    `/project/...` reference DB) preserves the absolute external path
    as both `external` and `container`, so `GetContainerModel` emits
    an identity bind mount.
    """
    from metasmith.bootstrap import _parse_path

    foreign = Path("/project/refdb/gtdb_v220/taxonomy.tsv")
    link = tmp_path / "tax.tsv"
    link.symlink_to(foreign)

    parsed = _parse_path(
        link,
        agent_home="/host/scratch/agent",
        external_cwd=tmp_path,
        task_key="TESTKEY",
    )

    # Foreign target survives intact on the external view.
    assert parsed.external == foreign
    # Container view defaults to the external path (identity bind).
    assert parsed.container == foreign
