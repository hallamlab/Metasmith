from pathlib import Path


def test_parse_path_symlink_into_home(tmp_path: Path) -> None:
    from metasmith.bootstrap import _parse_path
    from metasmith.constants import AgentPaths

    link = tmp_path / "input.fa"
    link.symlink_to(AgentPaths.HOME_ROOT / "runs/TESTKEY/inputs/some.fa")

    parsed = _parse_path(
        link,
        agent_home="/host/scratch/agent",
        external_cwd=tmp_path,
        task_key="TESTKEY",
    )

    assert parsed.external == Path("/host/scratch/agent/runs/TESTKEY/inputs/some.fa")
    assert parsed.local == AgentPaths.HOME_ROOT / "runs/TESTKEY/inputs/some.fa"


def test_parse_path_symlink_foreign(tmp_path: Path) -> None:
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

    assert parsed.external == foreign
    assert parsed.container == foreign
