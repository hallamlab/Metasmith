"""`bin/sbatch:64-65` `fix_paths` rewrites a Nextflow-generated
`.command.run` script so its references to the in-container roots are
re-pointed to the host-side roots:

    content = content.replace(str(AgentPaths.HOME_ROOT), str(agent.real_path))
    content = content.replace(str(AgentPaths.WORK_ROOT) + "/", str(external_workspace) + "/")

These naked `str.replace` calls corrupt any inner occurrence of the
matched substring, even on lines that shouldn't be rewritten — e.g.
literal user data, environment variable names, or paths to backup
directories that happen to contain the prefix.

This test feeds a `.command.run`-shaped script through the same
substitution and asserts that the inner occurrences survive untouched.
Today's code corrupts them; after the overhaul, the rewrite will
ascend via `PathMap` and `relative_to`, leaving inner occurrences
alone.

Reference: audit Category A (string-replace rewrites, silent overlap
corruption); the agents.py mirror lives in test_str_replace_path_overlap.py.
"""
from pathlib import Path

from metasmith.models.paths import reroot_in_text


def _sbatch_fix_paths_content(
    content: str, home_root: Path, real_path: Path, work_root: Path, external_workspace: Path
) -> str:
    """Replicates the rewrite at `src/metasmith/bin/sbatch:fix_paths`
    via the centralised :func:`reroot_in_text` helper. Pre-overhaul,
    this used raw ``str.replace`` and silently corrupted inner
    substrings.
    """
    content = reroot_in_text(content, home_root, real_path)
    content = reroot_in_text(content, work_root, external_workspace)
    return content


def test_fix_paths_does_not_corrupt_inner_home_root() -> None:
    """A `.command.run` line that legitimately references a backup
    directory under `/data/msm_home_old_backup` should be unchanged
    by the prefix rewrite. Today's code corrupts the inner occurrence.
    """
    home_root = Path("/msm_home")
    real_path = Path("/scratch/agent")
    work_root = Path("/ws")
    external_workspace = Path("/scratch/agent/runs/KEY")

    # The first line is the legitimate prefix occurrence (should be rewritten).
    # The second line references an unrelated path that happens to contain
    # the literal substring `/msm_home`. Should NOT be rewritten.
    content = (
        "cd /msm_home/runs/KEY/nxf_work/aa/bb\n"
        "cp /data/msm_home_old_backup/reference.fa ./ref.fa\n"
    )

    result = _sbatch_fix_paths_content(
        content, home_root, real_path, work_root, external_workspace
    )

    expected = (
        "cd /scratch/agent/runs/KEY/nxf_work/aa/bb\n"
        "cp /data/msm_home_old_backup/reference.fa ./ref.fa\n"
    )
    assert result == expected, (
        f"sbatch fix_paths corrupted inner /msm_home substring:\n"
        f"  got:\n{result}\n"
        f"  expected:\n{expected}"
    )


def test_fix_paths_does_not_corrupt_inner_work_root() -> None:
    """A `.command.run` line that legitimately references `/ws-bin/`
    (an unrelated path that happens to start with `/ws/...` after
    splitting) should not be rewritten. The matched substring
    `'/ws/'` is uniquely identifying for `WORK_ROOT/` only at the
    canonical prefix position.
    """
    home_root = Path("/msm_home")
    real_path = Path("/scratch/agent")
    work_root = Path("/ws")
    external_workspace = Path("/scratch/agent/runs/KEY")

    # `/wsadm/ws/` contains `/ws/` as a substring but is an unrelated path.
    content = (
        "cd /ws/nxf_work/aa/bb\n"
        "echo something > /wsadm/ws/scratchpad.txt\n"
    )

    result = _sbatch_fix_paths_content(
        content, home_root, real_path, work_root, external_workspace
    )

    expected = (
        "cd /scratch/agent/runs/KEY/nxf_work/aa/bb\n"
        "echo something > /wsadm/ws/scratchpad.txt\n"
    )
    assert result == expected, (
        f"sbatch fix_paths corrupted inner /ws/ substring:\n"
        f"  got:\n{result}\n"
        f"  expected:\n{expected}"
    )
