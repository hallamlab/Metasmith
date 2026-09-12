from pathlib import Path

from metasmith.models.paths import reroot_in_text


def _sbatch_fix_paths_content(
    content: str, home_root: Path, real_path: Path, work_root: Path, external_workspace: Path
) -> str:
    content = reroot_in_text(content, home_root, real_path)
    content = reroot_in_text(content, work_root, external_workspace)
    return content


def test_fix_paths_does_not_corrupt_inner_home_root() -> None:
    home_root = Path("/msm_home")
    real_path = Path("/scratch/agent")
    work_root = Path("/ws")
    external_workspace = Path("/scratch/agent/runs/KEY")

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
    home_root = Path("/msm_home")
    real_path = Path("/scratch/agent")
    work_root = Path("/ws")
    external_workspace = Path("/scratch/agent/runs/KEY")

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
