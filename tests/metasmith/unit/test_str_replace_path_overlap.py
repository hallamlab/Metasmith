from pathlib import Path

from metasmith.models.paths import PathMap


def test_path_map_local_to_external_preserves_inner_substring() -> None:
    path_map = PathMap(extern_home=Path("/scratch/agent"), task_key="K")

    lib_location = Path("/msm_home/data/msm_home_old_backup/lib.xgdb")

    result = path_map.LocalToExternal(lib_location)

    expected = Path("/scratch/agent/data/msm_home_old_backup/lib.xgdb")
    assert result == expected, (
        f"PathMap.LocalToExternal corrupted inner substring:\n"
        f"  got      = {result}\n"
        f"  expected = {expected}"
    )
