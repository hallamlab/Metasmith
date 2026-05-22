"""`agents.py:1192` rewrites a library location from the in-container
form rooted at `HOME_ROOT` (`/msm_home`) to its host-side form rooted at
`extern_home`, using a naked `str.replace`:

    _extern_location = str(lib.location).replace(
        str(AgentPaths.HOME_ROOT), str(extern_home)
    )

`str.replace` substitutes **every** occurrence of the pattern, not just
the prefix. If any path component happens to contain the literal
substring `/msm_home`, the inner occurrence is silently corrupted too.
The same shape exists in `bootstrap.py:81` (`str(p.readlink()).replace(
str(HOME_ROOT), agent_home)`) and `bin/sbatch:64`.

This test pins the bug by feeding a library location with a nested
`msm_home_old_backup` segment and asserting that only the prefix is
rewritten. Replacing the call with `path_map.LocalToExternal(p)` (which
uses `relative_to` + join) will make this pass.

Reference: audit Category A (string-replace rewrites, silent overlap
corruption).
"""
from pathlib import Path

from metasmith.models.paths import PathMap


def test_path_map_local_to_external_preserves_inner_substring() -> None:
    """A library archived under a directory whose name contains
    ``/msm_home`` as a substring (e.g. ``msm_home_old_backup``) must
    not have its inner occurrence rewritten when translating from the
    container view to the host view. Today's
    ``str.replace(HOME_ROOT, extern_home)`` at
    ``src/metasmith/agents.py:1192`` corrupts the inner occurrence;
    :func:`PathMap.LocalToExternal` uses ``Path.relative_to`` and is
    immune.
    """
    path_map = PathMap(extern_home=Path("/scratch/agent"), task_key="K")

    # A library location with the offending shape — the home-root prefix
    # is correctly rooted, but a downstream directory contains the
    # literal substring `msm_home`.
    lib_location = Path("/msm_home/data/msm_home_old_backup/lib.xgdb")

    result = path_map.LocalToExternal(lib_location)

    expected = Path("/scratch/agent/data/msm_home_old_backup/lib.xgdb")
    assert result == expected, (
        f"PathMap.LocalToExternal corrupted inner substring:\n"
        f"  got      = {result}\n"
        f"  expected = {expected}"
    )
