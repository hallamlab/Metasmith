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


def _agents_str_replace_extern_location(lib_location: Path, home_root: Path, extern_home: Path) -> str:
    """Replicates the line at `src/metasmith/agents.py:1192`."""
    return str(lib_location).replace(str(home_root), str(extern_home))


def test_str_replace_does_not_corrupt_inner_substring() -> None:
    """A library archived under a directory whose name contains
    `msm_home` (e.g. `msm_home_old_backup`) should not have the inner
    occurrence rewritten. Today's code corrupts it silently.
    """
    home_root = Path("/msm_home")
    extern_home = Path("/scratch/agent")

    # A library location with the offending shape — the home-root prefix
    # is correctly rooted, but a downstream directory contains the literal
    # substring `msm_home`.
    lib_location = Path("/msm_home/data/msm_home_old_backup/lib.xgdb")

    result = _agents_str_replace_extern_location(lib_location, home_root, extern_home)

    # The correct rewrite replaces ONLY the prefix.
    expected = "/scratch/agent/data/msm_home_old_backup/lib.xgdb"
    assert result == expected, (
        f"str.replace corrupted inner substring:\n"
        f"  got      = {result}\n"
        f"  expected = {expected}"
    )
