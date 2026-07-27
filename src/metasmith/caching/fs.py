"""Filesystem validation + materialization strategy selection (S6).

Reads `/proc/self/mountinfo` once per process, then resolves the longest-
prefix mount for a given absolute path. Maps each mount to a coarse
"local" / "network" class via the fs_type column. The compile-time pass
in workflow.py uses this to:

- Refuse to emit a workflow if cache_root and work_dir straddle two
  separate mounts — rename across mounts is non-atomic and would break
  promote's loser-of-race contract.
- Pick the `publishDir` mode for cacheable miss steps: hardlink on local,
  full copy on network (Lustre, NFS, GPFS, BeeGFS, etc).
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


NETWORK_FS_TYPES = frozenset({
    "nfs", "nfs4", "lustre", "gpfs", "beegfs", "cifs", "smbfs",
    "fuse.glusterfs", "glusterfs", "ceph", "ocfs2",
})


@dataclass(frozen=True)
class MountEntry:
    mount_point: str
    fs_type: str

    @property
    def kind(self) -> str:
        # `fuse.<name>` style entries — match either the prefix or the
        # full type. Most network FUSE filesystems should be treated as
        # network so promote falls back to copy mode.
        if self.fs_type in NETWORK_FS_TYPES:
            return "network"
        if self.fs_type.startswith("fuse.") and self.fs_type in NETWORK_FS_TYPES:
            return "network"
        return "local"


@lru_cache(maxsize=1)
def _read_mountinfo() -> tuple[MountEntry, ...]:
    """Parse `/proc/self/mountinfo` into MountEntry rows.

    See proc(5) — mountinfo format:
      mount_id parent_id major:minor root mount_point opts - fs_type src super_opts
    The fs_type column is the first token after the " - " separator.
    """
    entries: list[MountEntry] = []
    try:
        text = Path("/proc/self/mountinfo").read_text()
    except OSError:
        return ()
    for line in text.splitlines():
        if not line.strip():
            continue
        head, sep, tail = line.partition(" - ")
        if not sep:
            continue
        head_fields = head.split()
        tail_fields = tail.split()
        if len(head_fields) < 5 or len(tail_fields) < 1:
            continue
        mount_point = head_fields[4]
        fs_type = tail_fields[0]
        entries.append(MountEntry(mount_point=mount_point, fs_type=fs_type))
    return tuple(entries)


def _longest_prefix_mount(
    path: Path,
    entries: tuple[MountEntry, ...],
) -> MountEntry | None:
    """Return the MountEntry whose mount_point is the deepest prefix of path."""
    abs_path = str(path.resolve())
    best: MountEntry | None = None
    best_len = -1
    for entry in entries:
        mp = entry.mount_point
        if abs_path == mp or abs_path.startswith(mp.rstrip("/") + "/"):
            if len(mp) > best_len:
                best_len = len(mp)
                best = entry
    return best


def resolve_mount(path: Path) -> MountEntry | None:
    """Public: resolve `path` to its containing mount entry, or None."""
    return _longest_prefix_mount(path, _read_mountinfo())


def detect_strategy(path: Path, *, default: str = "link") -> str:
    """'link' on local FS, 'copy' on network FS, `default` if unknown."""
    entry = resolve_mount(path)
    if entry is None:
        return default
    return "copy" if entry.kind == "network" else "link"


class StraddleMountError(RuntimeError):
    """Raised when cache_root and work_dir live on different mounts."""


def assert_same_mount(cache_root: Path, work_dir: Path) -> MountEntry | None:
    """Refuse to compile if the two paths straddle distinct mounts.

    Returns the shared MountEntry for callers that want to inspect it.
    Returns None on systems without `/proc/self/mountinfo` (e.g. macOS in
    a dev container) — caller should treat that as "skip strategy
    selection" and use the default.
    """
    entries = _read_mountinfo()
    if not entries:
        return None
    cache_mnt = _longest_prefix_mount(cache_root, entries)
    work_mnt = _longest_prefix_mount(work_dir, entries)
    if cache_mnt is None or work_mnt is None:
        return None
    if cache_mnt.mount_point != work_mnt.mount_point:
        raise StraddleMountError(
            f"cache_root [{cache_root}] is on mount [{cache_mnt.mount_point}] "
            f"({cache_mnt.fs_type}) but work_dir [{work_dir}] is on mount "
            f"[{work_mnt.mount_point}] ({work_mnt.fs_type}). Rename across "
            "mounts is non-atomic; metasmith refuses to compile. Move "
            "cache_root or work_dir so they share a mount."
        )
    return cache_mnt
