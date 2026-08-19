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
        if self.fs_type in NETWORK_FS_TYPES:
            return "network"
        if self.fs_type.startswith("fuse.") and self.fs_type in NETWORK_FS_TYPES:
            return "network"
        return "local"


@lru_cache(maxsize=1)
def _read_mountinfo() -> tuple[MountEntry, ...]:
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
    return _longest_prefix_mount(path, _read_mountinfo())


def detect_strategy(path: Path, *, default: str = "link") -> str:
    entry = resolve_mount(path)
    if entry is None:
        return default
    return "copy" if entry.kind == "network" else "link"


class StraddleMountError(RuntimeError):
    pass
def assert_same_mount(cache_root: Path, work_dir: Path) -> MountEntry | None:
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
