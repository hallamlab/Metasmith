"""The pool's one write door.

Everything that enters the store enters here: a product a run promoted, a file
the user imported, a reference a pipeline fetched. One manifest shape, one
place that resolves a shard, one place that computes a size.

The door has two halves because a promotion is two writes on two machines. The
shard is written inside the task, on a cluster node that has no business
opening the driver's sqlite; the row is written in the driver once nextflow has
exited. `write_shard` is the first half and `index_shard` the second. `admit`
is both, for a caller that is one process.

A shard is always derived from the key and the origin, never from a path a
task recorded -- a task records the path as its container sees it, and the
driver that reads it back sees somewhere else entirely.
"""

from __future__ import annotations

import os
import shutil
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Sequence

from .keys import canonical_cbor, multihash_key
from .layout import (
    MANIFEST_NAME,
    imported_shard_dir,
    out_dir as _out_dir,
    shard_dir as _shard_dir,
)


MANIFEST_VERSION = 3

# The two origins a pool entry can have. A product is re-derivable, so it is
# keyed under the cache epoch and an epoch bump may strand it. An import may be
# the user's only copy, so it is keyed outside the epoch and nothing but an
# explicit forget removes it.
PRODUCT = "lineage"
IMPORTED = "imported"
ORIGINS = (PRODUCT, IMPORTED)


@dataclass(frozen=True)
class PoolFile:
    """One file in the pool: what it is, where it is, and what produced it.

    `relpath` names bytes the pool holds, under the shard. `abspath` names
    bytes it only indexes, which stay where the user put them. Exactly one of
    the two is set.
    """
    dtype_name: str = ""
    dtype_key: str = ""
    relpath: str = ""
    abspath: str = ""
    slot_id: str = ""
    branch_idx: int = 0
    parents: list = field(default_factory=list)
    size: int = 0
    # Where to copy the bytes from, for a file the pool is taking custody of.
    # Never written to the manifest: it names a path in the producing task.
    src: str = ""

    def __post_init__(self):
        assert bool(self.relpath) != bool(self.abspath), (
            f"a pool file is held ({self.relpath!r}) or indexed "
            f"({self.abspath!r}), not both and not neither"
        )

    @property
    def in_place(self) -> bool:
        return not self.relpath

    def Resolve(self, shard: Path) -> Path:
        return Path(self.abspath) if self.in_place else Path(shard) / self.relpath

    def InstanceId(self) -> str:
        """This file's own identity, the same one the run recorded for it.

        A held file's id is minted from its slot and its name, which is what
        Orchestrator._post stamps onto the channel. An indexed file was never
        produced by anything, so it arrives already carrying its identity.
        """
        from ..models.lineage import LinPayload

        if self.in_place:
            return self.slot_id
        if not self.slot_id:
            return ""
        return LinPayload.mint_file_id(self.slot_id, self.relpath)

    def Pack(self) -> dict:
        d = {
            "dtype_name": self.dtype_name,
            "dtype_key": self.dtype_key,
            "slot_id": self.slot_id,
            "branch_idx": int(self.branch_idx),
            "parents": list(self.parents),
            "size": int(self.size),
        }
        if self.in_place:
            d["abspath"] = self.abspath
        else:
            d["relpath"] = self.relpath
        return d

    @classmethod
    def Unpack(cls, raw: dict) -> "PoolFile":
        # A v1 manifest wrote a library-relative path into `relpath` and placed
        # nothing, so what it named was never under the shard. Read it as what
        # it was: an indexed file, not a held one.
        relpath = str(raw.get("relpath", ""))
        abspath = str(raw.get("abspath", ""))
        if relpath and Path(relpath).is_absolute():
            relpath, abspath = "", relpath
        if not relpath and not abspath:
            abspath = "?"
        return cls(
            dtype_name=str(raw.get("dtype_name", "")),
            dtype_key=str(raw.get("dtype_key", "")),
            relpath=relpath,
            abspath=abspath,
            slot_id=str(raw.get("slot_id", "")),
            branch_idx=int(raw.get("branch_idx", 0)),
            parents=list(raw.get("parents", []) or []),
            size=int(raw.get("size", 0) or 0),
        )


@dataclass(frozen=True)
class ShardWrite:
    status: str
    shard: Path
    manifest: dict
    size: int


IMPORT_KIND = "import"


def structural_import_id(dtype_name: str, name: str) -> str:
    """The identity of an imported item: its type, and the name it is given.

    The same shape as `structural_slot_id` and for the same reason. A product's
    id folds what derived it; an import was derived by nothing, so what it
    declares is all there is. Neither reads a byte or stats a path, which is
    what makes importing a folder of six hundred thousand files cost what
    importing one file costs -- and is the same reason the type carries the
    trust, since the type IS the structural input and validating the bytes
    would answer a question the identity never asked.

    Deliberately does NOT fold the cache epoch. A product is re-derivable, so an
    epoch bump may strand it; an import may be the user's only copy, and an id
    that moved with the epoch would strand one on every bump.
    """
    return multihash_key(
        canonical_cbor({"kind": IMPORT_KIND, "dtype": dtype_name, "name": name})
    ).hex()


def shard_for(cache_root: Path, key_hex: str, origin: str) -> Path:
    assert origin in ORIGINS, f"bad origin {origin!r}"
    if origin == IMPORTED:
        return imported_shard_dir(Path(cache_root), key_hex)
    return _shard_dir(Path(cache_root), key_hex)


def build_manifest(
    *,
    key: bytes,
    origin: str,
    files: Sequence[PoolFile],
    transform_key: str = "",
    signature: str = "",
    step_name: str = "",
    consumes: dict | None = None,
    lineage: dict | None = None,
    lineage_payload: bytes = b"",
) -> dict:
    return {
        "v": MANIFEST_VERSION,
        "key": key,
        "origin": origin,
        "tk": transform_key,
        "sig": signature,
        "step_name": step_name,
        "files": [f.Pack() for f in files],
        "consumes": dict(consumes or {}),
        # The lineage index, keyed by channel. Distinct from `lineage_payload`,
        # which is the opaque bytes an instance carries -- a v1 manifest wrote
        # those bytes under this same name, which `manifest_lineage` untangles.
        "lineage": dict(lineage or {}),
        "lineage_payload": lineage_payload or b"",
        "size": sum(int(f.size) for f in files),
    }


def manifest_files(manifest: dict) -> list[PoolFile]:
    return [PoolFile.Unpack(f) for f in manifest.get("files", []) or []]


def manifest_lineage(manifest: dict) -> tuple[dict, bytes]:
    """The lineage index and the lineage payload, whichever version wrote them."""
    raw = manifest.get("lineage")
    payload = manifest.get("lineage_payload") or b""
    if isinstance(raw, (bytes, bytearray)):
        return {}, bytes(raw)
    return dict(raw or {}), bytes(payload)


def manifest_size(manifest: dict) -> int:
    size = manifest.get("size")
    if size is not None:
        return int(size)
    # v1 recorded no total. Sum what the files say rather than reporting zero,
    # which would make an entry look free to keep.
    return sum(int(f.get("size", 0) or 0) for f in manifest.get("files", []) or [])


def write_shard(
    *,
    cache_root: Path,
    key: bytes,
    origin: str,
    files: Sequence[PoolFile],
    transform_key: str = "",
    signature: str = "",
    step_name: str = "",
    consumes: dict | None = None,
    lineage: dict | None = None,
    lineage_payload: bytes = b"",
) -> ShardWrite:
    """Stage this entry's shard and rename it into place. Never opens sqlite.

    A shard already there wins: two tasks that reached the same key produced
    the same thing, and the one that got there first has readers.
    """
    cache_root = Path(cache_root)
    key_hex = key.hex()
    manifest = build_manifest(
        key=key, origin=origin, files=files, transform_key=transform_key,
        signature=signature, step_name=step_name, consumes=consumes,
        lineage=lineage, lineage_payload=lineage_payload,
    )
    final = shard_for(cache_root, key_hex, origin)
    if final.exists():
        return ShardWrite("exists", final, manifest, int(manifest["size"]))

    tmp = cache_root / f"{key_hex}.{socket.gethostname()}.{os.getpid()}.tmp"
    if tmp.exists():
        shutil.rmtree(tmp)
    _out_dir(tmp).mkdir(parents=True)
    for f in files:
        if f.in_place or not f.src:
            continue
        _place(Path(f.src), tmp / f.relpath)
    (tmp / MANIFEST_NAME).write_bytes(canonical_cbor(manifest))
    final.parent.mkdir(parents=True, exist_ok=True)
    try:
        tmp.rename(final)
    except OSError:
        shutil.rmtree(tmp, ignore_errors=True)
        status = "exists" if final.exists() else "failed"
        return ShardWrite(status, final, manifest, int(manifest["size"]))
    return ShardWrite("promoted", final, manifest, int(manifest["size"]))


def index_shard(
    store,
    *,
    cache_root: Path,
    key: bytes,
    origin: str,
    shard: Path,
    manifest: dict,
    transform_key: str | None = None,
) -> None:
    """The second half: the driver's row for a shard that is already on disk.

    `output_root` is stored relative to the cache root, always. An absolute
    component would win the join that reads it back, which is how a collection
    pass ends up outside the store.
    """
    shard = Path(shard)
    assert shard.is_relative_to(Path(cache_root)), (
        f"shard [{shard}] is outside the store at [{cache_root}]"
    )
    store.upsert(
        key=key,
        transform_key=(
            transform_key if transform_key is not None
            else str(manifest.get("tk", ""))
        ),
        payload=canonical_cbor(manifest),
        output_root=str(shard.relative_to(Path(cache_root))),
        size_bytes=manifest_size(manifest),
        origin=origin,
    )


def admit(
    *,
    cache_root: Path,
    key: bytes,
    origin: str,
    files: Sequence[PoolFile],
    store=None,
    **meta,
) -> ShardWrite:
    """Both halves, for a caller that is one process."""
    from .store import CacheStore

    cache_root = Path(cache_root)
    cache_root.mkdir(parents=True, exist_ok=True)
    written = write_shard(
        cache_root=cache_root, key=key, origin=origin, files=files, **meta,
    )
    if written.status == "failed":
        return written
    own = store is None
    store = CacheStore.open(cache_root) if own else store
    try:
        index_shard(
            store, cache_root=cache_root, key=key, origin=origin,
            shard=written.shard, manifest=written.manifest,
        )
    finally:
        if own:
            store.close()
    return written


def _place(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if src.is_dir():
        if dest.exists():
            shutil.rmtree(dest)
        shutil.copytree(src, dest, symlinks=True)
        return
    try:
        os.link(src, dest)
    except OSError:
        shutil.copy2(src, dest)
