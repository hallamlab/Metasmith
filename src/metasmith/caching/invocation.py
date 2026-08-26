"""The cache unit: one group member's invocation, keyed on what it consumed.

Every side that needs a key calls this module. The Orchestrator runs it as
`python -m metasmith.caching.invocation` once per batch, the task promotes
under the key it was handed, and the virtual runtime calls the functions
in-process. Nothing here opens sqlite: a task and the head node both probe by
looking at the shard on disk.

Imports only `caching.keys` and the standard library, so the helper's start-up
cost stays that of one interpreter.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

from .keys import CACHE_KEY_VERSION, canonical_cbor, lineage_key, multihash_key
from .layout import MANIFEST_NAME, shard_dir


FILES_KEY = "FILES"
PROV_KEY = "PROV"
KEY_KEY = "KEY"

TOMBSTONE_NAME = "tombstone"

Consumed = dict[str, list[str]]


def consumed_of(entry: dict, slot_channels: list[str] | tuple[str, ...]) -> Optional[Consumed]:
    """Per slot, the own-id of every staged item, or None when one is unknown.

    `PROV` holds one index map per staged item, in slot order. An item's own id
    is the value under its slot's channel name. A member missing `PROV`, or an
    item without its own id, has no identity to key on and is uncacheable.
    """
    prov = entry.get(PROV_KEY)
    if not isinstance(prov, list) or len(prov) != len(slot_channels):
        return None
    consumed: Consumed = {}
    for channel, items in zip(slot_channels, prov):
        ids: list[str] = []
        for index in items:
            own = index.get(channel) if isinstance(index, dict) else None
            if not own:
                return None
            ids.extend(str(i) for i in own)
        consumed[channel] = sorted(set(ids))
    return consumed


def member_key(transform_key: str, signature: str, consumed: Consumed) -> bytes:
    inputs = [
        (slot, ident.encode("utf-8"))
        for slot in sorted(consumed)
        for ident in sorted(consumed[slot])
    ]
    return lineage_key(transform_key, signature, inputs)


def structural_slot_id(
    transform_key: str,
    signature: str,
    slot_key: str,
    branch: int,
    upstream_slot_ids: list[str],
) -> str:
    """The id of a produced slot: the producing transform and what feeds it.

    No step order and no sample id: run 1 with a given database and run 2 that
    downloads it shift every order, and a sample's chain must still match.
    """
    return multihash_key(
        canonical_cbor(
            {
                "v": CACHE_KEY_VERSION,
                "tk": transform_key,
                "sig": signature,
                "s": slot_key,
                "b": int(branch),
                "up": sorted(set(upstream_slot_ids)),
            }
        )
    ).hex()


def read_manifest(shard: Path) -> Optional[dict]:
    import cbor2

    p = shard / MANIFEST_NAME
    if not p.is_file():
        return None
    try:
        return cbor2.loads(p.read_bytes())
    except Exception:
        return None


def probe(cache_root: Path, key: bytes) -> Optional[Path]:
    """The shard that can serve `key`, or None.

    A hit needs a manifest, every file it lists present, and no tombstone.
    """
    shard = shard_dir(Path(cache_root), key.hex())
    if (shard / TOMBSTONE_NAME).exists():
        return None
    manifest = read_manifest(shard)
    if manifest is None:
        return None
    for f in manifest.get("files", []):
        if not (shard / f["relpath"]).exists():
            return None
    return shard


def probe_members(spec: dict) -> list[tuple[str, str, str]]:
    """One `(key_hex, verdict, shard)` per member; verdict is `-`, `hit` or `miss`."""
    tk = spec["tk"]
    sig = spec["sig"]
    slots = list(spec["slk"])
    cache_root = Path(spec["cache_root"])
    rows: list[tuple[str, str, str]] = []
    for entry in spec["members"]:
        consumed = consumed_of(entry, slots)
        if consumed is None:
            rows.append(("-", "-", ""))
            continue
        key = member_key(tk, sig, consumed)
        shard = probe(cache_root, key)
        if shard is None:
            rows.append((key.hex(), "miss", ""))
        else:
            rows.append((key.hex(), "hit", str(shard)))
    return rows


def main(argv: list[str] | None = None) -> int:
    spec = json.load(sys.stdin)
    for key_hex, verdict, shard in probe_members(spec):
        if verdict == "hit":
            sys.stdout.write(f"{key_hex}|hit|{shard}\n")
        else:
            sys.stdout.write(f"{key_hex}|{verdict}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
