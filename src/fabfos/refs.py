"""The reference databases, identified from their pin instead of their filesystem.

A reference here is 17 GB of diamond database, 7.2 GB of kofam profiles, an
embedding stack. `DataInstanceLibrary.AddItem` derives a leaf's identity from
the file's path and mtime, which costs nothing to take and nothing to retake --
but it moves whenever `dvc checkout` re-materialises the very same bytes, and
these are the entries under which the most downstream cache sits.

Every one of these chunks is DVC-pinned, and a `.dvc` file records an md5 that
DVC computed over the real bytes. That is a better identity than anything this
code could derive: agreed on by every host that checks out the same pin, and
indifferent to when the bytes last landed. So an id here is

    multihash_key(b"dvc\\0" + md5 + relpath)

mirroring `_fir.pin_external_leaf_ids`' construction for a remote path. The
relative path is folded in for the same reason `_mint_leaf_id` folds it: one pin
covers a whole chunk, so `kofam_ref/profiles` and `kofam_ref/ko_list.tsv` share
an md5 and would otherwise collapse to one identity.

**Pin granularity is chunk-level, and that over-invalidates.** Editing
`ko_list.tsv` moves the `kofam_ref` md5, which moves the id of `profiles` too.
That is a spurious cache *miss*, never a false hit, which is the safe direction;
a per-file md5 is recoverable from the `.dir` object in the DVC cache if the
over-invalidation ever costs more than it saves.

**An entry with no pin is not pinned.** It stays on `common.stage_ref`'s path
and takes the ordinary stat-derived id, correctly. Substituting a weaker id for
one that cannot be derived honestly is how a false cache hit gets built.

## The library on disk

`data/fabfos/refs.xgdb` is generated, never committed, and holds nothing but
`_metadata/`: its manifest names **absolute** paths, so the 24 GB stays where it
is and `PrepTransfer` (which queues `self.location`) never touches it. The
absolute paths are this checkout's and would be wrong anywhere else -- but the
*ids* inside are not, since they come from the pin and the relative path. That
is the payoff worth stating plainly: two hosts with different roots and
different index files still agree on every cache key.

## What pinning does and does not protect

`metasmith.models.libraries.pinned` owns that answer, and it is required reading
before trusting the stat stamp. The DVC-specific part lives here:

- `load_pinned_refs` compares each entry's recorded pin md5 against the current
  `.dvc` file before handing the library over. That is strictly stronger than
  the stat stamp, because it is the value the ids were minted from -- equality
  means the ids are still correct by construction.
- The common day-to-day drift is `dvc checkout` re-materialising the *same* pin,
  which moves mtime and changes nothing. With the md5 unchanged we `Restamp()`
  and continue, silently and without re-hashing. That is what keeps the stat
  stamp from being a nuisance nobody would leave switched on.
- A *changed* md5 means the ids are wrong. That raises, naming the entry and
  both md5s, and the fix is `fabfos refs pin` (instant, reads no data).
- **Nothing here changes a file mode.** DVC owns what is writable under
  `processed/`, so a `dvc checkout` or `dvc pull` needs no step from this side.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable

import yaml

from metasmith.models.libraries.pinned import PinnedLibraryError
from metasmith.python_api import DataInstanceLibrary
from metasmith.caching.keys import multihash_key

from .constants import RefPaths


REF_LAYOUT: dict[str, "str | tuple[str, ...]"] = {
    "ref::kofamscan_profiles": "kofam_ref/profiles",
    "ref::kofamscan_ko_list": "kofam_ref/ko_list.tsv",
    "ref::uniref50_diamond_db": "uniref50_dmnd/uniref50.dmnd",
    "ref::mnxr_lookup": "mnxr_lookup/mnxr_lookup.parquet",
    "ref::label_transfer_landmarks": "label_transfer_landmarks/landmarks",
    "ecspr::atom_pairs": "metabolism_bake/atom_pairs.parquet",
    "ecspr::direction_ratios": "metabolism_bake/direction.parquet",
    "ref::esm_c_600m_weights": "esm_c_weights/esmc_600m.tgz",
    "ref::label_transfer_landmarks_esmc": "label_transfer_landmarks_esmc/landmarks_esmc",
    "ref::ezpred_model": "ezpred_model/EZpred",
}

ANNOTATION_REFS = (
    "ref::kofamscan_profiles",
    "ref::kofamscan_ko_list",
    "ref::uniref50_diamond_db",
    "ref::mnxr_lookup",
    "ref::label_transfer_landmarks",
)
ECSPR_REFS = (
    "ecspr::atom_pairs",
    "ecspr::direction_ratios",
)

_TYPE_NAMESPACES = ("ref", "ecspr", "annotation", "sequences")


def relpaths_for(dtype: str) -> tuple[str, ...]:
    rel = REF_LAYOUT[dtype]
    return (rel,) if isinstance(rel, str) else tuple(rel)


def resolve_ref(refs_root: Path, dtype: str) -> Path | None:
    for rel in relpaths_for(dtype):
        p = Path(refs_root) / rel
        if p.exists():
            return p
    return None


def dvc_pin_for(refs_root: Path, rel: str) -> tuple[Path, str] | None:
    chunk = str(rel).split("/", 1)[0]
    pin = Path(refs_root) / f"{chunk}.dvc"
    if not pin.is_file():
        return None
    try:
        raw = yaml.safe_load(pin.read_text()) or {}
        outs = raw.get("outs") or []
        md5 = outs[0].get("md5") if outs else None
    except (OSError, yaml.YAMLError, AttributeError, IndexError):
        return None
    if not md5:
        return None
    return pin, str(md5)


def dvc_leaf_id(md5: str, rel: str) -> str:
    return multihash_key(b"dvc\x00" + md5.encode("utf-8") + rel.encode("utf-8")).hex()


#: Where a publish step records the identity its product ALREADY had.
#:
#: A reference is a transform product. Its `instance_id` is a real `origin:
#: lineage` id derived from the producing step's cache key -- and the publish
#: step throws it away, because it copies bytes and leaves the run's
#: `_metadata/index.yml` behind. What lands in `processed/` is an intermediate
#: that lost its lineage at the copy and got demoted to a leaf.
#:
#: This sidecar is the recording half. Where it has an entry, `pin_refs`
#: prefers it over the pin-derived id, and the reference's identity then moves
#: exactly when the build that produced it moves -- the rule every intermediate
#: already follows. The DVC-derived id is the backfill for everything published
#: before this existed: those runs happened on the cluster and left nothing
#: locally, so their provenance ids are not recoverable.
PROVENANCE_SIDECAR = "refs_provenance.yml"


def provenance_path(refs_root: Path) -> Path:
    return Path(refs_root) / PROVENANCE_SIDECAR


def read_published_provenance(refs_root: Path) -> dict[str, dict]:
    fp = provenance_path(refs_root)
    if not fp.is_file():
        return {}
    try:
        return yaml.safe_load(fp.read_text()) or {}
    except (OSError, yaml.YAMLError):
        return {}


def record_published_provenance(refs_root: Path, rel: str, *, instance_id: str,
                                origin: str = "lineage", run: str | None = None) -> None:
    fp = provenance_path(refs_root)
    data = read_published_provenance(refs_root)
    data[str(rel)] = {"instance_id": instance_id, "origin": origin, "run": run}
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(yaml.safe_dump(data, sort_keys=True))


def refs_library_path(refs_root: Path) -> Path:
    return RefPaths.xgdb(refs_root)


def _library_root() -> Path:
    return RefPaths.library_root()


def _library_root_or_why() -> str:
    # `--help` must answer even where no library is installed, which is exactly
    # where a reader most needs to be told the variable exists.
    try:
        return str(RefPaths.library_root())
    except FileNotFoundError:
        return "not found; set this"


def pin_refs(
    refs_root: Path,
    out: Path | None = None,
    *,
    types: Iterable[str] | None = None,
) -> dict:
    refs_root = Path(refs_root).expanduser().resolve()
    out = Path(out).expanduser().resolve() if out else refs_library_path(refs_root)
    wanted = tuple(types) if types is not None else tuple(REF_LAYOUT)

    lib_root = _library_root()
    if out.exists():
        try:
            existing = DataInstanceLibrary.Load(out)
            if existing.is_pinned:
                existing.Unpin()
        except (AssertionError, PinnedLibraryError, ValueError):
            pass
    lib = DataInstanceLibrary(out)
    lib.Purge()
    for ns in _TYPE_NAMESPACES:
        src = lib_root / "data_types" / f"{ns}.yml"
        if src.is_file():
            lib.AddTypeLibrary(src)

    added, skipped = {}, {}
    provenance: dict[Path, dict] = {}
    recorded = read_published_provenance(refs_root)
    for dtype in wanted:
        if dtype not in REF_LAYOUT:
            skipped[dtype] = "not in REF_LAYOUT"
            continue
        target = resolve_ref(refs_root, dtype)
        if target is None:
            skipped[dtype] = f"not present under {refs_root}"
            continue
        rel = str(target.relative_to(refs_root))
        pin = dvc_pin_for(refs_root, rel)
        if pin is None:
            skipped[dtype] = f"no .dvc pin covers [{rel}]"
            continue
        pin_path, md5 = pin
        published = recorded.get(rel)
        instance_id = published["instance_id"] if published else dvc_leaf_id(md5, rel)
        origin = published.get("origin", "lineage") if published else "leaf"
        try:
            lib.RegisterItem(target, dtype, instance_id=instance_id, origin=origin)
        except AssertionError as e:
            skipped[dtype] = str(e)
            continue
        provenance[target] = {
            "source": "lineage" if published else "dvc",
            "md5": md5,
            "pin": str(pin_path.relative_to(refs_root)),
            "rel": rel,
        }
        if published and published.get("run"):
            provenance[target]["run"] = published["run"]
        added[dtype] = str(target)

    report = lib.Pin(provenance=provenance)
    report.update(added=added, skipped=skipped, refs_root=str(refs_root))
    return report


def unpin_refs(refs_root: Path, out: Path | None = None) -> dict:
    out = Path(out).expanduser().resolve() if out else refs_library_path(Path(refs_root))
    lib = DataInstanceLibrary.Load(out)
    return lib.Unpin()


def load_pinned_refs(refs_root: Path, out: Path | None = None) -> DataInstanceLibrary | None:
    out = Path(out).expanduser().resolve() if out else refs_library_path(Path(refs_root))
    if not (out / "_metadata" / "index.yml").is_file():
        return None
    try:
        return DataInstanceLibrary.Load(out)
    except PinnedLibraryError as first:
        # A stamp moved. The pin md5 is the authority, so ask it before
        # bothering anyone: if the pins agree, the bytes the ids describe are
        # the bytes on disk and only mtime moved.
        lib = _load_without_stamp_check(out)
        if lib is None:
            raise
        bad = _pins_that_moved(Path(refs_root), lib)
        if bad:
            raise PinnedLibraryError(
                f"{first}\n\n  The DVC pins for these entries also changed, so the"
                " recorded ids are genuinely wrong:\n"
                + "\n".join(f"    [{k}] {v}" for k, v in bad.items())
                + "\n  Rebuild the reference library:  fabfos refs pin"
            ) from first
        lib.Restamp()
        print(
            "fabfos.refs: the references were re-materialised (stat stamps moved)"
            " but every DVC pin is unchanged, so the recorded ids are still"
            " correct. Re-stamped; no identity moved."
        )
        return lib


def _load_without_stamp_check(out: Path) -> DataInstanceLibrary | None:
    try:
        return DataInstanceLibrary.Load(out, check_pinned_stamps=False)
    except Exception:
        return None


def _pins_that_moved(refs_root: Path, lib: DataInstanceLibrary) -> dict[str, str]:
    moved: dict[str, str] = {}
    for name, entry in (lib._pinned or {}).get("entries", {}).items():
        prov = entry.get("provenance")
        if not prov or prov.get("source") != "dvc":
            continue
        now = dvc_pin_for(refs_root, prov.get("rel", ""))
        if now is None:
            moved[name] = f"the pin [{prov.get('pin')}] is gone"
        elif now[1] != prov.get("md5"):
            moved[name] = f"md5 {prov.get('md5')} -> {now[1]}"
    return moved


def refs_view(lib: DataInstanceLibrary, keep_types: Iterable[str]):
    keep = set(keep_types)
    hide = {p for p, dtype in lib.manifest.items() if dtype not in keep}
    if not hide:
        return lib
    return lib.AsView(hide, invert=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="fabfos refs", description=__doc__,
        epilog=(
            "environment (fabfos.constants.RefPaths owns all three):\n"
            f"  FABFOS_REFS_ROOT   the `processed/` root  [{RefPaths.REFS_ROOT}]\n"
            f"  FABFOS_REFS_XGDB   the pinned library     [{RefPaths.xgdb(RefPaths.REFS_ROOT)}]\n"
            f"  FABFOS_LIBRARY     the transform library  [{_library_root_or_why()}]\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="verb", required=True)
    for verb, helptext in (
        ("pin", "build the pinned reference library (reads no data)"),
        ("unpin", "lift the pin so the library can be rebuilt"),
        ("inspect", "show what the pinned library records"),
    ):
        s = sub.add_parser(verb, help=helptext)
        s.add_argument("--refs-root", default=str(RefPaths.REFS_ROOT),
                       help="the `processed/` root to pin"
                            f" (default: $FABFOS_REFS_ROOT, else {RefPaths.DEFAULT_REFS_ROOT})")
        s.add_argument("--out", default=None,
                       help="the pinned library to write"
                            " (default: $FABFOS_REFS_XGDB, else refs.xgdb beside --refs-root)")
    args = ap.parse_args(argv)

    root = Path(args.refs_root)
    if args.verb == "pin":
        report = pin_refs(root, args.out)
        for dtype, path in sorted(report["added"].items()):
            print(f"  pinned  {dtype:34s} {path}")
        for dtype, why in sorted(report["skipped"].items()):
            print(f"  skipped {dtype:34s} {why}")
        print(f"\n{report['pinned']} entries at {report['location']}")
        return 0
    if args.verb == "unpin":
        report = unpin_refs(root, args.out)
        print(f"unpinned {report['unpinned']} entries at {report['location']}")
        return 0
    lib = load_pinned_refs(root, args.out)
    if lib is None:
        print("no pinned reference library; run `fabfos refs pin`")
        return 1
    for path, dtype in sorted(lib.manifest.items(), key=lambda t: str(t[0])):
        print(f"  {dtype:34s} {lib.instance_meta[path]['instance_id'][:24]}…  {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
