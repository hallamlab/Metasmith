"""The reference databases, identified once instead of re-hashed every plan.

A reference here is 17 GB of diamond database, 7.2 GB of kofam profiles, an
embedding stack. `DataInstanceLibrary.AddItem` derives a leaf's identity from
the file's bytes, so registering these cost ~10 seconds of blake3 on every plan
against a solve that takes 1 -- and it bought nothing, because the answer is the
same every time. Two of the five are *directories*, which `_mint_leaf_id` cannot
content-address at all, so they were getting a fresh `uuid4` per plan and the
annotation lane's task key was already non-deterministic on one machine.

Every one of these chunks is DVC-pinned, and a `.dvc` file records an md5 that
DVC computed over the real bytes. That is a better identity than anything this
code could derive: cheaper (it is already written down), stronger for a
directory (DVC hashes the tree, we cannot), and agreed on by every host that
checks out the same pin. So an id here is

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

**An entry with no pin is not frozen.** It stays on `common.stage_ref`'s path
and pays the hash, correctly. Substituting a weaker id for one that cannot be
derived honestly is how a false cache hit gets built.

## The library on disk

`data/fabfos/refs.xgdb` is generated, never committed, and holds nothing but
`_metadata/`: its manifest names **absolute** paths, so the 24 GB stays where it
is and `PrepTransfer` (which queues `self.location`) never touches it. The
absolute paths are this checkout's and would be wrong anywhere else -- but the
*ids* inside are not, since they come from the pin and the relative path. That
is the payoff worth stating plainly: two hosts with different roots and
different index files still agree on every cache key.

## What freezing does and does not protect

`metasmith.models.libraries.frozen` owns that answer, and it is required reading
before trusting either the read-only mark or the stat stamp. The DVC-specific
part lives here:

- `load_frozen_refs` compares each entry's recorded pin md5 against the current
  `.dvc` file before handing the library over. That is strictly stronger than
  the stat stamp, because it is the value the ids were minted from -- equality
  means the ids are still correct by construction.
- The common day-to-day drift is `dvc checkout` re-materialising the *same* pin,
  which moves mtime and changes nothing. With the md5 unchanged we `Restamp()`
  and continue, silently and without re-hashing. That is what keeps the stat
  stamp from being a nuisance nobody would leave switched on.
- A *changed* md5 means the ids are wrong. That raises, naming the entry and
  both md5s, and the fix is `fabfos refs freeze` (instant, reads no data).
- **Freezing marks the entries read-only, which can make a later `dvc
  checkout`/`dvc pull` fail with EACCES.** Run `fabfos refs unfreeze` first. The
  failure is loud, which is the acceptable half of that trade.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable

import yaml

from metasmith.models.libraries.frozen import FrozenLibraryError
from metasmith.python_api import DataInstanceLibrary
from metasmith.caching.keys import multihash_key


#: type -> path relative to a `processed/` root. THE table: the pipelines, the
#: freeze step and the research drivers all read this one rather than restating
#: it, because a drift between two copies mis-keys an entry rather than failing.
#: A tuple means alternates, first existing wins -- the ESM-C pool is published
#: as `pool_esmc` on the fir mirror and `pool` locally.
REF_LAYOUT: dict[str, "str | tuple[str, ...]"] = {
    # `ref::reference_label_pool` is a DIRECTORY (index + embedding stack),
    # which is why it is one product: the consumer addresses the stack by row,
    # so an index from one build against a stack from another misindexes every
    # row silently.
    "ref::kofamscan_profiles": "kofam_ref/profiles",
    "ref::kofamscan_ko_list": "kofam_ref/ko_list.tsv",
    "ref::uniref50_diamond_db": "uniref50_dmnd/uniref50.dmnd",
    "ref::mnxr_lookup": "mnxr_lookup/mnxr_lookup.parquet",
    "ref::reference_label_pool": "reference_label_pool/pool",
    "ecspr::atom_pairs": "metabolism_bake/atom_pairs.parquet",
    "ecspr::direction_ratios": "metabolism_bake/direction.parquet",
    "ref::esm_c_600m_weights": "esm_c_weights/esmc_600m.tgz",
    "ref::reference_label_pool_esmc": ("reference_label_pool_esmc/pool_esmc",
                                       "reference_label_pool_esmc/pool"),
    "ref::ezpred_model": "ezpred_model/EZpred",
}

#: What each lane registers. Kept as explicit tuples rather than derived from a
#: namespace prefix: `ecspr::` and `ref::` do not line up with the lanes.
ANNOTATION_REFS = (
    "ref::kofamscan_profiles",
    "ref::kofamscan_ko_list",
    "ref::uniref50_diamond_db",
    "ref::mnxr_lookup",
    "ref::reference_label_pool",
)
ECSPR_REFS = (
    "ecspr::atom_pairs",
    "ecspr::direction_ratios",
)

#: The type namespaces a frozen refs library has to carry so its rows resolve.
_TYPE_NAMESPACES = ("ref", "ecspr", "annotation", "sequences")


def relpaths_for(dtype: str) -> tuple[str, ...]:
    """The candidate relative paths for a type, in preference order."""
    rel = REF_LAYOUT[dtype]
    return (rel,) if isinstance(rel, str) else tuple(rel)


def resolve_ref(refs_root: Path, dtype: str) -> Path | None:
    """The first candidate that exists under `refs_root`, or None."""
    for rel in relpaths_for(dtype):
        p = Path(refs_root) / rel
        if p.exists():
            return p
    return None


def dvc_pin_for(refs_root: Path, rel: str) -> tuple[Path, str] | None:
    """`(pin_path, md5)` for the chunk `rel` belongs to, or None if unpinned."""
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
    """The identity a DVC-pinned reference gets.

    Pinned by a test against a literal: changing this construction silently
    re-keys every cached run that touched a reference.
    """
    return multihash_key(b"dvc\x00" + md5.encode("utf-8") + rel.encode("utf-8")).hex()


#: Where a publish step records the identity its product ALREADY had.
#:
#: A reference is a transform product. Its `instance_id` is a real `origin:
#: lineage` id derived from the producing step's cache key -- and the publish
#: step throws it away, because it copies bytes and leaves the run's
#: `_metadata/index.yml` behind. What lands in `processed/` is an intermediate
#: that lost its lineage at the copy and got demoted to a leaf.
#:
#: This sidecar is the recording half. Where it has an entry, `freeze_refs`
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
    """Note that the file at `rel` is the product with this identity.

    Called by a publish step, keyed by the path relative to `processed/` so it
    survives the copy that loses the run directory. Additive and idempotent:
    re-publishing the same product rewrites its own row and touches no other.
    """
    fp = provenance_path(refs_root)
    data = read_published_provenance(refs_root)
    data[str(rel)] = {"instance_id": instance_id, "origin": origin, "run": run}
    fp.parent.mkdir(parents=True, exist_ok=True)
    fp.write_text(yaml.safe_dump(data, sort_keys=True))


def refs_library_path(refs_root: Path) -> Path:
    """Where the frozen library for `refs_root` lives.

    Beside the data rather than inside it: the manifest holds absolute paths, so
    putting the library *at* `processed/` with relative entries would look
    tidier and would stage 24 GB on every run.
    """
    override = os.environ.get("FABFOS_REFS_XGDB")
    if override:
        return Path(override).expanduser().resolve()
    return Path(refs_root).resolve().parent / "refs.xgdb"


def _library_root() -> Path:
    from .pipelines import common
    return common.resolve_library_root()


def freeze_refs(
    refs_root: Path,
    out: Path | None = None,
    *,
    types: Iterable[str] | None = None,
    permissions: bool = True,
) -> dict:
    """Build the frozen reference library. Reads no reference bytes."""
    refs_root = Path(refs_root).expanduser().resolve()
    out = Path(out).expanduser().resolve() if out else refs_library_path(refs_root)
    wanted = tuple(types) if types is not None else tuple(REF_LAYOUT)

    lib_root = _library_root()
    if out.exists():
        # An existing frozen library refuses Purge, correctly. Rebuilding one is
        # a legitimate act, so lift the freeze first rather than working around
        # it -- which also restores write on the entries it marked.
        try:
            existing = DataInstanceLibrary.Load(out)
            if existing.is_frozen:
                existing.Unfreeze()
        except (AssertionError, FrozenLibraryError, ValueError):
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
            # Deliberately not falling back to a content hash or a path-derived
            # id: an id nobody can re-derive on another host is worse than no
            # entry, because the entry would look authoritative.
            skipped[dtype] = f"no .dvc pin covers [{rel}]"
            continue
        pin_path, md5 = pin
        # A recorded provenance id is strictly better than one derived from the
        # pin: it is the identity the product actually had when it was made, so
        # it moves when the build moves rather than when the bytes are
        # re-materialised. The pin stays recorded either way -- `load_frozen_refs`
        # uses it to tell a re-materialisation from a real change.
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

    report = lib.Freeze(apply_permissions=permissions, provenance=provenance)
    report.update(added=added, skipped=skipped, refs_root=str(refs_root))
    return report


def unfreeze_refs(refs_root: Path, out: Path | None = None) -> dict:
    """Lift the freeze so `dvc checkout`/`dvc pull` can write again."""
    out = Path(out).expanduser().resolve() if out else refs_library_path(Path(refs_root))
    lib = DataInstanceLibrary.Load(out)
    return lib.Unfreeze()


def load_frozen_refs(refs_root: Path, out: Path | None = None) -> DataInstanceLibrary | None:
    """Load the frozen library, self-healing a re-materialised pin.

    Returns None when there is no frozen library, which is the un-migrated
    checkout: the caller warns once and falls back to staging references the
    slow way, so nothing hard-breaks on an upgrade.
    """
    out = Path(out).expanduser().resolve() if out else refs_library_path(Path(refs_root))
    if not (out / "_metadata" / "index.yml").is_file():
        return None
    try:
        return DataInstanceLibrary.Load(out)
    except FrozenLibraryError as first:
        # A stamp moved. The pin md5 is the authority, so ask it before
        # bothering anyone: if the pins agree, the bytes the ids describe are
        # the bytes on disk and only mtime moved.
        lib = _load_without_stamp_check(out)
        if lib is None:
            raise
        bad = _pins_that_moved(Path(refs_root), lib)
        if bad:
            raise FrozenLibraryError(
                f"{first}\n\n  The DVC pins for these entries also changed, so the"
                " recorded ids are genuinely wrong:\n"
                + "\n".join(f"    [{k}] {v}" for k, v in bad.items())
                + "\n  Rebuild the reference library:  fabfos refs freeze"
            ) from first
        lib.Restamp()
        print(
            "fabfos.refs: the references were re-materialised (stat stamps moved)"
            " but every DVC pin is unchanged, so the recorded ids are still"
            " correct. Re-stamped; no identity moved."
        )
        return lib


def _load_without_stamp_check(out: Path) -> DataInstanceLibrary | None:
    """`Load` with the stamp check suppressed, to adjudicate one failure.

    The same door `metasmith data verify` uses, and for the same reason: the
    result is immediately checked against the pin md5, which is strictly
    stronger. This is not the env kill switch -- that is a human's emergency
    exit and stays one.
    """
    try:
        return DataInstanceLibrary.Load(out, check_frozen_stamps=False)
    except Exception:
        return None


def _pins_that_moved(refs_root: Path, lib: DataInstanceLibrary) -> dict[str, str]:
    moved: dict[str, str] = {}
    for name, entry in (lib._frozen or {}).get("entries", {}).items():
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
    """The library narrowed to exactly the rows a lane should see.

    Two reasons this is a whitelist rather than "everything minus the
    overrides". A reference the caller overrode must be masked or the solver
    sees two candidates of that type -- the frozen default and the override --
    and picks between them arbitrarily. And the library holds every reference
    this checkout has, not just this lane's: handing the annotation lane an
    ESM-C weights row it would not otherwise have makes a transform reachable
    that was not, which changes the plan. A lane gets what it asked for.
    """
    keep = set(keep_types)
    hide = {p for p, dtype in lib.manifest.items() if dtype not in keep}
    if not hide:
        return lib
    return lib.AsView(hide, invert=True)


def _main(argv: list[str] | None = None) -> int:
    from .pipelines import common

    ap = argparse.ArgumentParser(prog="fabfos refs", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="verb", required=True)
    for verb, helptext in (
        ("freeze", "build the frozen reference library (reads no data)"),
        ("unfreeze", "lift the freeze so dvc can write again"),
        ("inspect", "show what the frozen library records"),
    ):
        s = sub.add_parser(verb, help=helptext)
        s.add_argument("--refs-root", default=str(common.DATA_PROCESSED))
        s.add_argument("--out", default=None)
        if verb == "freeze":
            s.add_argument("--no-permissions", action="store_true",
                           help="skip the read-only mark; the stamps are still recorded")
    args = ap.parse_args(argv)

    root = Path(args.refs_root)
    if args.verb == "freeze":
        report = freeze_refs(root, args.out, permissions=not args.no_permissions)
        for dtype, path in sorted(report["added"].items()):
            print(f"  frozen  {dtype:34s} {path}")
        for dtype, why in sorted(report["skipped"].items()):
            print(f"  skipped {dtype:34s} {why}")
        for line in report.get("chmod_failed", []):
            print(f"  chmod   {line}")
        print(f"\n{report['frozen']} entries at {report['location']}")
        print("Run `fabfos refs unfreeze` before any dvc checkout/pull of these chunks.")
        return 0
    if args.verb == "unfreeze":
        report = unfreeze_refs(root, args.out)
        print(f"unfroze {report['unfrozen']} entries at {report['location']}")
        return 0
    lib = load_frozen_refs(root, args.out)
    if lib is None:
        print("no frozen reference library; run `fabfos refs freeze`")
        return 1
    for path, dtype in sorted(lib.manifest.items(), key=lambda t: str(t[0])):
        print(f"  {dtype:34s} {lib.instance_meta[path]['instance_id'][:24]}…  {path}")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
