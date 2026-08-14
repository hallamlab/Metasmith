"""The method version: what FabFos *is*, as opposed to what it is running.

`version.txt` versions the CLI package. This versions the **method** -- the
composition of canon, the transform library, the container digests, the type
contract and the data library that together decide what a number coming out of
this pipeline means. The two move independently: a CLI bugfix is not a new
method, and repinning the engine library is a new method even if no fabfos
source changed.

The shape is metasmith's (`metasmith/constants.py`): a bare version file that
a human bumps, a content hash stamped alongside it, and a full version string
combining the two. The difference is *what* is hashed. Metasmith hashes its
source tree, because metasmith is the source tree. A method is not a tree --
it is a composition -- so what is hashed here is the ordered, canonically
serialized set of things that would change an answer:

  1. canon's content, and its declared STATUS
  2. the transform library's commit, and whether it was dirty
  3. metasmith's own FULL_VERSION (already version+build_hash)
  4. every container name -> digest, sorted
  5. the sha256 of the data library's index (the index, not 46 GB of bytes --
     the index already carries a per-item sha256, so hashing it transitively
     covers the data)
  6. the type contract -- one library per namespace, since a type's property
     set is what the planner matches on
  7. the planner's domain list, because the candidate space is part of the
     method

A run against a BUNDLED library (`src/fabfos/_library`, what `dev.sh -b` copies
and a wheel ships) hashes to a different id than the same library resolved from
the submodule, because `bundled` is part of the document. That is deliberate and
it under-claims: bundling copies without stamping, so nothing here can prove the
copy matches its source, and two ids that differ when the method did not is the
safe direction for the error to run.

Deliberately NOT hashed: user inputs, output paths, thread counts, runtime
choice, wall-clock. Those belong to a *run*. A run is described by the manifest
this module also writes; a method is described by the id.

Hashing canon by content means editing one of its comments bumps the method
id. That is accepted on purpose: canon's prose is not decoration, it is where
the method's decisions are written down, and a reader who changes what it says
has changed what the pipeline claims. Better a spurious bump than a silent one.

Stamping REFUSES while any container record is unresolved. A method id that
covers an image nobody else can pull is a promise the method cannot keep.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_MODULE = Path(__file__).resolve().parent
_REPO = _MODULE.parent.parent

METHOD_VERSION_FILE = _MODULE / "method_version.txt"
METHOD_VERSION = METHOD_VERSION_FILE.read_text().strip()


class MethodError(RuntimeError):
    """Raised when the method cannot be described or stamped."""


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _git(repo: Path, *args: str) -> str | None:
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True, text=True, timeout=30,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


@dataclass
class MethodDescription:
    """The hashed composition, plus the id derived from it."""

    version: str
    components: dict[str, Any] = field(default_factory=dict)
    unresolved_containers: list[str] = field(default_factory=list)

    def canonical_document(self) -> str:
        # sort_keys + explicit separators: the document must serialize
        # identically on every machine or the id is not comparable.
        return json.dumps(self.components, sort_keys=True, separators=(",", ":"))

    @property
    def method_hash(self) -> str:
        return hashlib.sha256(self.canonical_document().encode()).hexdigest()

    @property
    def method_id(self) -> str:
        return f"{self.version}+{self.method_hash[:7]}"

    def require_stampable(self) -> None:
        if self.unresolved_containers:
            raise MethodError(
                "refusing to stamp a method id while these containers are "
                f"unresolved: {', '.join(sorted(self.unresolved_containers))}. "
                "An id that covers an image nobody else can pull is a promise "
                "the method cannot keep. Resolve them, or record a blocker and "
                "do not claim a method version."
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "method_version": self.version,
            "method_hash": self.method_hash,
            "method_id": self.method_id,
            "components": self.components,
            "unresolved_containers": sorted(self.unresolved_containers),
        }


# The containers the reads -> significance path actually invokes. Stampability
# is gated on THESE, not on all 44 records: the library carries images for
# lanes this method never enters (stringtie for transcriptomics, sra-tools for
# fetching, deeptfactor/predictf as terminal annotation side-branches), and
# blocking a method version on an image it does not run would be a false claim
# in the other direction -- it would say the method is broken when it is not.
# Every digest is still HASHED into the id; only the refusal is scoped.
METHOD_PATH_CONTAINERS = frozenset({
    "ecspr",                      # the numeric core: solve, ablation, scorer
    "clean",                      # the CLEAN EC-annotation lane
    "diamond",                    # uniref lane
    "blast",                      # contig clustering
    "python_for_data_science",    # clustering / pool-size helpers
    "minimap2", "samtools", "bedtools",   # pool coverage
})


def _container_digests(repo: Path) -> tuple[dict[str, str], list[str]]:
    """Return (all digests, unresolved records that are ON the method path)."""
    import yaml

    records = sorted((repo / "provenance" / "containers").glob("*.yml"))
    digests: dict[str, str] = {}
    unresolved: list[str] = []
    for rec_path in records:
        rec = yaml.safe_load(rec_path.open()) or {}
        if rec.get("resolved") and str(rec.get("digest", "")).startswith("sha256:"):
            digests[rec_path.stem] = rec["digest"]
        elif rec_path.stem in METHOD_PATH_CONTAINERS:
            unresolved.append(rec_path.stem)
    return digests, unresolved


def describe_method(repo: Path | None = None) -> MethodDescription:
    """Assemble the hashed composition. Never raises on an unresolved container
    -- it records them, so `--describe-method` still works while the method is
    not yet stampable. Stamping is what refuses."""
    repo = Path(repo) if repo is not None else _REPO
    components: dict[str, Any] = {}

    # 1. canon: content + declared status. Still hashed while it is
    #    `_deprecated_canon` -- the live consumers have not moved off it, so it
    #    still decides answers, and a component stops being hashed when nothing
    #    reads it, not when it is renamed.
    canon_py = _MODULE / "_deprecated_canon.py"
    components["canon"] = {"sha256": _sha256_file(canon_py)}
    try:
        from . import _deprecated_canon as _canon  # noqa: WPS433

        for attr in ("STATUS", "STATUS_SINCE"):
            if hasattr(_canon, attr):
                components["canon"][attr.lower()] = str(getattr(_canon, attr))
    except Exception as e:  # canon must not be able to break `--describe-method`
        components["canon"]["import_error"] = f"{type(e).__name__}: {e}"

    # 2. the transform library: commit, and whether it was dirty. A dirty
    #    library is recorded, never silently ignored -- it means the method
    #    being run is not the method the commit names.
    try:
        from .pipelines.common import resolve_library_root
        from .pipelines import DOMAINS

        lib_root = resolve_library_root()
        # `git -C <path>` walks UP to the nearest repo, so asking a bundled
        # copy for its commit answers with whatever repo CONTAINS it -- for
        # `src/fabfos/_library` that is the superproject, and the method then
        # claims the FRONT END's commit as the library's. Silently: both are
        # plausible-looking sha1s. Ask only a path that is its own repo root.
        def _repo_root_of(p: Path) -> "Path | None":
            top = _git(p, "rev-parse", "--show-toplevel")
            return p if top == str(p) else None

        # A bundle is a copy, so its commit is the SOURCE's, not necessarily
        # the copy's -- `dev.sh -b` copies and does not stamp. The bundle's own
        # content still reaches the id through `type_contract` below.
        bundled = _repo_root_of(lib_root) is None
        lib_repo = _repo_root_of(_REPO / "src" / "metasmith_libraries") if bundled \
            else lib_root
        components["transform_library"] = {
            "bundled": bundled,
            "source_commit": _git(lib_repo, "rev-parse", "HEAD") if lib_repo else None,
            "dirty": bool(_git(lib_repo, "status", "--porcelain")) if lib_repo else None,
            "domains": sorted(DOMAINS),
        }
        # 6. the type contract -- one library per namespace
        contract = {}
        for ns_file in sorted((lib_root / "data_types").glob("*.yml")):
            contract[ns_file.stem] = _sha256_file(ns_file)
        components["type_contract"] = contract
    except Exception as e:
        components["transform_library"] = {"error": f"{type(e).__name__}: {e}"}

    # 3. metasmith's own full version
    try:
        from metasmith.constants import FULL_VERSION  # type: ignore

        components["metasmith"] = FULL_VERSION
    except Exception as e:
        components["metasmith"] = f"unavailable: {type(e).__name__}"

    # 4. container digests
    digests, unresolved = _container_digests(repo)
    components["containers"] = digests

    # 5. the data library index. Hashing the index rather than the payload is
    #    deliberate: the index already carries a sha256 per item, so this is a
    #    transitive pin over ~46 GB for the cost of one file read.
    try:
        index = repo / ".awm" / "data" / "ref" / "_metadata" / "index.yml"
        components["data_library"] = (
            {"index_sha256": _sha256_file(index)} if index.exists()
            else {"index": "absent"}
        )
    except Exception as e:
        components["data_library"] = {"error": f"{type(e).__name__}: {e}"}

    return MethodDescription(
        version=METHOD_VERSION,
        components=components,
        unresolved_containers=unresolved,
    )


def method_id(repo: Path | None = None) -> str:
    return describe_method(repo).method_id


def write_method_document(out_dir: Path, repo: Path | None = None) -> Path:
    """Write method.yml into a run's output directory, at the START of a run.

    The full document, not just the id: a bare hash mismatch tells a reader
    nothing, whereas a diffable document tells them it was the ecspr container
    digest that moved.
    """
    import yaml

    desc = describe_method(repo)
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "method.yml"
    with open(p, "w") as f:
        yaml.safe_dump(desc.to_dict(), f, sort_keys=False, default_flow_style=False)
    return p


def check_required(required: str, repo: Path | None = None) -> None:
    """Hard-fail unless the live method matches `required`.

    Without this the version is decorative. Accepts either the full id
    (`0.3.0+a1b2c3d`) or the bare version (`0.3.0`).
    """
    desc = describe_method(repo)
    actual = desc.method_id
    if required == actual or required == desc.version:
        return
    raise MethodError(
        f"method mismatch: required [{required}], running [{actual}]. "
        "Run `fabfos --describe-method` on both sides and diff the documents -- "
        "the component that differs is the one that changed."
    )
