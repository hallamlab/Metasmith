from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_MODULE = Path(__file__).resolve().parent
_REPO = _MODULE.parent.parent

METHOD_VERSION_FILE = _MODULE / "method_version.txt"
METHOD_VERSION = METHOD_VERSION_FILE.read_text().strip()


class MethodError(RuntimeError):
    pass
def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class MethodDescription:
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
    "ecspr",
    "clean",
    "diamond",
    "blast",
    "python_for_data_science",
    "minimap2", "samtools", "bedtools",
})


def _container_digests(repo: Path) -> tuple[dict[str, str], list[str]]:
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
    repo = Path(repo) if repo is not None else _REPO
    components: dict[str, Any] = {}

    canon_py = _MODULE / "_deprecated_canon.py"
    components["canon"] = {"sha256": _sha256_file(canon_py)}
    try:
        from . import _deprecated_canon as _canon  # noqa: WPS433

        for attr in ("STATUS", "STATUS_SINCE"):
            if hasattr(_canon, attr):
                components["canon"][attr.lower()] = str(getattr(_canon, attr))
    except Exception as e:
        components["canon"]["import_error"] = f"{type(e).__name__}: {e}"

    try:
        from metasmith._build_hash import compute_build_hash
        from .pipelines.common import resolve_library_root
        from .pipelines import DOMAINS

        lib_root = resolve_library_root()
        bundled = lib_root == _MODULE / "_library"
        components["transform_library"] = {
            "bundled": bundled,
            "content_hash": compute_build_hash(lib_root),
            "domains": sorted(DOMAINS),
        }
        contract = {}
        for ns_file in sorted((lib_root / "data_types").glob("*.yml")):
            contract[ns_file.stem] = _sha256_file(ns_file)
        components["type_contract"] = contract
    except Exception as e:
        components["transform_library"] = {"error": f"{type(e).__name__}: {e}"}

    try:
        from metasmith.constants import FULL_VERSION  # type: ignore

        components["metasmith"] = FULL_VERSION
    except Exception as e:
        components["metasmith"] = f"unavailable: {type(e).__name__}"

    digests, unresolved = _container_digests(repo)
    components["containers"] = digests

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
    import yaml

    desc = describe_method(repo)
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "method.yml"
    with open(p, "w") as f:
        yaml.safe_dump(desc.to_dict(), f, sort_keys=False, default_flow_style=False)
    return p


def check_required(required: str, repo: Path | None = None) -> None:
    desc = describe_method(repo)
    actual = desc.method_id
    if required == actual or required == desc.version:
        return
    raise MethodError(
        f"method mismatch: required [{required}], running [{actual}]. "
        "Run `fabfos --describe-method` on both sides and diff the documents -- "
        "the component that differs is the one that changed."
    )
