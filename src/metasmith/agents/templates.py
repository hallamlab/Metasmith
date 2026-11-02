from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from ..constants import MODULE_PATH
from .spec import Spec

TEMPLATES_DIR = "templates"
VENDOR_DIRNAME = "vendor"
TEMPLATE_FILE = "spec.yml"

DATA_TYPES_DIRNAME = "data_types"
TRANSFORMS_DIRNAME = "transforms"
RESOURCES_DIRNAME = "resources"


def standard_library_root() -> Path | None:
    # Where the shipped standard library is, for every consumer that needs it:
    # `gui.stdlib` copies it into a project, `agents.conda` reads its `envs/`
    # recipes. Two rungs, in this order.
    #
    # 1. The bundle vendored inside this package. An installed metasmith always
    #    has one, whatever it was installed by -- which is the point: conda's
    #    solve was the only thing that ever delivered a separate library
    #    package, so a wheel, a `pip install` and the relay-free runtimes had no
    #    mechanism at all.
    # 2. An importable `metasmith_libraries`, which is what a source checkout on
    #    PYTHONPATH has and a vendored install never needs.
    vendored = MODULE_PATH/VENDOR_DIRNAME
    if (vendored/DATA_TYPES_DIRNAME).is_dir(): return vendored
    import importlib.util
    try:
        spec = importlib.util.find_spec("metasmith_libraries")
    except (ImportError, ValueError):
        return None
    if spec is None or not spec.origin: return None
    root = Path(spec.origin).resolve().parent
    return root if (root/DATA_TYPES_DIRNAME).is_dir() else None


def library_index(root: Path | str) -> dict:
    root = Path(root)

    def dirs(path: Path) -> list[str]:
        if not path.is_dir(): return []
        return sorted(
            str(p) for p in path.iterdir()
            if p.is_dir() and not p.name.startswith((".", "__"))
        )

    types_dir = root / DATA_TYPES_DIRNAME
    return {
        "data_types": sorted(str(p) for p in types_dir.glob("*.yml")) if types_dir.is_dir() else [],
        "transform_libraries": dirs(root / TRANSFORMS_DIRNAME),
        "resource_libraries": dirs(root / RESOURCES_DIRNAME),
    }


def _namespaces_used(inline: dict) -> set[str]:
    out: set[str] = set()
    for entry in (inline.get("manifest") or {}).values():
        if not isinstance(entry, dict): continue
        names = [entry.get("type"), *(entry.get("parents") or {}).values()]
        for name in names:
            if isinstance(name, str) and "::" in name:
                out.add(name.split("::", 1)[0])
    return out


def _resolve_names(raw: dict, index: dict, root: Path) -> tuple[dict, list[str]]:
    unresolved: list[str] = []

    def present(ref: str) -> bool:
        p = Path(ref)
        return (p if p.is_absolute() else root / p).exists()

    out = dict(raw)
    lib = raw.get("input_library")
    if isinstance(lib, dict):
        by_stem = {Path(p).stem: str(p) for p in index.get("data_types") or []}
        types = dict(lib.get("types") or {})
        for ns in sorted(_namespaces_used(lib)):
            found = by_stem.get(ns)
            if found is not None:
                types[ns] = found
            elif ns not in types or not present(types[ns]):
                unresolved.append(f"type namespace [{ns}]")
        if types:
            out["input_library"] = lib | {"types": types}

    for key, label in (
        ("transform_libraries", "transform library"),
        ("resource_libraries", "resource library"),
    ):
        by_name = {Path(p).name: str(p) for p in index.get(key) or []}
        resolved = []
        for ref in raw.get(key) or []:
            found = by_name.get(Path(ref).name)
            if found is not None:
                resolved.append(found)
                continue
            resolved.append(ref)
            if not present(ref):
                unresolved.append(f"{label} [{Path(ref).name}]")
        out[key] = resolved
    return out, unresolved


@dataclass
class Template:
    name: str
    spec: Spec
    description: str = ""
    root: Path | None = None
    unresolved: list[str] = field(default_factory=list)


    @staticmethod
    def PathIn(root: Path | str, name: str, dirname: str = TEMPLATES_DIR) -> Path:
        return Path(root) / dirname / name / TEMPLATE_FILE

    def Save(self, root: Path | str, dirname: str = TEMPLATES_DIR) -> Path:
        root = Path(root).resolve()
        packed = self.spec.Pack(relative_to=root)

        lib = packed["input_library"]
        assert isinstance(lib, dict), (
            f"template [{self.name}] would ship a reference to an input library "
            f"directory [{lib}] instead of the library itself; a template's inputs "
            f"live inline in its own spec.yml"
        )
        packed["input_library"] = {k: v for k, v in lib.items() if k != "types"}
        for key in ("transform_libraries", "resource_libraries"):
            packed[key] = [Path(v).name for v in packed[key]]

        out = self.PathIn(root, self.name, dirname=dirname)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            yaml.safe_dump(
                {"name": self.name, "description": self.description} | packed,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        return out

    @classmethod
    def Load(
        cls, path: Path | str,
        root: Path | str | None = None,
        libraries: dict | None = None,
    ) -> "Template":
        path = Path(path).resolve()
        root = Path(root).resolve() if root is not None else path.parent.parent.parent
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        index = library_index(root) if libraries is None else libraries
        raw, unresolved = _resolve_names(raw, index, root)
        return cls(
            name=raw.get("name") or path.parent.name,
            description=raw.get("description") or "",
            spec=Spec.Unpack(raw, root=root),
            root=root,
            unresolved=unresolved,
        )

    @classmethod
    def Discover(
        cls, root: Path | str,
        dirname: str = TEMPLATES_DIR,
        libraries: dict | None = None,
    ) -> list["Template"]:
        root = Path(root).resolve()
        index = library_index(root) if libraries is None else libraries
        found = [
            cls.Load(p, root=root, libraries=index)
            for p in sorted((root / dirname).glob(f"*/{TEMPLATE_FILE}"))
        ]
        return sorted(found, key=lambda t: t.name)


    def Pack(self) -> dict:
        return {"name": self.name, "description": self.description} | self.spec.Pack(
            relative_to=self.root
        )
