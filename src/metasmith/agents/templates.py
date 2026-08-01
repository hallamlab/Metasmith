"""A workflow you can start from.

A template is a `Spec` whose inputs are `DEFERRED` -- the same object a stored
workflow is, so there is no template format to design, version or validate
separately. What a template adds is only where it lives and what it is called:
one file in a library repository, holding the spec, a name and a blurb.

    <root>/templates/<name>/spec.yml

The deferred rows -- typed, with lineage -- live inline in that same file
(`Spec.Pack`'s `input_library`, via `DataInstanceLibrary.PackInline`) rather
than as a sibling directory: a template's input library never leaves the repo
it ships in, so the self-contained, copy-everything format `Save`/`Load` use
for a real run's data -- built for shipping over ssh/globus to another host --
has nothing here to do.

Shipping them inside the library repository is what makes versioning free: a
template arrives in the same commit as the transforms it names and cannot be
older than the library it was found in.

Two properties are enforced here rather than left to whoever writes one:

* **A template holds names, not locations.** `Save` writes a type namespace as
  the `ns::type` strings its manifest already carries and a transform/resource
  library as a bare directory name; `Load` resolves those names against the
  library repository it is being read in. Neither half is optional: a path
  written into a template is one machine's checkout, and its failure is quiet
  -- the spec loads and the solve reports an unknown type, with nothing to say
  a path came along for the ride. Resolving at load rather than at each point
  of use is what keeps one code path: after `Load`, a template saved by a user
  and one shipped by a library are the same object.
* **The spec is what ships, not a picture of it.** Rendering a DAG while
  authoring is a debugging aid; a rendered SVG in the repository would be a
  drawing kept fresh by a build that never looks at it, in whatever theme the
  author happened to have. The GUI renders from the spec, in its own theme.

A name the repository cannot account for is recorded on the loaded template
(`unresolved`) rather than raised: listing what a project offers must not be
broken by one bad entry, and the caller that goes on to *use* it -- solving it
for a drawing, creating a workflow from it -- is the one with somewhere to
report it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from .spec import Spec

TEMPLATES_DIR = "templates"
TEMPLATE_FILE = "spec.yml"

# What a library repository looks like. `gui/stdlib.discover` returns this same
# three-key shape (plus what a clone knows about itself) and hands it to `Load`;
# a template read outside the GUI derives it from its own root.
DATA_TYPES_DIRNAME = "data_types"
TRANSFORMS_DIRNAME = "transforms"
RESOURCES_DIRNAME = "resources"


def library_index(root: Path | str) -> dict:
    """The type, transform, and resource libraries a repository holds."""
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
    """Every type namespace a packed input library needs to be read at all.

    Both sides of a manifest entry: its own type, and the name recorded for
    each parent -- `DataInstanceLibrary.Unpack` looks up both, so resolving
    only the first leaves a template that loads and then fails on lineage.
    """
    out: set[str] = set()
    for entry in (inline.get("manifest") or {}).values():
        if not isinstance(entry, dict): continue
        names = [entry.get("type"), *(entry.get("parents") or {}).values()]
        for name in names:
            if isinstance(name, str) and "::" in name:
                out.add(name.split("::", 1)[0])
    return out


def _resolve_names(raw: dict, index: dict, root: Path) -> tuple[dict, list[str]]:
    """Turn a stored template's names into this repository's real paths.

    Matched by file stem for a type namespace (the namespace *is* the stem) and
    by directory name for a library, which is what lets an older template that
    stored `transforms/logistics` and a current one that stores `logistics`
    land on the same library.

    A name with nothing to match is left exactly as it was rather than dropped:
    it may still be a path that resolves against the root (`Spec.Unpack`), and
    a repository laid out some other way is not this function's business. It is
    reported either way, and the caller decides.
    """
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
    """A named spec, on disk, naming the libraries it needs rather than locating them."""

    name: str
    spec: Spec
    description: str = ""
    root: Path | None = None
    # names this template's repository could not account for, from `Load`
    unresolved: list[str] = field(default_factory=list)

    # -- on disk -----------------------------------------------------------

    @staticmethod
    def PathIn(root: Path | str, name: str, dirname: str = TEMPLATES_DIR) -> Path:
        return Path(root) / dirname / name / TEMPLATE_FILE

    def Save(self, root: Path | str, dirname: str = TEMPLATES_DIR) -> Path:
        """Write `<root>/<dirname>/<name>/spec.yml`, references reduced to names.

        `dirname` defaults to the library-shipped `templates/`, but a caller
        with its own place to keep them (the GUI's project-local
        `user_templates/`) can point this elsewhere without a second format to
        keep in step.

        The type namespace map `PackInline` produces is dropped rather than
        rewritten: every namespace in it is already named by the manifest's own
        `ns::type` strings, so the map is the same information in the one form
        -- a path -- that does not travel. Libraries keep their directory name
        and nothing else. `Load` puts both back.
        """
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
        """Read one template, resolving its names.

        `root` defaults to the repository it sits in. `libraries` is what the
        names are resolved against -- the three-key shape `library_index`
        returns, which is also what `gui/stdlib.discover` produces. Left unset
        it is the template's own root, which is the right answer for a template
        that ships beside the libraries it names; the GUI passes the project's
        standard library instead, because a *user's* template sits in the
        project and names libraries that do not.
        """
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
        """Every template in a library repository, by name."""
        root = Path(root).resolve()
        index = library_index(root) if libraries is None else libraries
        found = [
            cls.Load(p, root=root, libraries=index)
            for p in sorted((root / dirname).glob(f"*/{TEMPLATE_FILE}"))
        ]
        return sorted(found, key=lambda t: t.name)

    # -- as data -----------------------------------------------------------

    def Pack(self) -> dict:
        return {"name": self.name, "description": self.description} | self.spec.Pack(
            relative_to=self.root
        )
