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

* **References are relative to the root.** `Save` refuses a library reference
  that does not live under it. A template naming an absolute path is one
  machine's checkout and would arrive at a colleague pointing at nothing; the
  failure of the silent version is a solve that reports an unknown type.
* **The spec is what ships, not a picture of it.** Rendering a DAG while
  authoring is a debugging aid; a rendered SVG in the repository would be a
  drawing kept fresh by a build that never looks at it, in whatever theme the
  author happened to have. The GUI renders from the spec, in its own theme.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

from .spec import Spec

TEMPLATES_DIR = "templates"
TEMPLATE_FILE = "spec.yml"


@dataclass
class Template:
    """A named spec, on disk, relative to the library repository it ships in."""

    name: str
    spec: Spec
    description: str = ""
    root: Path | None = None

    # -- on disk -----------------------------------------------------------

    @staticmethod
    def PathIn(root: Path | str, name: str, dirname: str = TEMPLATES_DIR) -> Path:
        return Path(root) / dirname / name / TEMPLATE_FILE

    def Save(self, root: Path | str, dirname: str = TEMPLATES_DIR) -> Path:
        """Write `<root>/<dirname>/<name>/spec.yml`, references made relative.

        `dirname` defaults to the library-shipped `templates/`, but a caller
        with its own place to keep them (the GUI's project-local
        `user_templates/`) can point this elsewhere without a second format to
        keep in step.

        Refuses a reference outside the root: that is the one way a template
        can be written successfully and still be useless everywhere else.
        """
        root = Path(root).resolve()
        packed = self.spec.Pack(relative_to=root)

        def _refs(v) -> list[str]:
            # input_library is a plain string for a stored workflow, but a
            # template's is inline data (see `Spec.Pack`) -- only its type
            # namespace references are paths that could escape the root.
            if isinstance(v, dict): return list(v.get("types", {}).values())
            if isinstance(v, str): return [v]
            return list(v)

        escaped = sorted(
            v for k in ("input_library", "transform_libraries", "resource_libraries")
            for v in _refs(packed[k])
            if Path(v).is_absolute()
        )
        assert not escaped, (
            f"template [{self.name}] references libraries outside [{root}], so it would "
            f"name nothing in another checkout: {', '.join(escaped)}"
        )
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
    def Load(cls, path: Path | str, root: Path | str | None = None) -> "Template":
        """Read one template. `root` defaults to the repository it sits in."""
        path = Path(path).resolve()
        root = Path(root).resolve() if root is not None else path.parent.parent.parent
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return cls(
            name=raw.get("name") or path.parent.name,
            description=raw.get("description") or "",
            spec=Spec.Unpack(raw, root=root),
            root=root,
        )

    @classmethod
    def Discover(cls, root: Path | str, dirname: str = TEMPLATES_DIR) -> list["Template"]:
        """Every template in a library repository, by name."""
        root = Path(root).resolve()
        found = [
            cls.Load(p, root=root)
            for p in sorted((root / dirname).glob(f"*/{TEMPLATE_FILE}"))
        ]
        return sorted(found, key=lambda t: t.name)

    # -- as data -----------------------------------------------------------

    def Pack(self) -> dict:
        return {"name": self.name, "description": self.description} | self.spec.Pack(
            relative_to=self.root
        )
