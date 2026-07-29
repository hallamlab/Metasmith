"""A workflow you can start from.

A template is a `Spec` whose inputs are `DEFERRED` -- the same object a stored
workflow is, so there is no template format to design, version or validate
separately. What a template adds is only where it lives and what it is called:
a directory in a library repository, holding the spec and the deferred input
library the spec points at.

    <root>/templates/<name>/spec.yml      the spec, plus a name and a blurb
    <root>/templates/<name>/inputs.xgdb   the deferred rows, typed, with lineage

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
    def PathIn(root: Path | str, name: str) -> Path:
        return Path(root) / TEMPLATES_DIR / name / TEMPLATE_FILE

    def Save(self, root: Path | str) -> Path:
        """Write `<root>/templates/<name>/spec.yml`, references made relative.

        Refuses a reference outside the root: that is the one way a template
        can be written successfully and still be useless everywhere else.
        """
        root = Path(root).resolve()
        packed = self.spec.Pack(relative_to=root)
        escaped = sorted(
            str(v) for k in ("input_library", "transform_libraries", "resource_libraries")
            for v in ([packed[k]] if isinstance(packed[k], str) else packed[k])
            if Path(v).is_absolute()
        )
        assert not escaped, (
            f"template [{self.name}] references libraries outside [{root}], so it would "
            f"name nothing in another checkout: {', '.join(escaped)}"
        )
        out = self.PathIn(root, self.name)
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
    def Discover(cls, root: Path | str) -> list["Template"]:
        """Every template in a library repository, by name."""
        root = Path(root).resolve()
        found = [
            cls.Load(p, root=root)
            for p in sorted((root / TEMPLATES_DIR).glob(f"*/{TEMPLATE_FILE}"))
        ]
        return sorted(found, key=lambda t: t.name)

    # -- as data -----------------------------------------------------------

    def Pack(self) -> dict:
        return {"name": self.name, "description": self.description} | self.spec.Pack(
            relative_to=self.root
        )
