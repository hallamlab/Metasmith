#!/usr/bin/env python3
"""Emit metasmith template author modules for the best-attested KBase workflows.

`solve_candidates.py` answers the coverage question over all 623 candidates. This
emits the durable artifact for the top of that list: a real author module per
workflow, in the shape `src/metasmith_libraries/build_templates.py` expects, plus
the committed mask each one solves against.

A template records its transform library by bare name, so the mask cannot be a
temporary directory. Each workflow gets `library/transforms/w_<shape>/`, holding
only the stubs that workflow uses.

    emit_templates.py [--top N]
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent.parent
LIB = REPO / "research" / "kbase" / "library"
RESULTS = HERE / "solve_results.jsonl"

sys.path.insert(0, str(LIB))
import mask  # noqa: E402

def _snake(name: str) -> str:
    # Split on a lower-to-upper boundary and before the tail of an acronym run,
    # so FBAModel is fba_model rather than f_b_a_model.
    out = re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    return re.sub(r"_+", "_", out).strip("_").lower()


def slug(t: str, unions: dict) -> str:
    if t == "report": return "report"
    members = unions.get(t)
    if members:
        return "any_" + _snake(members[0].split(".", 1)[-1])
    return _snake(t.split("_", 1)[-1] if "_" in t else t)


def template_name(r, taken: set, unions: dict) -> str:
    base = f"{slug(r['targets'][0], unions)}_from_{slug(r['inputs'][0], unions)}"
    name, i = base, 2
    while name in taken:
        name = f"{base}_{i}"; i += 1
    taken.add(name)
    return name


MODULE = '''#!/usr/bin/env python3
"""{name} -- a workflow {copies} public KBase narratives ran.

Reconstructed from narrative {rep} ({title}), whose app cells wire:

{chain}

The mask `w_{shape_id}` holds exactly those apps. Solving against the whole
generated library instead answers every target with an uploader, because 46 KBase
apps are pure sources and so are the cheapest producer of anything.
"""
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
import _authoring as A
from metasmith.python_api import DEFERRED, Spec

NAME = "{name}"
DESCRIPTION = """
{description}
"""


def build_spec(rebuild: bool = False) -> Spec:
    def inputs(lib):
        lib.AddTypeLibrary(A.TYPES / "kbase.yml")
        study = lib.AddItem(DEFERRED, "kbase::study")
        sample = lib.AddItem(DEFERRED, "kbase::sample", parents={{study}})
{adds}

    return Spec(
        input_library=A.deferred_inputs(NAME, inputs, rebuild=rebuild),
        sample_type="kbase::sample",
        target_types={targets},
        transform_libraries=A.transforms("w_{shape_id}"),
        resource_libraries=[A.envs()],
    )


if __name__ == "__main__":
    A.cli(sys.modules[__name__])
'''


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--top", type=int, default=12)
    a = ap.parse_args()

    sys.path.insert(0, str(HERE))
    import solve_candidates as SC
    unions = {name: list(members) for members, name in SC.union_index().items()}

    rows = [json.loads(l) for l in RESULTS.read_text().splitlines() if l]
    solved = [r for r in rows if r["ok"] and r["inputs"] and r["targets"]]
    solved.sort(key=lambda r: (-r["copies"], -r["steps"]))
    chosen = solved[:a.top]

    out_dir = LIB / "templates_src"
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in out_dir.glob("*.py"): stale.unlink()
    # A mask left over from a previous top-N is a library the build still
    # compiles and no template names.
    import shutil
    keep_masks = {f"w_{r['shape_id']}" for r in chosen}
    for d in (LIB / "transforms").glob("w_*"):
        if d.name not in keep_masks: shutil.rmtree(d)

    taken, names = set(), []
    for r in chosen:
        name = template_name(r, taken, unions)
        keep = set(r["apps"]) - set(r.get("dropped_sources") or [])
        mask.materialise(LIB / "transforms" / f"w_{r['shape_id']}", keep)

        adds = "\n".join(
            f'        lib.AddItem(DEFERRED, "kbase::{t}", parents={{sample}})'
            for t in r["inputs"])
        chain = "\n".join(f"    {i + 1}. {app}" for i, app in enumerate(r["apps"]))
        desc = (f"{' -> '.join(x.split('/')[-1] for x in r['apps'])}. "
                f"Seen in {r['copies']} public narratives.")
        (out_dir / f"{name}.py").write_text(MODULE.format(
            name=name, copies=r["copies"], rep=r["representative"],
            title=(r.get("title") or "untitled").replace('"', "'")[:60],
            chain=chain, description=desc, shape_id=r["shape_id"],
            adds=adds, targets=json.dumps([f"kbase::{t}" for t in r["targets"]]),
        ))
        names.append(name)

    (LIB / "build_templates.py").write_text(
        '#!/usr/bin/env python3\n'
        '"""Author every KBase template, and fail on any that no longer solves.\n\n'
        'Mirrors src/metasmith_libraries/build_templates.py. Run it after\n'
        'regenerating the library: a transform whose products change shape takes its\n'
        'templates down here, by name, rather than in someone\'s GUI a week later.\n"""\n'
        "import sys\nfrom pathlib import Path\n\n"
        "HERE = Path(__file__).resolve().parent\n"
        "sys.path.insert(0, str(HERE))\nsys.path.insert(0, str(HERE / 'templates_src'))\n"
        "import _authoring as A\n\n"
        f"AUTHORS = {json.dumps(names, indent=4)}\n\n"
        "BLOCKED = {}\n\n"
        "def main():\n"
        "    wanted = sys.argv[1:] or AUTHORS\n"
        "    for name in wanted:\n"
        "        if name in BLOCKED:\n"
        "            print(f'  {name}: BLOCKED -- {BLOCKED[name]}'); continue\n"
        "        A.author(__import__(name), rebuild='--rebuild' in sys.argv)\n\n"
        "if __name__ == '__main__':\n    main()\n")

    subprocess.run(
        [sys.executable, "-m", "metasmith", "build", "transforms",
         "--types", str(LIB / "data_types")]
        + sum([["--transforms", str(LIB / "transforms" / f"w_{r['shape_id']}")]
               for r in chosen], []),
        check=True, capture_output=True)
    print(f"emitted {len(names)} author modules and masks:")
    for n, r in zip(names, chosen):
        print(f"  {n:44s} {r['copies']:3d} copies  {r['steps']} steps  w_{r['shape_id']}")


if __name__ == "__main__":
    main()
