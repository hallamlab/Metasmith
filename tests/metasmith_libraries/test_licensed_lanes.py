"""A licensed lane plans only when its user gave the download or a prebuilt image.

No package: the lane is unreachable and the hint names where to get one. A
package: the plan builds the image before running the lane. A prebuilt image
given as the env (a lab's own SIF): the lane runs on it with no build.
"""

import sys
import tempfile
from pathlib import Path

import pytest

from conftest import MLIB

sys.path.insert(0, str(MLIB))
import _authoring as A  # noqa: E402
from metasmith.python_api import DEFERRED, DataInstanceLibrary, Plan, Spec, record_library  # noqa: E402

LANES = [
    ("annotation::pathway_table", "licensed::pathway_tools_package", "env::pathologic.env",
     "buildPathwayToolsImage", "pathologic", "https://biocyc.org/download-bundle.shtml"),
    ("annotation::signalp6_results", "licensed::signalp6_package", "env::signalp6.env",
     "buildSignalP6Image", "signalp6", "https://services.healthtech.dtu.dk/cgi-bin/sw_request?software=signalp"),
]


def _solve(target: str, given: list[str]):
    lib = DataInstanceLibrary(Path(tempfile.mkdtemp(prefix="msm-licensed-")))
    for t in ("sequences.yml", "annotation.yml", "licensed.yml", "env.yml"):
        lib.AddTypeLibrary(A.TYPES / t)
    orfs = lib.AddItem(DEFERRED, "sequences::orfs")
    lib.AddItem(DEFERRED, "annotation::ptools_annotation_table", parents={orfs})
    shared = [str(lib.AddItem(DEFERRED, t)) for t in given]
    return Plan(
        [Spec(
            input_library=record_library(lib),
            sample_type="sequences::orfs",
            shared_input_paths=shared,
            target_types=[target],
        )],
        transform_libraries=A.transforms("functionalAnnotation"),
        resource_libraries=[A.envs(), A.MLIB / "resources" / "lib"],
    )


@pytest.mark.parametrize("target,package,env,build,lane,url", LANES, ids=[l[4] for l in LANES])
def test_a_licensed_lane_needs_its_package(target, package, env, build, lane, url):
    task = _solve(target, [])
    assert not task.ok
    hint = next(h for h in task.plan.hints if h.kind == "unproduced_requirement")
    assert package in hint.message and url in hint.message, hint.message

    steps = [s.transform.name for s in _solve(target, [package]).plan.steps]
    assert steps == [build, lane]

    steps = [s.transform.name for s in _solve(target, [env]).plan.steps]
    assert steps == [lane], "a given image is used as is, not rebuilt"
