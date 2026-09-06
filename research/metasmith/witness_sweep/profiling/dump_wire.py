"""Encode a metagenomics arm to the wire and write it out, once."""
import json, sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "src/metasmith_libraries"))
sys.path.insert(0, str(ROOT / "research/metasmith/witness_sweep"))

import duplicate_work as D
from metasmith.agents.spec import _as_data_lib, _as_transform_lib
from metasmith.models.solver_wire import encode_problem
from metasmith.models.solver_engine import SOLVER_WIRE_VERSION
from metasmith.agents.targets import TargetBuilder
from metasmith.models.libraries import DataInstanceLibraryView
from metasmith.models.solver import Transform

arm, out = sys.argv[1], Path(sys.argv[2])
out.parent.mkdir(parents=True, exist_ok=True)
spec = D.TEMPLATE.build_spec()
targets = D.targets(arm)

data_lib = _as_data_lib(spec.input_library)
samples = [DataInstanceLibraryView(s) if not isinstance(s, DataInstanceLibraryView) else s
           for s in data_lib.AsSamples(spec.sample_type)]
resources = [_as_data_lib(x) for x in spec.resource_libraries]
res_views = [r if isinstance(r, DataInstanceLibraryView) else DataInstanceLibraryView(r)
             for r in resources]
transforms = [_as_transform_lib(x) for x in spec.transform_libraries]

tb = TargetBuilder(); tb.AddAll(targets)

def _get_endpoint(dtype_name):
    ns, name = dtype_name.split("::")
    for trlib in transforms:
        tlib = trlib.types.get(ns)
        if tlib is None or name not in tlib: continue
        return trlib.GetType(dtype_name)
    raise AssertionError(dtype_name)

target_model = Transform()
spec2dep = {}
for t in tb.resolve():
    d = target_model.AddRequirement(example=_get_endpoint(t.dtype_name),
                                    parents={spec2dep[p] for p in t.parents})
    spec2dep[t] = d

given = [[sample] + res_views for sample in samples]
# WorkflowPlan.Generate turns library views into endpoint sets; mirror the shape
from metasmith.models.workflow.plan import WorkflowPlan
enc = None
orig = WorkflowPlan.Generate
import metasmith.models.solver as S
real_solve = S.solve_by_mcts
def capture(given, transforms, target, seed=42, max_iter=256, max_refine=256):
    global enc
    enc = encode_problem(given, transforms, target, seed=seed, max_iter=max_iter,
                         max_refine=max_refine, wire_version=SOLVER_WIRE_VERSION)
    raise SystemExit(0)
S.solve_by_mcts = capture
import metasmith.models.workflow.plan as P
P.solve_by_mcts = capture
try:
    WorkflowPlan.Generate(given=given, transforms=transforms,
                          target_names=[t.dtype_name for t in tb.resolve()],
                          target_model=target_model, max_iter=256, max_refine=256, seed=42)
except SystemExit:
    pass
assert enc is not None, "never reached the solver"
out.write_text(json.dumps(enc.payload))
p = enc.payload
print(f"{arm}: nodes={len(p['nodes'])} transforms={len(p['transforms'])} "
      f"props={p['n_properties']} given_groups={len(p['given'])} "
      f"target_requires={len(p['transforms'][p['target_index']]['requires'])} -> {out}")
