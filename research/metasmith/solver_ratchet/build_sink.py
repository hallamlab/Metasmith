"""Encode generated `sink`-profile problems to the solver wire.

The `sink` dials are the only profile in SWEEP_PROFILES that reaches the iteration
cap, which is what makes these the cases a selection change can move. Problem seeds
are held out from the slices the two PUCT reports tuned on (0-19) and measured on
(40-79, 100+).
"""
import json, sys
from dataclasses import replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "src"))

from metasmith.testing.solver_bench import SWEEP_PROFILES
from metasmith.testing.solver_verification import generate_problem
from metasmith.models.solver_wire import encode_problem
from metasmith.models.solver_engine import SOLVER_WIRE_VERSION

out_dir = Path(sys.argv[1]); out_dir.mkdir(parents=True, exist_ok=True)
sink = dict(SWEEP_PROFILES)["sink"]

spec = [(int(n), int(s)) for n, s in (x.split(":") for x in sys.argv[2].split(","))]
for n_types, pseed in spec:
    name = f"sink{n_types}-{pseed}"
    p = generate_problem(pseed, replace(sink, n_types=n_types), name=name)
    enc = encode_problem([set(g) for g in p.given], list(p.transforms), p.target,
                         seed=42, max_iter=256, max_refine=8,
                         wire_version=SOLVER_WIRE_VERSION)
    (out_dir / f"{name}.json").write_text(json.dumps(enc.payload))
    pl = enc.payload
    print(f"{name}: nodes={len(pl['nodes'])} transforms={len(pl['transforms'])} "
          f"props={pl['n_properties']} given_groups={len(pl['given'])}")
