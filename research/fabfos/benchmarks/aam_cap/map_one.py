"""Map ONE reaction with Indigo and report `(status, seconds, mapped_smiles)` as JSON.

A separate process per reaction, and that is the measurement's only structural
requirement. Indigo's search can hang inside compiled code, where neither `signal.alarm`
nor its own `aam-timeout` option reaches -- the member lane answers that with a sidecar
and a watchdog, and this answers it with a parent that can send SIGKILL. A sweep that
ran the mappings in-process would stop on the first pathological reaction having
recorded nothing, which is the failure this is trying to measure.
"""
from __future__ import annotations

import json
import sys
import time

from indigo import Indigo

smi = json.loads(sys.argv[1])["smiles"]
budget = int(sys.argv[2])

ind = Indigo()
ind.setOption("aam-timeout", budget * 1000)
t0 = time.time()
try:
    rxn = ind.loadReaction(smi)
    rxn.automap("discard")
    out, status = rxn.smiles(), "ok"
except Exception as e:
    # Indigo raises rather than returning on its own timeout, so this is the ordinary
    # "did not finish in the budget" outcome and not an error in the sweep.
    out, status = "", f"error:{type(e).__name__}"
print(json.dumps(dict(status=status, secs=round(time.time() - t0, 2), mapped=out)))
