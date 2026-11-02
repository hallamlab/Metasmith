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
    out, status = "", f"error:{type(e).__name__}"
print(json.dumps(dict(status=status, secs=round(time.time() - t0, 2), mapped=out)))
