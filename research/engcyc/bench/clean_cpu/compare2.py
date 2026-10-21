"""Compare saved runs with a reference run over their shared ORFs: embedding cosine, and top-EC and
EC-set agreement of the in-memory maxsep calls (<name>.fast.calls.tsv), also after the lane's
score filter, which is the set the GPR mapper actually keeps.

Usage: python compare2.py <dir> <reference name> <name> [<name> ...]
"""
import collections
import json
import sys

import numpy as np

D, ref, names = sys.argv[1], sys.argv[2], sys.argv[3:]
MIN_SCORE = 0.02  # fabfos_evidence.CLEAN_MIN_SCORE: the lane drops calls below it


def load(name):
    m = json.load(open(f"{D}/{name}.json"))
    e = np.load(f"{D}/{name}.npy")
    calls = collections.defaultdict(list)
    for line in open(f"{D}/{name}.fast.calls.tsv"):
        o, ec, sc = line.rstrip("\n").split("\t")
        calls[o].append((float(sc), ec))
    return dict(zip(m["ids"], e)), calls


re_, rc = load(ref)
for name in names:
    e, c = load(name)
    common = [o for o in e if o in re_ and o in c and o in rc]
    a = np.stack([e[o] for o in common])
    b = np.stack([re_[o] for o in common])
    cos = (a * b).sum(1) / np.linalg.norm(a, axis=1) / np.linalg.norm(b, axis=1)
    top = np.mean([max(c[o])[1] == max(rc[o])[1] for o in common])
    eset = np.mean([{x for _, x in c[o]} == {x for _, x in rc[o]} for o in common])
    kept = lambda calls, o: {x for sc, x in calls[o] if sc >= MIN_SCORE}  # noqa: E731
    conf = [o for o in common if max(rc[o])[0] >= MIN_SCORE]
    kept_agree = np.mean([kept(c, o) == kept(rc, o) for o in common])
    top_conf = np.mean([max(c[o])[1] == max(rc[o])[1] for o in conf])
    print(json.dumps({"name": name, "ref": ref, "n": len(common), "cos_min": round(float(cos.min()), 5),
                      "cos_mean": round(float(cos.mean()), 6), "top_ec_agree": round(float(top), 4),
                      "ec_set_agree": round(float(eset), 4), "kept_set_agree": round(float(kept_agree), 4),
                      "n_ref_top_ge_min": len(conf), "top_ec_agree_ref_ge_min": round(float(top_conf), 4)}),
          flush=True)
