"""THE CHECK -- `_knn.vote` must BE the deployed lane's vote, not resemble it.

Every threshold this directory recommends is chosen on `_knn.vote`, and shipped into
`gpr_4lane.py::lane_embed`. If the two rules drift, the sweeps tune one lane and the
pipeline runs another, and nothing anywhere raises -- the tables would simply carry
the wrong calls at the recommended settings.

So the lane is LIFTED OUT OF THE LIVE TRANSFORM and run beside the harness on the
same random pools, at settings chosen to exercise each branch: the pre-quota no-op,
an absolute floor that refuses ORFs, a relative band that admits a handful, a k_max
below the retrieval width, and a zero vote floor. The emitted (query, label, score)
triples must match exactly.

ENV     PYTHONPATH="$PWD/src" mamba run -n msm python \
            research/fabfos/annotation_lanes/pbert/check_vote_matches_lane.py
"""
from __future__ import annotations

import re
import string
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
sys.path.insert(0, str(HERE))
import _knn  # noqa: E402

TRANSFORM = REPO / "src/metasmith_libraries/transforms/fabfos/gpr_4lane.py"
EV_LIB = REPO / "src/metasmith_libraries/resources/lib/fabfos_evidence.py"

# (nn_min, tau, k_max, floor)
SETTINGS = [
    (0.00, 0.00, 30, 0.20),     # the pre-quota rule
    (0.60, 0.00, 30, 0.20),     # absolute floor only -- refuses ORFs
    (0.00, 0.98, 30, 0.20),     # relative band only  -- admits a handful
    (0.60, 0.95, 8, 0.20),      # both, and k_max below the retrieval width
    (0.60, 0.95, 30, 0.00),     # zero vote floor: every label an admitted neighbour has
]


def lane_ns():
    """`lane_embed` and its readers, out of the transform's own DRIVER string."""
    body = re.search(r"^DRIVER = r'''\n(.*?)^'''", TRANSFORM.read_text(),
                     re.S | re.M).group(1)
    keys = {k for _, k, _, _ in string.Formatter().parse(body) if k}
    filled = body.format(**{k: {"ev_lib": str(EV_LIB), "threads": "2",
                                "lane_set": "chosen_4",
                                "source": "check"}.get(k, "") for k in keys})
    ns = {"__name__": "lane"}
    exec(compile(filled.split("\ndef main():")[0], "lane", "exec"), ns)  # noqa: S102
    return ns


def fixture(seed, n_ref=300, n_q=120, dim=24, vocab_n=40):
    """A pool with real structure: half the queries sit inside a cluster, half do not,
    so both the abstain and the admission branch see traffic."""
    rng = np.random.default_rng(seed)
    centres = rng.normal(size=(6, dim)).astype(np.float32)
    ref = np.concatenate([c + 0.15 * rng.normal(size=(n_ref // 6, dim))
                          for c in centres]).astype(np.float32)
    vocab = [f"MNXR{100000 + i}" for i in range(vocab_n)]
    lists = [";".join(sorted(set(rng.choice(vocab, size=int(rng.integers(0, 4)),
                                            replace=False))))
             for _ in range(len(ref))]
    half = n_q // 2
    q = np.concatenate([
        centres[rng.integers(0, 6, half)] + 0.15 * rng.normal(size=(half, dim)),
        rng.normal(size=(n_q - half, dim)) * 2.0,
    ]).astype(np.float32)
    return ref, lists, q


def main() -> int:
    ns = lane_ns()
    bad = 0
    for seed in (1, 2, 3):
        ref, lists, q = fixture(seed)
        acc = np.array([f"REF{i:05d}" for i in range(len(ref))], dtype=object)
        qid = np.array([f"Q{i:05d}" for i in range(len(q))], dtype=object)

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            (td / "lm").mkdir()
            dims = [f"dim_{i}" for i in range(ref.shape[1])]
            pd.concat([pd.DataFrame({"accession": acc, "mnxr_list": lists}),
                       pd.DataFrame(ref, columns=dims)], axis=1).to_parquet(
                td / "lm" / "landmarks.parquet", index=False)
            pd.concat([pd.DataFrame({"sequence_id": qid}),
                       pd.DataFrame(q, columns=dims)], axis=1).to_parquet(
                td / "q.parquet", index=False)

            # the harness side: one wide retrieval, reused for every setting
            m = _knn.build_metrics(ref, q, names=["cosine"])["cosine"]
            idx, val = _knn.topk(m, k=min(_knn.KWIDE, len(ref)))
            cos = np.einsum("nd,nkd->nk", _knn._norm(q), _knn._norm(ref)[idx])
            label_lists = [s.split(";") if s else [] for s in lists]

            for nn_min, tau, k_max, floor in SETTINGS:
                lane = ns["lane_embed"](str(td / "q.parquet"), str(td / "lm"),
                                        "pbert", floor, nn_min, tau, k_max)
                got = {(r.orf, r.mnxr): float(r.raw_score) for r in lane.itertuples()}

                I, C = _knn.refine(idx, cos, k=k_max)
                v = _knn.vote(I, C, label_lists, floor, nn_min, tau, k_max)
                want = {(o, lab): float(s)
                        for o, d in zip(qid, v) for lab, s in d.items()}

                # WHICH CALLS is exact; the SCORE carries float32 slack. The lane
                # takes its cosines from one `q @ ref.T` matmul and the harness from
                # an einsum over the retrieved rows, so the two sum the same products
                # in different orders and land up to one ULP apart -- 1.2e-7 at 0.5,
                # measured. TOL is far above that and far below any vote floor on the
                # grid, so a real difference in the rule cannot hide under it.
                TOL = 1e-5
                worst = max((abs(got[k] - want[k]) for k in set(got) & set(want)),
                            default=0.0)
                tag = f"seed {seed} nn_min={nn_min} tau={tau} k={k_max} floor={floor}"
                if set(got) == set(want) and worst <= TOL:
                    print(f"  ok   {tag}: {len(want)} calls agree "
                          f"(worst score gap {worst:.2e})")
                    continue
                bad += 1
                only_lane = sorted(set(got) - set(want))[:3]
                only_harness = sorted(set(want) - set(got))[:3]
                print(f"  DIFF {tag}: lane {len(got)} calls, harness {len(want)}; "
                      f"lane-only {only_lane}, harness-only {only_harness}, "
                      f"worst score gap {worst:.2e}")
    if bad:
        print(f"\n{bad} setting(s) disagree. The sweeps in this directory tune a rule "
              f"the pipeline does not run.", file=sys.stderr)
        return 1
    print("\nthe harness vote and the deployed lane agree on every setting")
    return 0


if __name__ == "__main__":
    sys.exit(main())
