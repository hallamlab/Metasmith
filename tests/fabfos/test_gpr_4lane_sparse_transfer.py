# The sparse label transfer must be the dense one, not merely like it.
#
# `gpr_4lane.lane_embed` used to materialise a dense (reference x MNXR) float32
# indicator matrix and take the kNN vote as `w @ L[nn]`. At the pinned pool that
# matrix is 222,019 x 13,112 -- 10.84 GiB, 99.97% zeros -- which is the whole
# reason the step had to declare 48 GB. It is now a CSR-shaped gather over only
# the labels the K neighbours actually carry.
#
# That is a rewrite of the one number the `pbert` lane reports, so "looks right"
# is not a standard. This runs BOTH forms over the same random pools and requires
# the emitted rows to match exactly: same (query, mnxr, donor) triples, in the
# same order, with scores equal to float tolerance.
#
# The cases are chosen for where the two forms could legitimately disagree:
#
# * a reference whose `mnxr_list` REPEATS a label -- the dense form writes 1.0
#   idempotently, an accumulation would count it twice;
# * a reference with NO labels, and a query whose whole neighbourhood has none;
# * an empty token from a trailing `;`, which `sorted(set(...))` must drop and
#   `vidx` would otherwise KeyError on;
# * ties in the similarity, which decide the `best` donor.
#
# Run: python tests/test_gpr_4lane_sparse_transfer.py   (or under pytest)
from __future__ import annotations

import re
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[2]
MAPPER = (REPO / "src" / "metasmith_libraries" / "resources" / "lib"
          / "fabfos_gpr" / "gpr_4lane.py")

K = 30
FLOOR = 0.20


def _sparse_impl():
    src = MAPPER.read_text()
    # The vote is the middle of `lane_embed`; run it here against arrays rather
    # than files by re-executing just the arithmetic, which is the block below.
    start = src.index("    # THE LABEL MATRIX IS SPARSE")
    end = src.index('    print("[gpr] " + channel + ": "')
    block = src[start:end]
    block = re.sub(r"    q_orf, q_raw, _ = _read_query\(parquet\)\n"
                   r"(    if q_raw\.shape\[1\].*?referent\"\)\n)"
                   r"    q_emb = _norm\(q_raw\)\n", "", block, flags=re.S)
    return block


SPARSE_BLOCK = _sparse_impl()


def _norm(x):
    n = np.linalg.norm(x, axis=1, keepdims=True)
    return x / np.clip(n, 1e-9, None)


def dense_rows(ref_mnxr_list, ref_orf, ref_emb, q_orf, q_emb, floor=FLOOR):
    label_lists = [s.split(";") if s else [] for s in ref_mnxr_list]
    vocab = sorted({m for ls in label_lists for m in ls})
    vidx = {m: i for i, m in enumerate(vocab)}
    L = np.zeros((len(ref_mnxr_list), len(vocab)), dtype=np.float32)
    for r, ls in enumerate(label_lists):
        for m in ls:
            L[r, vidx[m]] = 1.0
    rows = []
    for s in range(0, len(q_emb), 256):
        sim = q_emb[s:s+256] @ ref_emb.T
        top = np.argpartition(-sim, min(K, sim.shape[1]-1), axis=1)[:, :K]
        for bi in range(sim.shape[0]):
            nn = top[bi]
            vals = np.clip(sim[bi, nn], 0, None)
            tot = vals.sum()
            if tot <= 0:
                continue
            w = vals / tot
            votes = w @ L[nn]
            best = int(np.argmax(sim[bi, nn]))
            for j in np.nonzero(votes >= floor)[0]:
                rows.append((q_orf[s+bi], vocab[j], ref_orf[nn[best]],
                             float(min(votes[j], 1.0))))
    return rows


def sparse_rows(ref_mnxr_list, ref_orf, ref_emb, q_orf, q_emb, floor=FLOOR):
    import pandas as pd
    ns = {
        "np": np, "pd": pd, "floor": floor,
        "nn_min": 0.0, "tau": 0.0, "k_max": K, "channel": "pbert",
        "ref": pd.DataFrame({"mnxr_list": ref_mnxr_list}),
        "ref_orf": np.asarray(ref_orf, dtype=object),
        "ref_emb": ref_emb, "q_orf": np.asarray(q_orf, dtype=object),
        "q_emb": q_emb, "rows": [],
    }
    exec("if 1:\n" + SPARSE_BLOCK, ns)          # noqa: S102 -- the point of the test
    return ns["rows"]


def _case(seed, n_ref, n_q, dim, vocab_n, max_labels, *, dupe=False, empty=False,
          trailing=False, ties=False):
    rng = np.random.default_rng(seed)
    vocab = [f"MNXR{100000 + i}" for i in range(vocab_n)]
    lists = []
    for r in range(n_ref):
        k = int(rng.integers(0, max_labels + 1))
        picks = list(rng.choice(vocab, size=k, replace=False)) if k else []
        if dupe and picks:
            picks = picks + [picks[0]]
        if empty and r % 17 == 0:
            picks = []
        s = ";".join(picks)
        if trailing and picks and r % 5 == 0:
            s = s + ";"
        lists.append(s)
    ref_emb = _norm(rng.normal(size=(n_ref, dim)).astype(np.float32))
    q_emb = _norm(rng.normal(size=(n_q, dim)).astype(np.float32))
    if ties:
        ref_emb[1::2] = ref_emb[0::2][: len(ref_emb[1::2])]
    ref_orf = [f"R{i}" for i in range(n_ref)]
    q_orf = [f"Q{i}" for i in range(n_q)]
    return lists, ref_orf, ref_emb, q_orf, q_emb


CASES = {
    "plain":     dict(seed=1, n_ref=400, n_q=300, dim=32, vocab_n=60, max_labels=4),
    "duplicate_label": dict(seed=2, n_ref=400, n_q=300, dim=32, vocab_n=40,
                            max_labels=3, dupe=True),
    "unlabelled_refs": dict(seed=3, n_ref=400, n_q=300, dim=32, vocab_n=25,
                            max_labels=2, empty=True),
    "trailing_semicolon": dict(seed=4, n_ref=400, n_q=300, dim=32, vocab_n=30,
                               max_labels=3, trailing=True),
    "ties":      dict(seed=5, n_ref=400, n_q=200, dim=16, vocab_n=20, max_labels=3,
                      ties=True),
    "wide_block": dict(seed=6, n_ref=900, n_q=700, dim=48, vocab_n=200, max_labels=6),
}


def check(name: str) -> None:
    # A floor well below the production 0.20: at 0.20 over random embeddings the
    # vote almost never clears, and a comparison over five rows proves nothing.
    args = _case(**CASES[name])
    d = dense_rows(*args, floor=0.02)
    s = sparse_rows(*args, floor=0.02)
    if name == "trailing_semicolon":
        # THE ONE DELIBERATE DIFFERENCE. A trailing `;` puts an EMPTY token in
        # the dense form's vocabulary, so it emits rows whose `mnxr` is "" --
        # which `validate_gpr`'s MNXR pattern rejects, one whole run later. The
        # sparse form drops the token at parse. Compared against the dense rows
        # MINUS those, because "identical" would mean reproducing a bug.
        d = [r for r in d if r[1]]
    assert len(d) == len(s), f"{name}: dense {len(d)} rows, sparse {len(s)}"
    for i, (a, b) in enumerate(zip(d, s)):
        assert a[0] == b[0], f"{name}[{i}]: query {a[0]!r} vs {b[0]!r}"
        assert a[1] == b[1], f"{name}[{i}]: mnxr {a[1]!r} vs {b[1]!r}"
        assert a[2] == b[2], f"{name}[{i}]: donor {a[2]!r} vs {b[2]!r}"
        assert abs(a[3] - b[3]) < 1e-6, f"{name}[{i}]: score {a[3]} vs {b[3]}"
    assert d, f"{name}: produced no rows, so nothing was actually compared"
    print(f"  {name:20s} {len(d):>6,} rows identical")


def test_sparse_label_transfer_matches_dense():
    for name in CASES:
        check(name)


def test_trailing_semicolon_would_have_crashed_a_naive_port():
    # The empty token is not hypothetical: `vidx[""]` is a KeyError.
    #
    # The dense form tolerated it by putting `""` in the vocabulary; the sparse
    # form drops it in `sorted(set(...))`. Both must therefore agree that no row
    # ever carries an empty mnxr, which the schema validator would reject anyway.
    args = _case(**CASES["trailing_semicolon"])
    dense = dense_rows(*args, floor=0.02)
    sparse = sparse_rows(*args, floor=0.02)
    assert any(not r[1] for r in dense), (
        "the fixture no longer produces the empty-token rows it exists to cover")
    assert all(r[1] for r in sparse), "an empty MNXR id reached the output"
    print(f"  empty-token rows: dense emits {sum(1 for r in dense if not r[1])}, "
          f"sparse emits 0")


if __name__ == "__main__":
    print("sparse vs dense label transfer:")
    test_sparse_label_transfer_matches_dense()
    test_trailing_semicolon_would_have_crashed_a_naive_port()
    print("OK")
