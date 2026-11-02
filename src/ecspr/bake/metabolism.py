"""Encode the ensemble intermediates into the three compiled metabolism tables.

Ported from the pre-library ``build_references/bake_metabolism.py``. Two changes, both
consequences of this being a transform now rather than a script:

  * paths are arguments. The original read ``data/raw/mnxref-4_5/`` and wrote
    ``data/reference/metabolism/`` through ``refs.py``'s constants.
  * the sources are the ENSEMBLE outputs rather than a pre-baked bundle that was itself
    an R6 sitting in the acquisition tier. That bundle is what the tier rule deleted;
    the trio can now only be rebuilt through the full chain, which is the point.

What this is NOT
----------------
It is a re-encoding, not a re-derivation. Nothing here recomputes an atom mapping or a
free energy; every number that comes out is the number that went in, at float32. The
selftest exists to prove exactly that, row by row over EVERY row -- because the one
failure mode that matters (a too-narrow rank field merging two distinct atoms onto one
node) RAISES the network's conductance and so reads as an improvement rather than a bug.

Two writers, and why that is now safe
-------------------------------------
The trio used to be written by one step, because all three files carry an identical
bake-identity block and a reader refuses a mismatched trio -- so three independent
writers would plan just as happily and hand a reader an atom_pairs baked against a
different vocab, decoding every node to the wrong metabolite without raising.

It is now written by two: :func:`bake_pairs` in the AAM assembly and
:func:`bake_direction` in the direction assembly. What makes that safe is that the second
writer does not COMPUTE an identity block, it INHERITS one -- it reads ``vocab.parquet``,
takes the block verbatim, and encodes against that vocabulary. Agreement is structural
rather than coincidental, and the direction writer cannot mint a vocabulary even by
mistake because it has no code path that builds one.

The reaction space is the REACTION UNIVERSE
-------------------------------------------
``lookup::reactions``, one row per MNXR, rather than the union of the two source tables.
The union was there to stop a reaction with a direction row and no atom pairs from
dropping out of the vocabulary -- a dropped reaction falls back to ratio 1.0, i.e. fully
reversible, MORE conductance than the evidence supports, silently and in the flattering
direction. The universe is a superset of that union, so the hazard becomes structurally
impossible instead of contingent on what direction happened to contain, and the space no
longer depends on a table the pairs writer has not seen.

It also drops MetaNetX's ``EMPTY`` sentinel, which is not a reaction. Nothing may encode
a symbol the vocabulary lacks: ``Vocab.encode`` returns -1 for an unknown, and -1 cast to
the uint32 rxn column is 4,294,967,295 -- a code that indexes nothing and raises nowhere.
:func:`_encode_checked` is what stands between that and the file.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from . import encoding as refs


def _say(msg=""):
    print(msg, flush=True)


def read_pairs_source(src_pairs: Path, src_reactions: Path):
    for p in (src_pairs, src_reactions):
        if not Path(p).exists():
            raise SystemExit(f"missing input {p}")

    t0 = time.perf_counter()
    ap_sha = refs.sha256_file(src_pairs)
    pairs = pd.read_parquet(src_pairs)
    universe = sorted(set(pd.read_parquet(src_reactions, columns=["mnxr"])["mnxr"]))
    _say(f"  read sources                       {(time.perf_counter()-t0)*1000:7.0f} ms")
    _say(f"    aam_pairs   {len(pairs):>9,} rows  sha {ap_sha[:16]}")
    _say(f"    universe    {len(universe):>9,} reactions")

    orphans = set(pairs["mnxr"].unique()) - set(universe)
    if orphans:
        raise SystemExit(
            f"{len(orphans)} reactions have atom pairs but are absent from the reaction "
            f"universe (e.g. {sorted(orphans)[:3]}). lookup::reactions is built from the "
            f"same reac_prop the mappers read, so this means the two came from different "
            f"MetaNetX releases.")

    return pairs, universe, ap_sha


def read_direction_source(src_direction: Path):
    if not Path(src_direction).exists():
        raise SystemExit(f"missing input {src_direction}")
    t0 = time.perf_counter()
    dir_sha = refs.sha256_file(src_direction)
    direction = pd.read_parquet(src_direction, columns=["mnxr", "ratio", "dir_tier"])
    _say(f"  read source                        {(time.perf_counter()-t0)*1000:7.0f} ms")
    _say(f"    direction   {len(direction):>9,} rows  sha {dir_sha[:16]}")
    return direction, dir_sha


def _encode_checked(V: refs.Vocab, kind: str, symbols, *, what: str) -> np.ndarray:
    codes = V.encode(kind, symbols)
    bad = codes < 0
    if bad.any():
        names = sorted({str(s) for s, b in zip(symbols, bad) if b})
        raise SystemExit(
            f"{len(names)} {kind} symbol(s) in {what} are absent from the vocabulary "
            f"(e.g. {names[:5]}). Encoding them would write an unsigned -1.")
    return codes


def build_vocabulary(pairs: pd.DataFrame, universe: list[str]):
    elements = sorted(pairs["element"].unique())
    unexpected = set(elements) - set(refs.ELEMENT_ORDER)
    if unexpected:
        raise SystemExit(
            f"element(s) {sorted(unexpected)} are in the source but not in "
            f"refs_encoding.ELEMENT_ORDER; adding one renumbers the element codes, so "
            f"it is a deliberate edit there, not something to infer here")

    mets = sorted(set(pairs["substrate"].unique()) | set(pairs["product"].unique()))

    rxns = sorted(universe)

    spaces = {
        "element": list(refs.ELEMENT_ORDER),
        "met": mets,
        "rxn": rxns,
        "method": sorted(pairs["method"].dropna().unique()),
        "source": sorted(pairs["source"].dropna().unique()),
    }
    vocab = refs.build_vocab(spaces)
    _say("    vocabulary  " + "  ".join(f"{k} {len(v):,}" for k, v in spaces.items()))
    return vocab, refs.Vocab(vocab)


def encode_pairs(pairs: pd.DataFrame, V: refs.Vocab) -> pd.DataFrame:
    met = V.codes("met")
    enc = pd.DataFrame({
        "element": pd.Series(V.encode("element", pairs["element"].to_numpy()),
                             dtype=np.uint8),
        "rxn": pd.Series(_encode_checked(V, "rxn", pairs["mnxr"].to_numpy(),
                                         what="atom_pairs"), dtype=np.uint32),
        "tail_met": np.fromiter((met[s] for s in pairs["substrate"]), np.uint16,
                                count=len(pairs)),
        "tail_rank": pairs["sub_idx"].to_numpy().astype(np.uint16),
        "head_met": np.fromiter((met[s] for s in pairs["product"]), np.uint16,
                                count=len(pairs)),
        "head_rank": pairs["prod_idx"].to_numpy().astype(np.uint16),
        "pair_w": pairs["pair_w"].to_numpy().astype(np.float32),
        "method": pd.Series(V.encode("method", pairs["method"].to_numpy()), dtype=np.uint8),
        "source": pd.Series(V.encode("source", pairs["source"].to_numpy()), dtype=np.uint8),
        "confidence": pairs["confidence"].to_numpy().astype(np.float32),
    })
    return enc.sort_values(list(refs.ATOM_PAIRS_SORT),
                           kind="mergesort").reset_index(drop=True)


def encode_direction(direction: pd.DataFrame, V: refs.Vocab) -> pd.DataFrame:
    enc = pd.DataFrame({
        "rxn": pd.Series(_encode_checked(V, "rxn", direction["mnxr"].to_numpy(),
                                         what="direction"), dtype=np.uint32),
        "ratio": direction["ratio"].to_numpy().astype(np.float64),
        "dir_tier": direction["dir_tier"].to_numpy().astype(np.uint8),
    })
    return enc.sort_values("rxn", kind="mergesort").reset_index(drop=True)


def write_pairs(enc: pd.DataFrame, identity: dict, out: Path,
                file_meta: dict | None = None) -> Path:
    table = pa.Table.from_pandas(enc, preserve_index=False)
    md = dict(table.schema.metadata or {})
    md.update(refs.identity_metadata(identity, file_meta))
    schema = table.schema.with_metadata(md)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    codes = enc["element"].to_numpy()
    with pq.ParquetWriter(out, schema, compression="zstd",
                          compression_level=refs.ZSTD_LEVEL) as w:
        for code in np.unique(codes):
            w.write_table(table.filter(pa.array(codes == code)))
    return out


def bake_pairs(src_pairs: Path, src_reactions: Path,
               out_vocab: Path, out_pairs: Path) -> dict:
    _say("== bake metabolism: vocabulary + atom pairs ==")
    pairs, universe, ap_sha = read_pairs_source(src_pairs, src_reactions)

    t0 = time.perf_counter()
    vocab, V = build_vocabulary(pairs, universe)
    max_rank = int(max(pairs["sub_idx"].max(), pairs["prod_idx"].max()))
    met_bits, rank_bits = refs.bit_widths(V.size("met"), max_rank)
    _say(f"    widths      met {met_bits} + rank {rank_bits} = node {met_bits+rank_bits} bits, "
         f"edge {2*(met_bits+rank_bits)} of {refs.NODE_KEY_BUDGET}  "
         f"(max atom rank {max_rank}, headroom to {(1<<rank_bits)-1})")
    _say(f"  vocabulary                         {(time.perf_counter()-t0)*1000:7.0f} ms")

    t0 = time.perf_counter()
    enc_pairs = encode_pairs(pairs, V)
    _say(f"  encode + sort                      {(time.perf_counter()-t0)*1000:7.0f} ms")

    identity = {
        "bake_version": refs.BAKE_VERSION,
        "src_atom_pairs_sha256": ap_sha,
        "src_atom_pairs_rows": int(len(pairs)),
        "vocab_sha256": refs.vocab_sha256(vocab),
        "n_element": V.size("element"),
        "n_met": V.size("met"),
        "n_rxn": V.size("rxn"),
        "n_method": V.size("method"),
        "n_source": V.size("source"),
        "met_bits": met_bits,
        "rank_bits": rank_bits,
        "max_atom_rank": max_rank,
        "element_order": list(refs.ELEMENT_ORDER),
        "orientation": refs.ORIENTATION,
        "atom_pairs_rows": int(len(enc_pairs)),
    }

    t0 = time.perf_counter()
    refs.write_with_identity(pa.Table.from_pandas(vocab, preserve_index=False),
                             out_vocab, identity)
    write_pairs(enc_pairs, identity, out_pairs)
    _say(f"  write                              {(time.perf_counter()-t0)*1000:7.0f} ms")

    baked = (Path(out_vocab), Path(out_pairs))
    for p in baked:
        _say(f"    {p.name:22s} {p.stat().st_size/1e6:7.2f} MB")
    _say(f"    row groups in atom_pairs: {pq.ParquetFile(out_pairs).num_row_groups} "
         f"(one per element)")
    _say(f"    reaction coverage: {pairs['mnxr'].nunique():,} of {V.size('rxn'):,} "
         f"reactions carry atom pairs")
    _say(f"    bake {identity['vocab_sha256'][:16]}")
    return identity


def bake_direction(src_direction: Path, in_vocab: Path, out_direction: Path) -> dict:
    _say("== bake metabolism: direction ==")
    if not Path(in_vocab).exists():
        raise SystemExit(
            f"missing vocabulary {in_vocab}. Direction is coded against the AAM "
            f"assembly's vocabulary and cannot mint its own -- a second vocabulary would "
            f"produce a trio that assert_same_bake refuses.")
    identity = refs.read_identity(in_vocab)
    if identity.get("bake_version") != refs.BAKE_VERSION:
        raise SystemExit(
            f"{in_vocab} was written at bake_version {identity.get('bake_version')} but "
            f"this bake_metabolism speaks {refs.BAKE_VERSION}; the rxn codes do not mean "
            f"the same thing")
    V = refs.load_vocab(in_vocab)
    _say(f"    vocabulary  rxn {V.size('rxn'):,}  bake "
         f"{identity['vocab_sha256'][:16]}  (inherited)")

    direction, dir_sha = read_direction_source(src_direction)

    t0 = time.perf_counter()
    enc_dir = encode_direction(direction, V)
    _say(f"  encode + sort                      {(time.perf_counter()-t0)*1000:7.0f} ms")

    file_meta = {
        "src_direction_sha256": dir_sha,
        "src_direction_rows": int(len(direction)),
        "direction_rows": int(len(enc_dir)),
    }

    t0 = time.perf_counter()
    refs.write_with_identity(pa.Table.from_pandas(enc_dir, preserve_index=False),
                             out_direction, identity, file_meta=file_meta)
    _say(f"  write                              {(time.perf_counter()-t0)*1000:7.0f} ms")
    _say(f"    {Path(out_direction).name:22s} "
         f"{Path(out_direction).stat().st_size/1e6:7.2f} MB")
    _say(f"    reaction coverage: {len(enc_dir):,} of {V.size('rxn'):,} reactions scored "
         f"({V.size('rxn') - len(enc_dir):,} default to ratio 1.0)")
    return identity


class _Checks:
    def __init__(self):
        self.failures: list[str] = []

    def __call__(self, label, ok, detail=""):
        print(f"[{'PASS' if ok else 'FAIL'}] {label}" + (f" -- {detail}" if detail else ""),
              flush=True)
        if not ok:
            self.failures.append(label)

    def report(self) -> int:
        _say()
        if self.failures:
            _say(f"{len(self.failures)} FAILED: " + "; ".join(self.failures))
            return 1
        _say("all checks passed")
        return 0


def selftest_pairs(src_pairs: Path, src_reactions: Path,
                   out_vocab: Path, out_pairs: Path) -> int:
    check = _Checks()

    _say("== selftest: vocabulary + atom pairs are a re-encoding and nothing more ==")
    ident = refs.assert_same_bake(out_vocab, out_pairs)
    check("bake identity agrees across vocab and atom_pairs", True,
          f"bake {ident['vocab_sha256'][:16]}")

    pairs = pd.read_parquet(src_pairs)
    universe = set(pd.read_parquet(src_reactions, columns=["mnxr"])["mnxr"])
    V = refs.load_vocab(out_vocab)
    enc = refs.load_atom_pairs(out_pairs)

    check("source atom_pairs unchanged since the bake",
          refs.sha256_file(src_pairs) == ident["src_atom_pairs_sha256"])
    check("vocabulary hash matches the identity block",
          refs.vocab_sha256(V.df) == ident["vocab_sha256"])

    check("reaction space is the reaction universe",
          set(V.symbols("rxn")) == universe,
          f"{V.size('rxn'):,} reactions")
    check("recorded n_rxn matches the vocabulary", ident["n_rxn"] == V.size("rxn"))

    check("atom_pairs row census preserved", len(enc) == len(pairs),
          f"{len(enc):,} vs {len(pairs):,}")
    check("zero-weight pair rows preserved",
          int((enc["pair_w"].to_numpy() == 0).sum()) == int((pairs["pair_w"] == 0).sum()),
          f"{int((enc['pair_w'].to_numpy()==0).sum()):,} rows")

    t0 = time.perf_counter()
    rank_bits = ident["rank_bits"]
    src = pairs.sort_values(["element", "mnxr", "substrate", "product",
                             "sub_idx", "prod_idx"], kind="mergesort").reset_index(drop=True)
    met_sym = V.symbols("met")
    rxn_sym = V.symbols("rxn")
    el_sym = V.symbols("element")
    tail_node, head_node = refs.pack_pairs(enc, rank_bits)
    tm, ti = refs.unpack_node(tail_node, rank_bits)
    hm, hi = refs.unpack_node(head_node, rank_bits)
    dec = pd.DataFrame({
        "element": el_sym[enc["element"].to_numpy()],
        "mnxr": rxn_sym[enc["rxn"].to_numpy()],
        "substrate": met_sym[tm], "product": met_sym[hm],
        "sub_idx": ti, "prod_idx": hi,
        "pair_w": enc["pair_w"].to_numpy().astype(float),
    }).sort_values(["element", "mnxr", "substrate", "product", "sub_idx", "prod_idx"],
                   kind="mergesort").reset_index(drop=True)

    for col in ("element", "mnxr", "substrate", "product"):
        check(f"round trip exact: {col}",
              bool((dec[col].to_numpy() == src[col].to_numpy()).all()))
    for col in ("sub_idx", "prod_idx"):
        check(f"round trip exact: {col}",
              bool((dec[col].to_numpy() == src[col].to_numpy()).all()))

    dw = np.abs(dec["pair_w"].to_numpy() - src["pair_w"].to_numpy())
    rel = dw / np.maximum(np.abs(src["pair_w"].to_numpy()), 1.0)
    check("round trip within float32: pair_w", float(rel.max()) <= 1e-6,
          f"max relative error {float(rel.max()):.2e}")
    _say(f"       ({len(enc):,} rows verified in {(time.perf_counter()-t0)*1000:.0f} ms)")

    key = ["element", "mnxr", "substrate", "sub_idx"]
    gs = src.groupby(key, observed=True)["pair_w"].sum()
    gd = dec.groupby(key, observed=True)["pair_w"].sum()
    gd = gd.reindex(gs.index)
    check("per-source-atom weight sums preserved",
          bool((np.abs(gd.to_numpy() - gs.to_numpy())
                <= 1e-6 * np.maximum(gs.to_numpy(), 1.0)).all()),
          f"{len(gs):,} source atoms, max drift "
          f"{float(np.abs(gd.to_numpy()-gs.to_numpy()).max()):.2e}")

    max_rank = int(max(pairs["sub_idx"].max(), pairs["prod_idx"].max()))
    check("recorded max atom rank matches the source", ident["max_atom_rank"] == max_rank,
          f"{max_rank}")
    check("rank field is wide enough for the observed maximum",
          max_rank < (1 << ident["rank_bits"]),
          f"{max_rank} < {1 << ident['rank_bits']}")
    check("metabolite field is wide enough for the vocabulary",
          ident["n_met"] <= (1 << ident["met_bits"]),
          f"{ident['n_met']:,} <= {1 << ident['met_bits']:,}")
    check("edge key fits in int64",
          2 * (ident["met_bits"] + ident["rank_bits"]) <= refs.NODE_KEY_BUDGET)

    return check.report()


def selftest_direction(src_direction: Path, out_vocab: Path, out_direction: Path) -> int:
    check = _Checks()

    _say("== selftest: direction is a re-encoding and nothing more ==")
    ident = refs.assert_same_bake(out_vocab, out_direction)
    check("direction carries the vocabulary's bake identity, byte for byte", True,
          f"bake {ident['vocab_sha256'][:16]}")

    fmeta = refs.read_file_meta(out_direction)
    direction = pd.read_parquet(src_direction, columns=["mnxr", "ratio", "dir_tier"])
    V = refs.load_vocab(out_vocab)
    enc_dir = refs.load_direction(out_direction)
    rxn_sym = V.symbols("rxn")

    check("per-file provenance block is present",
          set(fmeta) == {"src_direction_sha256", "src_direction_rows", "direction_rows"},
          f"{sorted(fmeta)}")
    check("source direction unchanged since the bake",
          refs.sha256_file(src_direction) == fmeta.get("src_direction_sha256"))
    check("direction row census preserved", len(enc_dir) == len(direction),
          f"{len(enc_dir):,} vs {len(direction):,}")

    dsrc = direction.set_index("mnxr")
    ddec = pd.DataFrame({
        "mnxr": rxn_sym[enc_dir["rxn"].to_numpy()],
        "ratio": enc_dir["ratio"].to_numpy().astype(float),
        "dir_tier": enc_dir["dir_tier"].to_numpy(),
    }).set_index("mnxr").reindex(dsrc.index)
    check("every source reaction survives the round trip",
          not bool(ddec["ratio"].isna().any()),
          f"{int(ddec['ratio'].isna().sum()):,} missing")
    check("round trip exact: dir_tier",
          bool((ddec["dir_tier"].to_numpy() == dsrc["dir_tier"].to_numpy()).all()))
    check("round trip exact: ratio",
          bool((ddec["ratio"].to_numpy() == dsrc["ratio"].to_numpy()).all()),
          f"float64 preserved; ratios span {dsrc['ratio'].min():.1e} to "
          f"{dsrc['ratio'].max():.1e}")
    n_flip_src = int((dsrc["ratio"] > 1.0).sum())
    check("edge-flipping ratio count preserved",
          int((ddec["ratio"] > 1.0).sum()) == n_flip_src,
          f"{n_flip_src:,} of {len(dsrc):,} ratios exceed 1")

    lut = refs.ratio_by_code(V, enc_dir)
    check("ratio lookup spans the whole reaction space", len(lut) == V.size("rxn"),
          f"{int((lut != 1.0).sum()):,} of {len(lut):,} reactions carry a non-unit ratio")

    return check.report()


def cmd_pairs(args):
    bake_pairs(Path(args.aam_pairs), Path(args.reactions),
               Path(args.out_vocab), Path(args.out_pairs))
    _say()
    return selftest_pairs(Path(args.aam_pairs), Path(args.reactions),
                          Path(args.out_vocab), Path(args.out_pairs))


def cmd_direction(args):
    bake_direction(Path(args.direction), Path(args.vocab), Path(args.out))
    _say()
    return selftest_direction(Path(args.direction), Path(args.vocab), Path(args.out))


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("pairs", help="mint the vocabulary and encode the atom pairs")
    p.set_defaults(fn=cmd_pairs)
    p.add_argument("--aam-pairs", required=True, help="the AAM assembly's stacked table")
    p.add_argument("--reactions", required=True,
                   help="lookup::reactions -- the reaction universe the vocabulary is "
                        "coded against")
    p.add_argument("--out-vocab", required=True)
    p.add_argument("--out-pairs", required=True)

    p = sub.add_parser("direction", help="encode direction against an existing vocabulary")
    p.set_defaults(fn=cmd_direction)
    p.add_argument("--direction", required=True,
                   help="the direction assembly's combined annotation")
    p.add_argument("--vocab", required=True,
                   help="ref::metabolism_vocab from the AAM assembly. This step does not "
                        "mint a vocabulary -- it inherits one, identity block and all.")
    p.add_argument("--out", required=True)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
