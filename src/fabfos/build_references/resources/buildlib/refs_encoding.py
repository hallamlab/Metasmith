"""Encoding and bake identity for the compiled metabolism reference tables.

Ported from the pre-library ``build_references/refs.py``, with one change: every
path constant is gone. The original opened ``data/raw/mnxref-4_5/atom_pairs.parquet``
and wrote ``data/reference/metabolism/`` by name, which is exactly the coupling the
transform library removes -- a transform is handed its inputs and told where its
outputs go, and a module that knows a repo layout cannot run inside one. Functions
that used to default to a constant now take the path.

The compiled tables are three parquet files:

    vocab.parquet        int-code vocabulary, 5 kinds
    atom_pairs.parquet   the atom-atom mapping
    direction.parquet    per-reaction directionality

They are ONE artifact in three files. Each carries the same bake-identity block in its
parquet file-level key-value metadata, and :func:`assert_same_bake` refuses a set whose
blocks disagree -- because ``atom_pairs`` stores metabolites as integer codes, reading it
against a different bake's ``vocab`` decodes every node to the wrong metabolite and
raises nothing at all.

Two writers, one block
----------------------
The trio is produced by two steps, not one: the AAM assembly mints the vocabulary and
encodes the pairs, the direction assembly encodes ratios against that vocabulary. The
identity block is therefore MINTED ONCE and INHERITED verbatim -- the direction writer
reads it off ``vocab.parquet`` with :func:`read_identity` and passes it through
unchanged, so ``assert_same_bake`` holds by construction rather than by two independent
computations happening to agree. Anything a writer knows only about its OWN file goes in
a second, per-file block (:data:`BAKE_FILE_KEY`, see :func:`read_file_meta`), which
``assert_same_bake`` does not compare; that is where the direction table's source hash
and row counts live.

This is also why the reaction space is the REACTION UNIVERSE (``lookup::reactions``)
rather than the union of the two tables: a union cannot be computed by a writer that has
only seen one of them, and coding against the universe is a superset of coding against
either, so the hazard the union was guarding against becomes structurally impossible
instead of contingent.

Node codes
----------
A node is ``(metabolite, canonical_atom_rank)``, and the compile path wants it as a
single integer so edges can be factorised without tuples::

    node_code = met_code << rank_bits | atom_rank

That packing happens at LOAD time, not at rest. On disk the two fields stay separate
(``tail_met`` / ``tail_rank``), because fusing them makes every value distinct and
destroys the run-length dictionary encoding parquet gets on a 34k-symbol metabolite
column: measured on the real table, packed-at-rest is 16.75 MB against 9.71 MB split,
while packing at load costs 4 ms. :func:`pack_pairs` does it on the way in.

``met_bits`` and ``rank_bits`` are MEASURED at bake time from the vocabulary and the
observed maximum rank, never hardcoded, and recorded in the identity block. Getting
``rank_bits`` too small is the dangerous failure: the rank field bleeds into the
metabolite field, two distinct atoms merge into one node, and the merge RAISES the
network's conductance -- it looks like an improvement. Hence the assertion at bake time
and the width check on every load.

Ratios stay float64
-------------------
``pair_w`` and ``confidence`` narrow to float32 (~3e-8 relative, four orders below the
spread of the evidence weights they multiply). ``ratio`` does NOT. The consumer's flip
test is a threshold at exactly 1.0, and four reactions in the real table sit within
float32 epsilon of it (e.g. MNXR112716 at 1.0000000000016507): narrowing silently
reorients those four edges. A re-encoding may not change topology.

Orientation
-----------
``atom_pairs`` stores tail/head exactly as the source table writes substrate/product.
The ``ratio > 1`` edge flip stays a consumer-side operation (see
``ecspr.model.build.graph_from_pairs``): 10,485 of the 83,796 ratios exceed 1, so a bake that
pre-flipped would double-apply against a consumer that also flips, silently restoring
the unfavoured direction.

Build side, not run side
------------------------
This module lives in ``buildlib::`` because everything that calls it today compiles a
reference. :func:`compile_atom_graph` is the exception in kind -- it READS a finished
bake -- and it is here because the reference gate is what calls it, to check the compiled
tables against ``ecspr.model.build.graph_from_pairs`` on the string tables. When a run-side
transform first needs to read a bake, that half moves to ``lib::``; until then, shipping
it in the wheel would ship a reader nothing calls.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# 2: the reaction space moved from the union of the two source tables to the reaction
# universe, and the block is minted by one writer and inherited by the other. Both change
# what `rxn` codes MEAN, so a v1 file and a v2 file may not be mixed -- which is what the
# version check in assert_same_bake is for.
BAKE_VERSION = 2
BAKE_KEY = b"ecspr_bake"
# Facts about ONE file, which assert_same_bake deliberately does not compare. A writer
# that has not seen the other tables cannot compute their hashes, and demanding it did is
# what would force the trio back into a single step.
BAKE_FILE_KEY = b"ecspr_bake_file"
ORIENTATION = "as_written"

# The four elements the atom mapping covers. Order is the element code assignment and
# is recorded in the identity block, so a future fifth element cannot silently renumber
# an existing bake's codes.
ELEMENT_ORDER = ("C", "N", "P", "S")

# One vocabulary file, five id spaces, each densely coded WITHIN its kind. A single
# global space would push metabolites past 16 bits for no gain and would make the width
# assertion disagree with the encoder.
VOCAB_KINDS = ("element", "met", "rxn", "method", "source")

# int64 has 63 usable bits; an edge key packs two node keys, so a node key may not
# exceed 31. Asserted at bake time rather than assumed.
NODE_KEY_BUDGET = 62

ATOM_PAIRS_COLS = ("element", "rxn", "tail_met", "tail_rank", "head_met", "head_rank",
                   "pair_w", "method", "source", "confidence")
# The sort key is every id column, in this order. Sorting on the full key rather than a
# prefix is what makes the file compress: measured 9.71 MB fully sorted against 11.58 MB
# sorted on (element, rxn, tail_met) alone.
ATOM_PAIRS_SORT = ("element", "rxn", "tail_met", "tail_rank", "head_met", "head_rank")
DIRECTION_COLS = ("rxn", "ratio", "dir_tier")
VOCAB_COLS = ("kind", "code", "symbol")
ZSTD_LEVEL = 9


# =====================================================================
# bit widths and node packing
# =====================================================================

def bit_widths(n_met: int, max_atom_rank: int) -> tuple[int, int]:
    """Measured widths for ``(met_bits, rank_bits)``, with the packing budget asserted.

    Both are computed from the data, not chosen. ``n_met`` is the metabolite vocabulary
    size (max code is ``n_met - 1``); ``max_atom_rank`` is the largest atom rank seen
    anywhere in the source table -- over ALL elements, not one slice. Measuring it on the
    carbon slice alone gives 492 where the true maximum is 496, and that kind of
    off-by-a-slice is exactly what silently truncates a node key.
    """
    if n_met < 1:
        raise ValueError("empty metabolite vocabulary")
    if max_atom_rank < 0:
        raise ValueError(f"negative atom rank: {max_atom_rank}")
    met_bits = max(1, int(n_met - 1).bit_length())
    rank_bits = max(1, int(max_atom_rank).bit_length())
    total = 2 * (met_bits + rank_bits)
    if total > NODE_KEY_BUDGET:
        raise ValueError(
            f"node key does not fit: met_bits={met_bits} + rank_bits={rank_bits} "
            f"-> edge key {total} bits > {NODE_KEY_BUDGET}. Widen the key type or "
            f"re-scope the vocabulary; do NOT truncate.")
    return met_bits, rank_bits


def pack_node(met_code, atom_rank, rank_bits: int) -> np.ndarray:
    """``(met_code, atom_rank) -> node_code``, with the rank overflow checked."""
    met_code = np.asarray(met_code, np.int64)
    atom_rank = np.asarray(atom_rank, np.int64)
    if atom_rank.size and int(atom_rank.max()) >= (1 << rank_bits):
        raise ValueError(
            f"atom rank {int(atom_rank.max())} does not fit in {rank_bits} bits; "
            f"packing it would merge two distinct atoms onto one node")
    if atom_rank.size and int(atom_rank.min()) < 0:
        raise ValueError("negative atom rank")
    return ((met_code << rank_bits) | atom_rank).astype(np.uint32)


def unpack_node(node_code, rank_bits: int) -> tuple[np.ndarray, np.ndarray]:
    """``node_code -> (met_code, atom_rank)``."""
    nc = np.asarray(node_code, np.int64)
    return (nc >> rank_bits).astype(np.int64), (nc & ((1 << rank_bits) - 1)).astype(np.int64)


def pack_edge(tail_node, head_node, node_bits: int) -> np.ndarray:
    """Two node codes into one int64, for factorising edges without tuples."""
    t = np.asarray(tail_node, np.int64)
    h = np.asarray(head_node, np.int64)
    return (t << node_bits) | h


def pack_pairs(pairs: pd.DataFrame, rank_bits: int) -> tuple[np.ndarray, np.ndarray]:
    """``(tail_met, tail_rank, head_met, head_rank) -> (tail_node, head_node)``.

    The on-disk split into four columns is a storage decision (see the module
    docstring); every consumer wants the packed form, so it is one call away.
    """
    return (pack_node(pairs["tail_met"].to_numpy(), pairs["tail_rank"].to_numpy(), rank_bits),
            pack_node(pairs["head_met"].to_numpy(), pairs["head_rank"].to_numpy(), rank_bits))


# =====================================================================
# vocabulary
# =====================================================================

class Vocab:
    """The three-column vocabulary table, with per-kind lookups both ways."""

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self._sym: dict[str, np.ndarray] = {}
        self._code: dict[str, dict] = {}
        for kind, g in df.groupby("kind", sort=False):
            g = g.sort_values("code")
            codes = g["code"].to_numpy()
            if not np.array_equal(codes, np.arange(len(g))):
                raise ValueError(f"vocabulary kind {kind!r} is not densely coded from 0")
            self._sym[str(kind)] = g["symbol"].to_numpy()
            self._code[str(kind)] = {s: i for i, s in enumerate(g["symbol"])}

    def __contains__(self, kind: str) -> bool:
        return kind in self._sym

    def symbols(self, kind: str) -> np.ndarray:
        """``code -> symbol``, as an array indexed by code."""
        return self._sym[kind]

    def codes(self, kind: str) -> dict:
        """``symbol -> code``."""
        return self._code[kind]

    def size(self, kind: str) -> int:
        return len(self._sym[kind])

    def encode(self, kind: str, symbols) -> np.ndarray:
        """Vectorised ``symbol -> code``. Unknown symbols become -1, never a raise --
        an absent metabolite is a coverage fact the caller must be able to see."""
        m = self._code[kind]
        return np.fromiter((m.get(s, -1) for s in symbols), np.int64, count=len(symbols))


def build_vocab(spaces: dict[str, list]) -> pd.DataFrame:
    """``{kind: [symbol, ...]}`` -> the three-column table, sorted by kind then code."""
    rows = []
    for kind in VOCAB_KINDS:
        for code, symbol in enumerate(spaces[kind]):
            rows.append((kind, code, symbol))
    df = pd.DataFrame(rows, columns=list(VOCAB_COLS))
    df["code"] = df["code"].astype(np.uint32)
    return df


def vocab_sha256(vocab: pd.DataFrame) -> str:
    """Hash of the vocabulary's CONTENT, not its file bytes.

    The hash has to go inside vocab.parquet as well as the other two, so it cannot be a
    hash of the file. Hashing the canonical tuples also makes it stable across parquet
    encoders and compression settings, which a file hash is not.
    """
    h = hashlib.sha256()
    for kind, code, symbol in vocab[list(VOCAB_COLS)].itertuples(index=False):
        h.update(f"{kind}\t{int(code)}\t{symbol}\n".encode())
    return h.hexdigest()


# =====================================================================
# bake identity
# =====================================================================

def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def identity_metadata(identity: dict, file_meta: dict | None = None) -> dict:
    """The two file-level KV entries a baked table carries.

    Separated from the writers so that ``write_pairs``, which drives its own
    ``ParquetWriter`` to get one row group per element, cannot drift from this one.
    """
    md = {BAKE_KEY: json.dumps(identity, sort_keys=True).encode()}
    if file_meta is not None:
        md[BAKE_FILE_KEY] = json.dumps(file_meta, sort_keys=True).encode()
    return md


def write_with_identity(table: pa.Table, path: Path, identity: dict,
                        file_meta: dict | None = None, **kw) -> Path:
    """Write a parquet file carrying the bake identity in its file-level KV metadata.

    ``file_meta`` is this file's own provenance -- source hash, row counts -- and is
    written under a separate key precisely so :func:`assert_same_bake` does not compare
    it across the trio.
    """
    md = dict(table.schema.metadata or {})
    md.update(identity_metadata(identity, file_meta))
    table = table.replace_schema_metadata(md)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    kw.setdefault("compression_level", ZSTD_LEVEL)
    pq.write_table(table, path, compression="zstd", **kw)
    return path


def read_identity(path) -> dict:
    md = pq.read_schema(path).metadata or {}
    raw = md.get(BAKE_KEY)
    if raw is None:
        raise ValueError(
            f"{path} carries no bake identity -- it was not written by "
            f"compile/bake_metabolism.py, or it was rewritten by a tool that dropped "
            f"the file-level metadata")
    return json.loads(raw.decode())


def read_file_meta(path) -> dict:
    """This file's own provenance block, or ``{}`` for a file that carries none.

    Absent is a legitimate answer -- ``vocab.parquet`` has nothing to say about itself
    that is not already in the shared block -- so this returns empty rather than raising,
    unlike :func:`read_identity`.
    """
    md = pq.read_schema(path).metadata or {}
    raw = md.get(BAKE_FILE_KEY)
    return {} if raw is None else json.loads(raw.decode())


def assert_same_bake(*paths) -> dict:
    """Every file must carry a byte-identical SHARED identity block. Returns it.

    Refusing here is the point: atom_pairs stores metabolites as codes into vocab, so a
    mismatched pair decodes to the wrong metabolites and produces a graph rather than an
    error. The trio must be passed explicitly -- there is no default set, because there
    is no fixed location a bake lives at any more.

    Only :data:`BAKE_KEY` is compared. Each file's own provenance lives under
    :data:`BAKE_FILE_KEY` and is expected to differ -- see :func:`read_file_meta`.
    """
    if not paths:
        raise TypeError("assert_same_bake needs the trio's paths; there is no default set")
    paths = [Path(p) for p in paths]
    ids = {p: read_identity(p) for p in paths}
    first_path, first = next(iter(ids.items()))
    for p, ident in ids.items():
        if ident != first:
            diff = sorted(k for k in set(ident) | set(first)
                          if ident.get(k) != first.get(k))
            raise ValueError(
                f"bake identity mismatch between {first_path.name} and {p.name}; "
                f"disagreeing keys: {diff}")
    if first.get("bake_version") != BAKE_VERSION:
        raise ValueError(
            f"bake_version {first.get('bake_version')} but this refs_encoding speaks "
            f"{BAKE_VERSION}")
    met_bits, rank_bits = first["met_bits"], first["rank_bits"]
    if 2 * (met_bits + rank_bits) > NODE_KEY_BUDGET:
        raise ValueError("recorded bit widths exceed the packing budget")
    return first


# =====================================================================
# loading
# =====================================================================

def load_vocab(path) -> Vocab:
    return Vocab(pd.read_parquet(path))


def load_atom_pairs(path, element: str | None = None, vocab: Vocab | None = None) -> pd.DataFrame:
    """The atom-pair table, optionally pruned to one element at the parquet layer.

    The file is written with one row group per element, so the element filter really
    does skip row groups rather than reading everything and masking.
    """
    path = Path(path)
    if element is None:
        return pd.read_parquet(path)
    if vocab is None:
        raise TypeError("filtering by element needs the vocab, to map it to its code")
    code = vocab.codes("element").get(element)
    if code is None:
        raise KeyError(f"element {element!r} is not in this bake "
                       f"(have {list(vocab.codes('element'))})")
    return pd.read_parquet(path, filters=[("element", "==", code)])


def load_direction(path) -> pd.DataFrame:
    return pd.read_parquet(path)


def ratio_by_code(vocab: Vocab, direction: pd.DataFrame) -> np.ndarray:
    """``rxn_code -> ratio``, defaulting to 1.0 for reactions the table does not cover.

    1.0 is the undirected limit, i.e. the most permissive value. Every default is
    therefore MORE conductance than the evidence supports, which is why the reaction
    vocabulary is the whole reaction universe -- so that this array is fully populated
    and the default is never silently taken for a reaction the direction ensemble
    actually scored. Coding against the universe rather than against the two tables'
    union makes that a property of the vocabulary itself rather than of what direction
    happened to contain.
    """
    out = np.ones(vocab.size("rxn"), float)
    out[direction["rxn"].to_numpy()] = direction["ratio"].to_numpy().astype(float)
    return out


# =====================================================================
# compiling one element's graph
# =====================================================================

def compile_atom_graph(element: str, weights: dict, *, ident: dict, vocab: Vocab,
                       pairs: pd.DataFrame, direction: pd.DataFrame | None = None,
                       ratios: bool = True, use_confidence: bool = False,
                       ratio_lut: np.ndarray | None = None, meta: dict | None = None):
    """Build one element's :class:`AtomGraph` from the baked tables.

    Semantically identical to ``ecspr.model.build.graph_from_pairs`` -- same edge definition,
    same ``ratio > 1`` flip, same parallel-edge summation, same drop of zero-conductance
    and self-loop rows -- but the join, the flip and the edge factorisation all happen on
    integers, and metabolite symbols are decoded only for the surviving unique nodes.
    That equivalence is what the reference gate checks.

    ``weights`` is ``{mnxr: E_r}`` keyed by STRING reaction id, because a reaction with
    evidence but no atom pairs must remain expressible; it is mapped to codes once here.

    ``pairs`` may be the whole table or one element's slice; it is filtered here either
    way. ``ident`` is the trio's verified bake identity (see :func:`assert_same_bake`) --
    passed in rather than re-read, so one gate run cannot check one bake and compile
    another.

    Node *order* differs from ``graph_from_pairs`` (the baked table is sorted, so
    first-seen order differs); node and edge *counts*, and every conductance, do not.
    """
    from ecspr.model.graph import AtomGraph  # deferred: only the compile path needs scipy

    ecode = vocab.codes("element")[element]
    pairs = pairs[pairs["element"].to_numpy() == ecode]

    rank_bits = ident["rank_bits"]
    n_rxn_in = len(weights)

    # weights: string -> code, once. Reactions outside the vocabulary are the AAM gap and
    # are counted, not dropped silently.
    rxn_codes = vocab.codes("rxn")
    er_by_code = np.zeros(vocab.size("rxn"), float)
    n_unknown = 0
    for mnxr, e in weights.items():
        c = rxn_codes.get(mnxr)
        if c is None:
            n_unknown += 1
            continue
        er_by_code[c] = float(e)
    live = er_by_code > 0.0

    rxn = pairs["rxn"].to_numpy()
    sel = live[rxn]
    d = pairs[sel]
    rxn = rxn[sel]
    if len(d) == 0:
        return AtomGraph([], [], np.zeros(0), np.zeros(0),
                         dict(meta or {}, element=element, n_reactions_requested=n_rxn_in,
                              n_reactions_used=0, n_aam_gap=n_rxn_in, n_nodes=0, n_edges=0,
                              n_weights_off_vocab=n_unknown))

    gp = er_by_code[rxn] * d["pair_w"].to_numpy().astype(float)
    if use_confidence:
        gp = gp * d["confidence"].to_numpy().astype(float)

    if ratios:
        if ratio_lut is None:
            if direction is None:
                raise TypeError("ratios=True needs the direction table or a ratio_lut")
            ratio_lut = ratio_by_code(vocab, direction)
        ratio = ratio_lut[rxn]
    else:
        ratio = np.ones(len(d))

    # ratio > 1 => the reaction runs against the way its equation is written: flip the
    # edge and invert the ratio. Direction evidence may only ever throttle, never
    # amplify -- taking gm = ratio * gp literally at ratio 3e17 short-circuits the graph.
    tail_node, head_node = pack_pairs(d, rank_bits)
    flip = ratio > 1.0
    tail = np.where(flip, head_node, tail_node).astype(np.int64)
    head = np.where(flip, tail_node, head_node).astype(np.int64)
    ratio = np.where(flip, 1.0 / np.maximum(ratio, np.finfo(float).tiny), ratio)
    gm = ratio * gp

    keep = (gp > 0) & (tail != head)
    n_used = int(np.unique(rxn).size)
    if not keep.any():
        return AtomGraph([], [], np.zeros(0), np.zeros(0),
                         dict(meta or {}, element=element, n_reactions_requested=n_rxn_in,
                              n_reactions_used=n_used, n_aam_gap=n_rxn_in - n_used,
                              n_nodes=0, n_edges=0, n_weights_off_vocab=n_unknown))
    tail, head, gp, gm = tail[keep], head[keep], gp[keep], gm[keep]

    # One edge per distinct ORDERED (tail, head); parallel rows sum onto it. Packing the
    # pair into one int64 keeps the factorise on integers instead of tuples.
    node_bits = ident["met_bits"] + rank_bits
    ecodes, euniq = pd.factorize(pack_edge(tail, head, node_bits), sort=False)
    ne = len(euniq)
    gp_e = np.bincount(ecodes, gp, minlength=ne)
    gm_e = np.bincount(ecodes, gm, minlength=ne)

    # representative row per edge, so tail/head are read once per EDGE not per row
    first = np.zeros(ne, np.int64)
    first[ecodes[::-1]] = np.arange(len(ecodes))[::-1]
    et, eh = tail[first], head[first]

    ncodes, nuniq = pd.factorize(np.concatenate([et, eh]), sort=False)
    met_code, atom_rank = unpack_node(nuniq, rank_bits)
    met_sym = vocab.symbols("met")
    nodes = [(str(met_sym[m]), int(r)) for m, r in zip(met_code, atom_rank)]
    edges = list(zip(ncodes[:ne].tolist(), ncodes[ne:].tolist()))

    m = dict(meta or {})
    m.update(element=element,
             n_reactions_requested=n_rxn_in,
             n_reactions_used=n_used,
             n_aam_gap=n_rxn_in - n_used,
             n_pair_rows=int(len(d)),
             n_nodes=len(nodes), n_edges=len(edges),
             n_metabolites=int(np.unique(met_code).size),
             n_directed_rows=int((ratio != 1.0).sum()),
             n_reversed_rows=int(flip.sum()),
             n_weights_off_vocab=n_unknown,
             bake=ident["vocab_sha256"][:16])
    return AtomGraph(nodes, edges, gp_e, gm_e, m)
