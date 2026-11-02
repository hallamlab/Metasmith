from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BAKE_VERSION = 2
BAKE_KEY = b"ecspr_bake"
BAKE_FILE_KEY = b"ecspr_bake_file"
ORIENTATION = "as_written"

ELEMENT_ORDER = ("C", "N", "P", "S")

VOCAB_KINDS = ("element", "met", "rxn", "method", "source")

NODE_KEY_BUDGET = 62

ATOM_PAIRS_COLS = ("element", "rxn", "tail_met", "tail_rank", "head_met", "head_rank",
                   "pair_w", "method", "source", "confidence")
ATOM_PAIRS_SORT = ("element", "rxn", "tail_met", "tail_rank", "head_met", "head_rank")
DIRECTION_COLS = ("rxn", "ratio", "dir_tier")
VOCAB_COLS = ("kind", "code", "symbol")
ZSTD_LEVEL = 9


def bit_widths(n_met: int, max_atom_rank: int) -> tuple[int, int]:
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
    nc = np.asarray(node_code, np.int64)
    return (nc >> rank_bits).astype(np.int64), (nc & ((1 << rank_bits) - 1)).astype(np.int64)


def pack_edge(tail_node, head_node, node_bits: int) -> np.ndarray:
    t = np.asarray(tail_node, np.int64)
    h = np.asarray(head_node, np.int64)
    return (t << node_bits) | h


def pack_pairs(pairs: pd.DataFrame, rank_bits: int) -> tuple[np.ndarray, np.ndarray]:
    return (pack_node(pairs["tail_met"].to_numpy(), pairs["tail_rank"].to_numpy(), rank_bits),
            pack_node(pairs["head_met"].to_numpy(), pairs["head_rank"].to_numpy(), rank_bits))


class Vocab:
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
        return self._sym[kind]

    def codes(self, kind: str) -> dict:
        return self._code[kind]

    def size(self, kind: str) -> int:
        return len(self._sym[kind])

    def encode(self, kind: str, symbols) -> np.ndarray:
        m = self._code[kind]
        return np.fromiter((m.get(s, -1) for s in symbols), np.int64, count=len(symbols))


def build_vocab(spaces: dict[str, list]) -> pd.DataFrame:
    rows = []
    for kind in VOCAB_KINDS:
        for code, symbol in enumerate(spaces[kind]):
            rows.append((kind, code, symbol))
    df = pd.DataFrame(rows, columns=list(VOCAB_COLS))
    df["code"] = df["code"].astype(np.uint32)
    return df


def vocab_sha256(vocab: pd.DataFrame) -> str:
    h = hashlib.sha256()
    for kind, code, symbol in vocab[list(VOCAB_COLS)].itertuples(index=False):
        h.update(f"{kind}\t{int(code)}\t{symbol}\n".encode())
    return h.hexdigest()


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def identity_metadata(identity: dict, file_meta: dict | None = None) -> dict:
    md = {BAKE_KEY: json.dumps(identity, sort_keys=True).encode()}
    if file_meta is not None:
        md[BAKE_FILE_KEY] = json.dumps(file_meta, sort_keys=True).encode()
    return md


def write_with_identity(table: pa.Table, path: Path, identity: dict,
                        file_meta: dict | None = None, **kw) -> Path:
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
            f"ecspr.bake.metabolism, or it was rewritten by a tool that dropped "
            f"the file-level metadata")
    return json.loads(raw.decode())


def read_file_meta(path) -> dict:
    md = pq.read_schema(path).metadata or {}
    raw = md.get(BAKE_FILE_KEY)
    return {} if raw is None else json.loads(raw.decode())


def assert_same_bake(*paths) -> dict:
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


def load_vocab(path) -> Vocab:
    return Vocab(pd.read_parquet(path))


def load_atom_pairs(path, element: str | None = None, vocab: Vocab | None = None) -> pd.DataFrame:
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
    out = np.ones(vocab.size("rxn"), float)
    out[direction["rxn"].to_numpy()] = direction["ratio"].to_numpy().astype(float)
    return out


def compile_atom_graph(element: str, weights: dict, *, ident: dict, vocab: Vocab,
                       pairs: pd.DataFrame, direction: pd.DataFrame | None = None,
                       ratios: bool = True, use_confidence: bool = False,
                       ratio_lut: np.ndarray | None = None, meta: dict | None = None):
    from ..model.graph import AtomGraph

    ecode = vocab.codes("element")[element]
    pairs = pairs[pairs["element"].to_numpy() == ecode]

    rank_bits = ident["rank_bits"]
    n_rxn_in = len(weights)

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

    node_bits = ident["met_bits"] + rank_bits
    ecodes, euniq = pd.factorize(pack_edge(tail, head, node_bits), sort=False)
    ne = len(euniq)
    gp_e = np.bincount(ecodes, gp, minlength=ne)
    gm_e = np.bincount(ecodes, gm, minlength=ne)

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
