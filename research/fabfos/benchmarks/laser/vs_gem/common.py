from __future__ import annotations

import json
import re
import sys
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[5]
HERE = Path(__file__).resolve().parent
REFS = HERE / "refs"
OUT = HERE / "out"
CACHE = HERE / "cache"
for _d in (REFS, OUT, CACHE):
    _d.mkdir(parents=True, exist_ok=True)

EXTRACTION = ROOT / "data" / "fabfos" / "benchmarks" / "laser" / "extraction.tsv"
ATOM_PAIRS = ROOT / "data" / "fabfos" / "benchmark" / "reference_tier4" / "atom_pairs_tier4.parquet"
TIER4_FREEZE = ROOT / "data" / "fabfos" / "benchmark" / "reference_tier4" / "TIER4_FREEZE.md"
BAKE = ROOT / "data" / "fabfos" / "processed" / "metabolism_bake"
MNX = ROOT / "data" / "fabfos" / "originals" / "metanetx" / "4.5"
CHEM_PROP = MNX / "chem_prop.tsv"
CHEM_XREF = MNX / "chem_xref.tsv"
REAC_PROP = MNX / "reac_prop.tsv"
REAC_XREF = MNX / "reac_xref.tsv"
sys.path.insert(0, str(ROOT / "src" / "metasmith_libraries" / "resources" / "lib"))
import fabfos_evidence as FE                                          # noqa: E402

RUNS = ROOT / "data" / "fabfos" / "runs"
LIB = ROOT / "src" / "metasmith_libraries" / "resources" / "lib"

POOL_SEED = 20260809
POOL_N = 500

HOST_GEM_DIR = {"iML1515": "e_coli_k12", "iECDH10B": "e_coli_dh10b"}

MUTATION_TOKENS = frozenset(
    {"aa_snps", "nuc_snps", "indel", "frameshift", "mutated", "truncated",
     "is_insertion"})

ELEMENTS = ("C", "N", "P", "S")


def _genes(row) -> list:
    if not isinstance(row, str) or not row.strip():
        return []
    try:
        return json.loads(row)
    except json.JSONDecodeError:
        return []


def has_mutation(genes_json: str) -> bool:
    for g in _genes(genes_json):
        if "mut" in (g.get("actions") or []):
            return True
        for tok in (g.get("mutation") or "").split(","):
            if tok.strip() in MUTATION_TOKENS:
                return True
    return False


def split_mnxr(cell) -> list:
    if not isinstance(cell, str) or not cell.strip() or cell.strip().lower() == "nan":
        return []
    return [t.strip() for t in cell.split(",") if t.strip()]


def load_extraction() -> pd.DataFrame:
    df = pd.read_csv(EXTRACTION, sep="\t", dtype={"add_mnxr": str, "del_mnxr": str})
    df["has_mutation"] = df.genes_json.apply(has_mutation)
    df["add_rxns"] = df.add_mnxr.apply(split_mnxr)
    df["del_rxns"] = df.del_mnxr.apply(split_mnxr)
    df["is_probe"] = df.medium.isna() & df.carbon.isna()
    df["host_dir"] = df.host_gem.map(HOST_GEM_DIR)
    return df


def scored_set(df: pd.DataFrame) -> pd.DataFrame:
    return df[~df.has_mutation].copy()


def assert_census(df: pd.DataFrame, native: dict | None = None) -> dict:
    raw = pd.read_csv(EXTRACTION, sep="\t", nrows=5, dtype=str)
    cell = pd.read_csv(EXTRACTION, sep="\t", dtype=str).add_mnxr.dropna()
    n_comma = cell.str.count(",").sum()
    n_semi = cell.str.count(";").sum()
    assert n_semi == 0 and n_comma > 0, (
        f"add_mnxr delimiter changed: {n_comma} commas, {n_semi} semicolons")
    del raw

    s = scored_set(df)
    got = {
        "n_raw": len(df),
        "n_mutation_lost": int(df.has_mutation.sum()),
        "n_scored": len(s),
        "n_probe": int(s.is_probe.sum()),
        "n_add_rxn_universe": len({r for rs in s.add_rxns for r in rs}),
        "n_del_rxn_universe": len({r for rs in s.del_rxns for r in rs}),
        "n_deletion_only": int((s.add_rxns.apply(len) == 0).sum()),
        "median_total_edits": float(
            (s.add_rxns.apply(len) + s.del_rxns.apply(len)).median()),
    }
    want = {"n_raw": 382, "n_mutation_lost": 147, "n_scored": 235,
            "n_add_rxn_universe": 472, "n_del_rxn_universe": 222,
            "n_deletion_only": 50, "median_total_edits": 5.0}
    bad = {k: (got[k], v) for k, v in want.items() if got[k] != v}
    if bad:
        raise AssertionError(f"census drifted (got, want): {bad}")

    if native is not None:
        het = s.apply(
            lambda r: any(x not in native.get(r.host_dir, set()) for x in r.add_rxns),
            axis=1)
        got["n_no_heterologous_add"] = int((~het).sum())
        got["n_all_native_add"] = int((~het & (s.add_rxns.apply(len) > 0)).sum())
        if got["n_no_heterologous_add"] != 122 or got["n_all_native_add"] != 72:
            raise AssertionError(
                f"heterologous strata drifted: {got['n_no_heterologous_add']} "
                f"no-het (want 122), {got['n_all_native_add']} all-native (want 72)")
    return got


_WS = re.compile(r"\s+")
_NONALNUM = re.compile(r"[^a-z0-9]+")


def norm_conservative(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).strip().casefold()
    s = _WS.sub(" ", s).rstrip(".")
    return s


def norm_aggressive(s: str) -> str:
    return _NONALNUM.sub("", norm_conservative(s))


def atom_universe(element: str = "C") -> set:
    p = pd.read_parquet(ATOM_PAIRS, columns=["element", "substrate", "product"])
    p = p[p.element == element]
    return set(p["substrate"].unique()) | set(p["product"].unique())


def read_gpr(path):
    return FE.read_gpr(path)


def host_native_reactions(host_dir: str) -> set:
    g = read_gpr(RUNS / host_dir / "gpr" / "gpr_gem.parquet")
    return set(g.mnxr.astype(str).unique())


def empirical_p(observed: float, null: np.ndarray, tail: str = "greater") -> float:
    null = np.asarray(null, float)
    null = null[np.isfinite(null)]
    if null.size == 0:
        return float("nan")
    if tail == "greater":
        exceed = int((null >= observed).sum())
    elif tail == "less":
        exceed = int((null <= observed).sum())
    else:
        exceed = int((np.abs(null) >= abs(observed)).sum())
    return (exceed + 1) / (null.size + 1)


def bh_fdr(pvals, alpha: float = 0.05):
    p = np.asarray(pvals, float)
    ok = np.isfinite(p)
    q = np.full(p.shape, np.nan)
    idx = np.flatnonzero(ok)
    if idx.size == 0:
        return q, np.zeros(p.shape, bool)
    order = idx[np.argsort(p[idx], kind="stable")]
    n = order.size
    ranked = p[order] * n / np.arange(1, n + 1)
    q[order] = np.minimum.accumulate(ranked[::-1])[::-1].clip(max=1.0)
    return q, (q <= alpha) & ok
