"""Atom-transfer PAIRS from cached atom-mapping output: which metabolite's atoms
land in which metabolite, per element.

WHY THIS EXISTS
---------------
The bipartite met-rxn graph the solve runs on is a STAR: a reaction is a node joined
to every metabolite it touches. Eliminating that node -- which is exactly what the
Woodbury update's `_build_delta` does -- leaves a CLIQUE over every participant, so
every pair of participants gets conductance whether or not one atom passes between
them. The edge weights are atom counts, so the method looks stoichiometry-aware; the
TOPOLOGY throws the atom mapping away.

Measured on the real carbon graph: 52.8% of the reaction-induced metabolite pairs
transfer no atoms at all. On MNXR106432 (acetyl-CoA + CO2 + NADPH = pyruvate + NADP+
+ CoA) the pyruvate-NADPH channel carries ZERO carbons at conductance 0.700, while
the real one-carbon pyruvate-CO2 channel gets 0.033 -- a zero-atom channel 21x more
conductive than a real one.

The information needed to fix it already exists and is already computed. The unifier
(`20b_aam_unify.py:count_transits`) holds the substrate metabolite and the product
metabolite of each mapped atom IN THE SAME LOOP ITERATION, and then discards the
pairing into two independent role counters:

    sm = sub_mnxms[si]                       # substrate MNXM   <- the pair
    pm = prod_mnxms[j]                       # product MNXM     <- is right here
    if sm: counts[el][f"{sm}:substrate"] += 1     # and then thrown
    if pm: counts[el][f"{pm}:product"]   += 1     # away, separately

This module keeps `(sm, pm)`. Everything else is the same computation.

RE-EXTRACTION, NOT RE-MAPPING. The mapped SMILES are cached with their atom-map
numbers; nothing here runs RXNMapper or LocalMapper. This is arithmetic over a cache.

WHY A METABOLITE-LEVEL PAIR IS STILL NOT ENOUGH
-----------------------------------------------
Worth stating because it is the trap this whole design walks around. Pairing at the
METABOLITE level does not fix the stated problem. The true atom map splits
acetyl-CoA's 23 carbons by destination: atoms [0,1] -- the acetyl group -- become
pyruvate; the other 21 -- the CoA moiety -- become CoA. So acetyl-CoA -> CoA is a
GENUINE 21-carbon edge that any metabolite-level graph keeps, and a path arriving on
acetyl carbons could still leave through the CoA moiety carrying 21 carbons it never
brought. Only atom-resolved nodes deny that, which is why this module emits the atom
INDICES and not merely the counts -- the indices are what the class refinement
downstream partitions on.

WHAT IS REFUSED, AND WHY -- AND THE TWO REFUSALS THAT WERE BUGS
---------------------------------------------------------------
This module's first version refused 40% of the mapper universe and reported that as a
coverage finding about the method's reach. It was not. Two of the three refusals were
defects in THIS FILE, and they cost the atom lane 29 of the curated set4 axes:

  * `stripped` (19,930 rxns, 34.6%) was a REGEX MISMATCH, not unbalanced chemistry.
    The story told here -- "generic metabolites with no SMILES were dropped before
    mapping, so the mapper re-routed their atoms" -- describes a real phenomenon in a
    DIFFERENT file (`aam_stripped_input.tsv`), which this module never reads and which
    is disjoint from the universe it does read. The real cause: `EQ_TERM` matched
    `WATER`/`BIOMASS`, which the SMILES builder's regex does not, so the metabolite
    count exceeded the template count and every water-bearing reaction was refused.
    See `EQ_TERM`. Measured: 30,938 of 30,939 metabolites in the universe HAVE a
    canonical SMILES -- nothing was dropped for lack of structure.
  * `ambiguous_duplicate` (3,255 rxns) was 98.7% SELF-INFLICTED: the same metabolite
    twice is not an ambiguity. See `match_mols`. Only 43 were real.
  * DUPLICATE-METABOLITE reactions where two DISTINCT MNXMs share one canonical SMILES
    are genuinely ambiguous -- `pop(0)` picks between different metabolites -- and stay
    refused. That refusal is real.

The lesson worth keeping: a refusal rate is not evidence about the world until the
refusals have been read. Reporting 37.8% coverage as a property of the atom mapping,
when most of it was this file disagreeing with a regex, put a false limitation into a
report and nearly retired a set of biologically real axes. Every status here is
counted and reported so the next reader can check rather than believe.

Env: rdkit + pandas. No SCADC paths: every input arrives as an argument.
"""
from __future__ import annotations

import argparse
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
from rdkit import Chem, RDLogger
from rdkit.Chem import AllChem

RDLogger.DisableLog("rdApp.*")

ELEMENTS = ("C", "N", "S", "P")

EQ_TERM = re.compile(r"(\d+(?:\.\d+)?)\s+(MNXM\w+)@\w+")

PAIR_COLS = ("mnxr", "element", "substrate", "product", "n_atoms",
             "sub_idx", "prod_idx", "pair_w")
STATUS_COLS = ("mnxr", "status", "n_sub", "n_prod", "n_mapped_sub", "n_mapped_prod")


def canon_smiles(smi: str):
    if not smi:
        return None
    try:
        mol = Chem.MolFromSmiles(smi)
    except Exception:
        return None
    if mol is None:
        return None
    for a in mol.GetAtoms():
        a.SetAtomMapNum(0)
    try:
        return Chem.MolToSmiles(mol, canonical=True)
    except Exception:
        return None


def load_mnxm_smiles(chem_prop: Path, want: set) -> dict:
    out = {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 9 or p[0] not in want:
                continue
            if p[8].strip():
                out[p[0]] = p[8].strip()
    return out


def load_mnxm_formulas(chem_prop: Path, want: set) -> dict:
    out = {}
    with open(chem_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) < 9 or p[0] not in want:
                continue
            if p[3].strip():
                out[p[0]] = p[3].strip()
    return out


def load_equations(reac_prop: Path, want: set) -> dict:
    out = {}
    with open(reac_prop) as fh:
        for line in fh:
            if line.startswith("#") or not line.strip():
                continue
            p = line.rstrip("\n").split("\t")
            if len(p) > 1 and p[0] in want:
                out[p[0]] = p[1]
    return out


def parse_equation(eq: str):
    if "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)

    def expand(s):
        out = []
        for coef, m in EQ_TERM.findall(s):
            for _ in range(max(1, int(round(float(coef))))):
                out.append(m)
        return out

    L, R = expand(lhs), expand(rhs)
    return (L, R) if (L and R) else None


def frag_keys(mol):
    m2 = Chem.Mol(mol)
    for a in m2.GetAtoms():
        a.SetAtomMapNum(0)
    try:
        Chem.SanitizeMol(m2)
    except Exception:
        return None, None
    try:
        cs = Chem.MolToSmiles(m2, canonical=True)
    except Exception:
        cs = None
    conn = None
    try:
        ik = Chem.MolToInchiKey(m2)
        if ik and "-" in ik:
            conn = ik.split("-", 1)[0]
    except Exception:
        pass
    return cs, conn


def match_mols(mols: list, mnxms: list, canon: dict, conn: dict = None):
    pool = defaultdict(list)
    kpool = defaultdict(list)
    for m in mnxms:
        cs = canon.get(m)
        if cs:
            pool[cs].append(m)
        if conn:
            k = conn.get(m)
            if k:
                kpool[k].append(m)
    cand = {cs: sorted(set(v)) for cs, v in pool.items()}
    kcand = {k: sorted(set(v)) for k, v in kpool.items()}
    diluted = any(len(v) > 1 for v in cand.values())
    out = []
    for mol in mols:
        if mol is None:
            out.append([])
            continue
        cs, k = frag_keys(mol)
        c = cand.get(cs) if cs else None
        if not c and conn and k:
            c = kcand.get(k)
            if c and len(c) > 1:
                diluted = True
        if not c:
            out.append([])
        else:
            w = 1.0 / len(c)
            out.append([(m, w) for m in c])
    return out, diluted


def multiplicity_dilution(named: list, mnxms: list):
    mult = Counter(mnxms)
    claims = Counter()
    for cands in named:
        for m, _w in cands:
            claims[m] += 1
    scale = {m: min(1.0, mult.get(m, 0) / n) for m, n in claims.items() if n}
    if all(s >= 1.0 for s in scale.values()):
        return named, False
    return ([[(m, w * scale.get(m, 1.0)) for m, w in cands] for cands in named],
            True)


def canonical_ranks(mol):
    m2 = Chem.Mol(mol)
    for a in m2.GetAtoms():
        a.SetAtomMapNum(0)
    try:
        Chem.SanitizeMol(m2)
        return list(Chem.CanonicalRankAtoms(m2, breakTies=True))
    except Exception:
        return None


FORMULA_TERM = re.compile(r"([A-Z][a-z]?)(\d*)")


def count_element(formula: str, X: str):
    if not formula or not isinstance(formula, str) or formula.strip() in ("", "*"):
        return None
    if "*" in formula or "(" in formula or ")" in formula:
        return None
    n, seen = 0, False
    for sym, num in FORMULA_TERM.findall(formula):
        if not sym:
            continue
        if sym == X:
            seen = True
            n += int(num) if num else 1
    return n if seen else 0


def forced_pairs(sub_mnxms: list, prod_mnxms: list, formulas: dict, ranks_of: dict):
    out = {}
    for X in ELEMENTS:
        sx, px = [], []
        bad = False
        for side, ms in ((sx, sub_mnxms), (px, prod_mnxms)):
            for m in set(ms):
                c = count_element(formulas.get(m), X)
                if c is None:
                    bad = True
                    break
                if c:
                    side.append((m, c))
            if bad:
                break
        if bad or len(sx) != 1 or len(px) != 1:
            continue
        (sm, sn), (pm, pn) = sx[0], px[0]
        if sn != pn or sm == pm:
            continue
        sr, pr = ranks_of.get((sm, X)), ranks_of.get((pm, X))
        if not sr or not pr or len(sr) != sn or len(pr) != pn:
            continue
        w = 1.0 / sn
        out[(X, sm, pm)] = [(a, b, w) for a in sr for b in pr]
    return out


def distinct_structures(mnxms: list, smiles_of: dict) -> int:
    seen, n = set(), 0
    for m in mnxms:
        s = smiles_of.get(m)
        key = s if s is not None else ("\0unknown", m)
        if key not in seen:
            seen.add(key)
            n += 1
    return n


def reduce_for_element(sub_mnxms: list, prod_mnxms: list, formulas: dict, X: str):
    keep_s, keep_p, n_s, n_p = [], [], 0, 0
    for side, keep, ms in ((0, keep_s, sub_mnxms), (1, keep_p, prod_mnxms)):
        for m in ms:
            c = count_element(formulas.get(m), X)
            if c is None:
                return None
            if c:
                keep.append(m)
                if side == 0:
                    n_s += c
                else:
                    n_p += c
    if not keep_s or not keep_p or n_s != n_p:
        return None
    return keep_s, keep_p


def pairs_from_mapped(mapped_smi: str, sub_mnxms: list, prod_mnxms: list,
                      canon: dict, conn: dict = None, align: str = "strict",
                      collapsed_counts: tuple | None = None):
    if not mapped_smi or pd.isna(mapped_smi):
        return {}, "no_mapping"
    try:
        rxn = AllChem.ReactionFromSmarts(mapped_smi, useSmiles=True)
    except Exception:
        return {}, "unparseable"
    if rxn is None:
        return {}, "unparseable"

    n_sub_t = rxn.GetNumReactantTemplates()
    n_prod_t = rxn.GetNumProductTemplates()
    sub_mols = [rxn.GetReactantTemplate(i) for i in range(n_sub_t)]
    prod_mols = [rxn.GetProductTemplate(j) for j in range(n_prod_t)]

    if align == "strict":
        exact = (n_sub_t == len(sub_mnxms) and n_prod_t == len(prod_mnxms))
        as_collapsed = (collapsed_counts is not None
                        and (n_sub_t, n_prod_t) == tuple(collapsed_counts))
        if not (exact or as_collapsed):
            return {}, "stripped"

    if align == "structural":
        fwd_s, a_s = match_mols(sub_mols, sub_mnxms, canon, conn)
        fwd_p, a_p = match_mols(prod_mols, prod_mnxms, canon, conn)
        rev_s, b_s = match_mols(prod_mols, sub_mnxms, canon, conn)
        rev_p, b_p = match_mols(sub_mols, prod_mnxms, canon, conn)
        n_fwd = sum(1 for x in fwd_s if x) + sum(1 for x in fwd_p if x)
        n_rev = sum(1 for x in rev_s if x) + sum(1 for x in rev_p if x)
        flipped = n_rev > n_fwd
        if flipped:
            sub_mols, prod_mols = prod_mols, sub_mols
            sub_named, prod_named = rev_s, rev_p
            amb_s, amb_p = b_s, b_p
        else:
            sub_named, prod_named = fwd_s, fwd_p
            amb_s, amb_p = a_s, a_p
        sub_named, ov_s = multiplicity_dilution(sub_named, sub_mnxms)
        prod_named, ov_p = multiplicity_dilution(prod_named, prod_mnxms)
        amb_s, amb_p = amb_s or ov_s, amb_p or ov_p
    else:
        sub_named, amb_s = match_mols(sub_mols, sub_mnxms, canon, conn)
        prod_named, amb_p = match_mols(prod_mols, prod_mnxms, canon, conn)

    sub_ranks = [canonical_ranks(m) for m in sub_mols]
    prod_ranks = [canonical_ranks(m) for m in prod_mols]
    if any(r is None for r in sub_ranks) or any(r is None for r in prod_ranks):
        return {}, "unrankable"

    sub_index = {}
    for i, mol in enumerate(sub_mols):
        for a in mol.GetAtoms():
            n = a.GetAtomMapNum()
            if n > 0:
                sub_index[n] = (i, sub_ranks[i][a.GetIdx()], a.GetSymbol())

    pairs = defaultdict(list)
    for j, mol in enumerate(prod_mols):
        for a in mol.GetAtoms():
            n = a.GetAtomMapNum()
            if n <= 0:
                continue
            el = a.GetSymbol()
            if el not in ELEMENTS:
                continue
            src = sub_index.get(n)
            if src is None:
                continue
            si, s_rank, sel = src
            if sel != el:
                continue
            sub_cands = sub_named[si] if si < len(sub_named) else []
            prod_cands = prod_named[j] if j < len(prod_named) else []
            if not sub_cands or not prod_cands:
                continue
            p_rank = prod_ranks[j][a.GetIdx()]
            for sm, ws in sub_cands:
                for pm, wp in prod_cands:
                    pairs[(el, sm, pm)].append((s_rank, p_rank, ws * wp))
    if not pairs:
        return {}, "no_pairs"
    return dict(pairs), ("ambiguous_diluted" if (amb_s or amb_p) else "ok")


def load_placeholders(path: Path):
    d = pd.read_csv(path, sep="\t")
    return dict(zip(d.mnxm, d.smiles))


def load_resolved(path: Path):
    from io import StringIO
    _kept = [ln for ln in Path(path).read_text().splitlines() if not ln.lstrip().startswith("#")]
    d = pd.read_csv(StringIO("\n".join(_kept)), sep="\t")
    return dict(zip(d.mnxm, d.smiles))


def load_balance(path: Path):
    d = pd.read_csv(path, sep="\t")
    return {(r.mnxr, r.element): bool(r.balanced) for r in d.itertuples(index=False)}


def cmd_extract(args):
    aam = pd.concat([pd.read_csv(a, sep="\t") for a in args.aam], ignore_index=True)
    aam = aam.drop_duplicates(subset="mnxr", keep="first")
    if args.min_confidence is not None and "confidence" in aam.columns:
        aam = aam[aam.confidence >= args.min_confidence]
    mnxrs = set(aam.mnxr)
    print(f"[atom-pairs] {len(aam):,} mapped reactions from "
          f"{', '.join(Path(a).name for a in args.aam)}", flush=True)

    ph = load_placeholders(Path(args.placeholders)) if args.placeholders else {}
    res = load_resolved(Path(args.resolved)) if args.resolved else {}
    bal = load_balance(Path(args.balance)) if args.balance else {}
    if ph:
        print(f"[atom-pairs] {len(ph):,} placeholder generics (atoms suppressed)",
              flush=True)
    if res:
        print(f"[atom-pairs] {len(res):,} curated structures (atoms KEPT -- they are the "
              f"metabolite's own)", flush=True)
        both = set(ph) & set(res)
        if both:
            raise SystemExit(f"[atom-pairs] {sorted(both)} are both placeheld and "
                             f"resolved -- a metabolite is scaffolding or it is real, "
                             f"not both")
    if bal:
        nbad = sum(1 for v in bal.values() if not v)
        print(f"[atom-pairs] {len(bal):,} (rxn, element) balance verdicts; "
              f"{nbad:,} refused as unbalanced", flush=True)

    partial_of = {}
    class_of = {}
    if args.universe:
        pu = pd.read_parquet(args.universe)
        has_class = "submission_class" in pu.columns
        for r in pu.itertuples(index=False):
            class_of[r.mnxr] = str(r.submission_class) if has_class else ""
            el = getattr(r, "element", None)
            if el is None or pd.isna(el):
                continue
            partial_of[r.mnxr] = (str(r.base_mnxr), str(el), list(r.sub_mnxms),
                                  list(r.prod_mnxms))
        print(f"[atom-pairs] universe: {len(pu):,} submissions, of which "
              f"{len(partial_of):,} are element reductions read for ONE element",
              flush=True)

    want = set()
    parsed = {}
    for key, (_base, _el, ks, kp) in partial_of.items():
        parsed[key] = (ks, kp)
        want |= set(ks) | set(kp)
    rest = mnxrs - set(partial_of)
    if rest:
        eqs = load_equations(Path(args.reac_prop), rest)
        for r, eq in eqs.items():
            pe = parse_equation(eq)
            if pe:
                parsed[r] = pe
                want |= set(pe[0]) | set(pe[1])
    print(f"[atom-pairs] {len(parsed):,} templates "
          f"({len(partial_of):,} reduced, {len(parsed) - len(partial_of):,} from the "
          f"equation); {len(want):,} metabolites", flush=True)

    raw = load_mnxm_smiles(Path(args.chem_prop), want)
    raw.update({m: s for m, s in ph.items() if m in want})
    raw.update({m: s for m, s in res.items() if m in want})
    canon, conn = {}, {}
    for m, smi in raw.items():
        mol = Chem.MolFromSmiles(smi)
        if mol is None:
            continue
        cs, k = frag_keys(mol)
        if cs:
            canon[m] = cs
        if k:
            conn[m] = k
    print(f"[atom-pairs] {len(canon):,} metabolites with canonical SMILES, "
          f"{len(conn):,} with an InChIKey connectivity key", flush=True)

    formulas, ranks_of = {}, {}
    if args.fallback_forced:
        formulas = load_mnxm_formulas(Path(args.chem_prop), want)
        for m, smi in raw.items():
            mol = Chem.MolFromSmiles(smi)
            if mol is None:
                continue
            rk = canonical_ranks(mol)
            if rk is None:
                continue
            for X in ELEMENTS:
                idx = [a.GetIdx() for a in mol.GetAtoms() if a.GetSymbol() == X]
                if idx:
                    ranks_of[(m, X)] = [rk[i] for i in idx]
        print(f"[atom-pairs] fallback armed: {len(formulas):,} formulas, "
              f"{len(ranks_of):,} (metabolite, element) rank sets", flush=True)

    rows, status_rows = [], []
    tally = defaultdict(int)
    for k, rec in enumerate(aam.itertuples(index=False)):
        r = rec.mnxr
        pe = parsed.get(r)
        if pe is None:
            tally["no_equation"] += 1
            status_rows.append(dict(mnxr=r, status="no_equation", n_sub=0, n_prod=0,
                                    n_mapped_sub=0, n_mapped_prod=0))
            continue
        subs, prods = pe
        pairs, status = pairs_from_mapped(
            getattr(rec, "mapped_rxn_smiles", None), subs, prods, canon,
            conn=conn if args.connectivity_fallback else None, align=args.align,
            collapsed_counts=(distinct_structures(subs, raw),
                              distinct_structures(prods, raw)))
        if not pairs and status == "no_mapping" and args.fallback_forced:
            fp = forced_pairs(subs, prods, formulas, ranks_of)
            if fp:
                pairs, status = fp, "forced"
        if pairs and (ph or bal):
            pairs = {(el, sm, pm): v for (el, sm, pm), v in pairs.items()
                     if sm not in ph and pm not in ph
                     and bal.get((r, el), True)}
            if not pairs:
                status = "placeholder_only"
        out_mnxr = r
        if r in partial_of:
            out_mnxr, only = partial_of[r][0], partial_of[r][1]
            dropped = {el for el, _s, _p in pairs} - {only}
            if dropped:
                tally[f"other elements discarded unread"] += len(dropped)
            pairs = {(el, sm, pm): v for (el, sm, pm), v in pairs.items() if el == only}
            if not pairs and status in ("ok", "ambiguous_diluted"):
                status = "no_pairs_for_element"
        tally[status] += 1
        status_rows.append(dict(mnxr=r, status=status, n_sub=len(subs),
                                n_prod=len(prods),
                                n_mapped_sub=len({p[1] for p in pairs}),
                                n_mapped_prod=len({p[2] for p in pairs})))
        for (el, sm, pm), idxs in pairs.items():
            rows.append(dict(mnxr=out_mnxr, element=el, substrate=sm, product=pm,
                             n_atoms=len(idxs),
                             sub_idx=",".join(str(i) for i, _, _ in idxs),
                             prod_idx=",".join(str(j) for _, j, _ in idxs),
                             pair_w=",".join(repr(float(w)) for _, _, w in idxs),
                             submission_class=class_of.get(r, "")))
        if (k + 1) % 10000 == 0:
            print(f"[atom-pairs]   {k+1:,}/{len(aam):,}", flush=True)

    df = pd.DataFrame(rows, columns=list(PAIR_COLS) + ["submission_class"])
    sdf = pd.DataFrame(status_rows, columns=list(STATUS_COLS))
    df.to_parquet(args.out, index=False)
    sdf.to_csv(args.out_status, sep="\t", index=False)

    print(f"\n[atom-pairs] {len(df):,} (rxn, element, sub, prod) pairs "
          f"over {df.mnxr.nunique():,} reactions -> {args.out}")
    print("\n[atom-pairs] per-reaction outcome:")
    for s, n in sorted(tally.items(), key=lambda x: -x[1]):
        print(f"    {s:22s} {n:>7,}  ({100*n/len(aam):5.1f}%)")
    if len(df):
        print("\n[atom-pairs] pairs per element:")
        for el, g in df.groupby("element"):
            print(f"    {el}: {len(g):>7,} pairs  {g.n_atoms.sum():>9,} atoms  "
                  f"{g.mnxr.nunique():>6,} reactions")
    return 0


def cmd_selftest(args):
    smi = ("CC(=O)[S:1][CH2:2][CH2:3][NH:4][C:5](=[O:6])[CH2:7][CH2:8]"
           ">>[CH3:9][C:10](=[O:11])[C:12](=[O:13])[OH:14]")
    rxn = AllChem.ReactionFromSmarts(smi, useSmiles=True)
    print(f"parsed: {rxn is not None} "
          f"({rxn.GetNumReactantTemplates()} sub / {rxn.GetNumProductTemplates()} prod)")
    print("selftest is a parse check only; the real check is `verify` against MNX.")
    return 0


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("extract"); p.set_defaults(fn=cmd_extract)
    p.add_argument("--aam", required=True, nargs="+",
                   help="cached mapper output(s): mnxr, mapped_rxn_smiles, confidence. "
                        "Repeatable, so a rescued universe (ecspr_aam_rescue) can be "
                        "concatenated with the base one; first file wins on duplicates.")
    p.add_argument("--placeholders", default=None,
                   help="ecspr_aam_rescue placeholder map. Names the stand-in fragments "
                        "so they match, then suppresses their atoms from the pairs.")
    p.add_argument("--resolved", default=None,
                   help="ecspr_aam_rescue curated structure crosswalk. Supplies structures "
                        "MetaNetX lacks. NOT placeholders: their atoms are kept, because "
                        "they are the metabolite's own.")
    p.add_argument("--balance", default=None,
                   help="ecspr_aam_rescue per-(reaction, element) concrete-balance "
                        "verdicts; unbalanced elements are dropped for that reaction.")
    p.add_argument("--universe", default=None,
                   help="an `interm::aam_universe` submission table. Rows carrying an "
                        "`element` are element reductions: their participant lists are "
                        "read from here rather than from the equation, and every OTHER "
                        "element's map is discarded unread because for those the "
                        "reduction really is a strip. Rows without one are whole "
                        "reactions and take the equation-derived template as always.")
    p.add_argument("--reac-prop", required=True)
    p.add_argument("--chem-prop", required=True)
    p.add_argument("--out", required=True, help="pairs parquet")
    p.add_argument("--out-status", required=True, help="per-reaction outcome tsv")
    p.add_argument("--fallback-forced", action="store_true",
                   help="for reactions the mapper returned nothing for (its 512-token "
                        "limit, not a chemistry verdict), emit the pairs conservation "
                        "FORCES: one substrate and one product carrying the element, one "
                        "atom each. Only n==1; multi-atom cases stay refused because the "
                        "atom correspondence would have to be invented.")
    p.add_argument("--align", choices=("strict", "structural"), default="strict",
                   help="strict: WE built the reaction SMILES, so template count must "
                        "equal participant count and a mismatch means atoms were "
                        "re-routed. structural: a foreign database wrote it over its "
                        "own participant set, so the counts are expected to differ and "
                        "naming is by structure alone. Use structural for MetaCyc and "
                        "strict for anything built from reac_prop.")
    p.add_argument("--connectivity-fallback", action="store_true",
                   help="when a template's canonical SMILES matches no participant, "
                        "retry on the InChIKey connectivity block -- which is invariant "
                        "to the protonation and tautomer differences MetaCyc and "
                        "MetaNetX have about the same molecule, and to nothing else")
    p.add_argument("--min-confidence", type=float, default=None,
                   help="omit to keep the mapper's whole universe (tier B applies no "
                        "confidence gate; the acetyl-CoA split is chemically correct "
                        "at confidence 0.032, so a gate here would cost real pairs)")

    p = sub.add_parser("selftest"); p.set_defaults(fn=cmd_selftest)
    return ap.parse_args(argv)


if __name__ == "__main__":
    a = parse_args()
    sys.exit(a.fn(a))
