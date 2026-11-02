# Putting a MetaNetX reaction onto an existing stoichiometric frame.
#
# WHY THIS IS LIBRARY CODE. A de-novo annotation asserts reactions in MNXR space, and a
# curated genome-scale model names its own. Carving a frame to what the annotation
# supports therefore loses every assertion the frame never had a reaction for, and only an
# insertion step puts them back. That step was measured and tuned inside one benchmark
# directory that nothing else referenced, which is how the flux-balance arm came to share
# no code at all with the conductance arm.
#
# FOUR ADMISSION RULES sit on top of the bridge. Each exists because it was measured to
# matter, and the numbers are from the E. coli DH1 frame at a two-lane threshold: 1,713
# MNXR the GEM lacks, 131 of which survive to insertion.
#
# 1. **Take only balanced, non-transport MetaNetX reactions.** MetaNetX marks 39,073 of
#    its reactions unbalanced, and an unbalanced reaction is a free source or sink for
#    whatever it fails to conserve. 675 of the 1,713 fall here, and a further 38 are
#    transport.
#
# 2. **Insert only when the frame holds every metabolite, each as a distinct one.**
#    `MetBridge` will create a metabolite on demand. A created metabolite has exactly one
#    producer and one consumer -- the inserted reaction itself -- so it carries no flux at
#    steady state and the reaction is structurally dead. 380 of the 1,713 fall here.
#
#    Two metabolites collapsing onto one is the same failure wearing a different face. An
#    epimerase reads `substrate + 2 H+ = product`, and both epimers carry one formula and
#    one charge, so the loosest bridge tier maps them to a single frame metabolite. The
#    substrate and the product cancel and `2 H+ ->` survives as a free proton sink. 25
#    fall here, and a further rule rejects whatever reduces to protons and water.
#
# 3. **Re-balance on the frame, correcting protons only.** MetaNetX balances against its
#    own protonation and BiGG protonates differently, so a reaction can leave MetaNetX
#    balanced and land on the frame carrying spare protons. `fadh2_c + 2 h_c + oaa_c =
#    fad_c + mal__L_c` is one of seven such, and each one alone raises growth from 0.982
#    to 1.384 per hour by acting as a proton sink. 25 insertions take a proton correction.
#    7 stay unbalanced after it, which means the bridge reached the wrong metabolite.
#
# 4. **Let a carboxy-lyase run only forwards.** See `_direction`.
#
# **CAUTION** MetaNetX ships no direction, and full reversibility is the right prior for
# an annotation that says only "this gene catalyses this reaction". It is also how an
# inserted reaction completes an energy-generating cycle. Two of those 131 make 1000 units
# of ATP from an empty medium. CALL `close_energy_cycles` AFTER EVERY INSERTION. It is not
# optional and it is not a diagnostic.
#
# Together the four rules and the cycle closure leave the de-novo insertion worth 1.5% of
# growth rate rather than 41%.
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

BIG = 1000.0

#: Prefix for an inserted reaction, so a model can always say which of its reactions came
#: from the annotation rather than from the curated frame.
HET_PREFIX = "HET_"

REJECTIONS = ("no_stoichiometry", "transport", "unbalanced", "unparsable",
              "orphan_metabolite", "collapsed", "trivial", "degenerate",
              "no_formula", "unbalanced_on_frame", "already_present")

#: Metabolites that do not count towards "this reaction still transforms something".
FREE = ("h_c", "h_p", "h_e", "h2o_c", "h2o_p", "h2o_e")

_EQ = re.compile(r"^\s*(\d+(?:\.\d+)?)\s+(MNXM\S+?)@(MNXD\d+)\s*$")
_COMP = re.compile(r"_([a-z]{1,2})$")

_CHEM_PROP_COLS = ["mnxm", "name", "reference", "formula", "charge", "mass",
                   "inchi", "inchikey", "smiles"]
_REAC_PROP_COLS = ["mnxr", "equation", "reference", "classifs",
                   "is_balanced", "is_transport"]


def parse_equation(eq: str):
    """MetaNetX's `1 MNXM01@MNXD1 = ...` form -> `{mnxm: coefficient}`.

    Returns None for a multi-compartment equation. The frame's compartments are its own,
    and a MetaNetX transport reaction says nothing about which of them it crosses.
    """
    if not isinstance(eq, str) or "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)
    out, comps = {}, set()
    for side, sign in ((lhs, -1.0), (rhs, 1.0)):
        for term in side.split("+"):
            term = term.strip()
            if not term:
                continue
            m = _EQ.match(term)
            if not m:
                return None
            coef, mnxm, comp = float(m.group(1)), m.group(2), m.group(3)
            comps.add(comp)
            out[mnxm] = out.get(mnxm, 0.0) + sign * coef
    if len(comps) > 1:
        return None
    return {k: v for k, v in out.items() if v != 0.0} or None


def strip_compartment(bigg_id: str) -> str:
    return _COMP.sub("", bigg_id)


class Release:
    """The MetaNetX tables an insertion reads, from one pinned release directory.

    `chem_prop.tsv` is 1.5M rows and `chem_xref.tsv` larger still, so both are sliced to
    the columns used and cached as parquet beside the release or in `cache`. `reac_prop`
    is read per call and filtered to what was asked for, because the needed set is small
    and the file is not.
    """

    def __init__(self, directory, *, cache=None):
        self.dir = Path(directory)
        self.cache = Path(cache) if cache else self.dir
        for f in ("reac_prop.tsv", "chem_prop.tsv", "chem_xref.tsv"):
            if not (self.dir / f).exists():
                raise FileNotFoundError(f"{self.dir} holds no {f}; --mnx wants a "
                                        f"MetaNetX release directory")
        self._chem = None
        self._bigg = None

    def _cached(self, name, build):
        p = self.cache / name
        if p.exists():
            return pd.read_parquet(p)
        df = build()
        p.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(p, index=False)
        return df

    @property
    def chem_prop(self) -> pd.DataFrame:
        if self._chem is None:
            self._chem = self._cached("chem_prop_slim.parquet", self._read_chem)
        return self._chem

    @property
    def bigg(self) -> pd.DataFrame:
        """`bigg` base id -> `mnxm`, from the `biggM:` rows of `chem_xref`."""
        if self._bigg is None:
            self._bigg = self._cached("biggM_bridge.parquet", self._read_bigg)
        return self._bigg

    def _read_chem(self) -> pd.DataFrame:
        d = pd.read_csv(self.dir / "chem_prop.tsv", sep="\t", comment="#", header=None,
                        names=_CHEM_PROP_COLS, dtype=str, low_memory=False)
        return d[["mnxm", "name", "formula", "charge", "inchikey"]]

    def _read_bigg(self) -> pd.DataFrame:
        x = pd.read_csv(self.dir / "chem_xref.tsv", sep="\t", comment="#", header=None,
                        names=["source", "mnxm", "description"], dtype=str,
                        low_memory=False)
        x = x[x.source.str.startswith("biggM:", na=False)].copy()
        x["bigg"] = x.source.str[len("biggM:"):]
        return x[["bigg", "mnxm"]].drop_duplicates()

    def reac_prop(self, needed) -> dict:
        rp = pd.read_csv(self.dir / "reac_prop.tsv", sep="\t", comment="#", header=None,
                         names=_REAC_PROP_COLS, dtype=str, low_memory=False)
        rp = rp[rp.mnxr.isin(set(needed))]
        return {r.mnxr: dict(equation=r.equation, classifs=r.classifs,
                             is_balanced=r.is_balanced, is_transport=r.is_transport)
                for r in rp.itertuples(index=False)}

    def bridge(self, model) -> "MetBridge":
        return MetBridge(model, self.chem_prop, self.bigg)


class MetBridge:
    """An MNXM -> frame metabolite map, in four tiers of decreasing confidence.

    Identity through the BiGG crosswalk, then InChIKey skeleton, then formula and charge,
    then creation. `insert` never lets it create: a created metabolite is what makes an
    inserted reaction structurally dead, which is admission rule 2.
    """

    def __init__(self, model, chem_prop: pd.DataFrame, bigg: pd.DataFrame):
        self.model = model
        self.tiers = {"direct": 0, "inchikey": 0, "formula_charge": 0,
                      "created": 0, "reused": 0}
        mnxm_by_bigg = bigg.groupby("bigg").mnxm.apply(list).to_dict()

        self.by_mnxm, self.by_ikey, self.by_fc = {}, {}, {}
        p = chem_prop.set_index("mnxm")
        self.formula = p.formula.to_dict()
        self.charge = p.charge.to_dict()
        self.ikey = p.inchikey.to_dict()

        for met in model.metabolites:
            if met.compartment != "c":
                continue
            for mnxm in mnxm_by_bigg.get(strip_compartment(met.id), []):
                self.by_mnxm.setdefault(mnxm, met)
                k = self._ikey14(mnxm)
                if k:
                    self.by_ikey.setdefault(k, met)
                fc = self._fc(mnxm)
                if fc:
                    self.by_fc.setdefault(fc, met)

    def _ikey14(self, mnxm):
        v = self.ikey.get(mnxm)
        return str(v)[:14] if isinstance(v, str) and v else None

    def _fc(self, mnxm):
        f, c = self.formula.get(mnxm), self.charge.get(mnxm)
        if isinstance(f, str) and f and f != "*" and c is not None and not pd.isna(c):
            return (f, str(c))
        return None

    def get(self, mnxm: str, create: bool = True):
        import cobra
        m = self.by_mnxm.get(mnxm)
        if m is not None:
            self.tiers["direct"] += 1
            return m, "direct"
        k = self._ikey14(mnxm)
        if k and k in self.by_ikey:
            m = self.by_ikey[k]
            self.by_mnxm[mnxm] = m
            self.tiers["inchikey"] += 1
            return m, "inchikey"
        fc = self._fc(mnxm)
        if fc and fc in self.by_fc:
            m = self.by_fc[fc]
            self.by_mnxm[mnxm] = m
            self.tiers["formula_charge"] += 1
            return m, "formula_charge"
        if not create:
            return None, "absent"
        mid = f"{mnxm}_c"
        if mid in self.model.metabolites:
            self.tiers["reused"] += 1
            m = self.model.metabolites.get_by_id(mid)
        else:
            f = self.formula.get(mnxm)
            m = cobra.Metabolite(mid, compartment="c", name=str(mnxm),
                                 formula=(f if isinstance(f, str) and f != "*" else None))
            self.model.add_metabolites([m])
            self.tiers["created"] += 1
        self.by_mnxm[mnxm] = m
        return m, "created"


def _direction(classifs, mets: dict, bounds):
    """Bounds for one insertion. A carboxy-lyase may only decarboxylate.

    MetaNetX ships no direction, so an EC 4.1.1 reaction inserted reversibly gives the
    model a carboxylase that fixes CO2 for free. One such insertion, the reverse of
    `co2_c + dhap_c = h_c + ohpb_c`, raises growth from 0.982 to 1.102 on glucose alone.
    Physiological CO2 does not drive a decarboxylase backwards, so the reverse closes.
    """
    lo, hi = float(bounds[0]), float(bounds[1])
    if "4.1.1." not in str(classifs or ""):
        return lo, hi
    co2 = [c for m, c in mets.items() if m.id == "co2_c"]
    if not co2:
        return lo, hi
    return (max(lo, 0.0), hi) if co2[0] > 0 else (lo, min(hi, 0.0))


def _rebalance(model, mets: dict):
    """Balance a bridged reaction on the frame, adjusting protons only.

    Returns `(metabolites, outcome)` where outcome is one of `balanced`, `protonated`,
    `no_formula` or `unbalanced_on_frame`. Correcting the proton coefficient is the
    standard fix and the only correction allowed: anything still unbalanced after it is a
    bridge that mapped the wrong metabolite, not a protonation difference.
    """
    import cobra
    probe = cobra.Reaction("_probe")
    probe.add_metabolites(dict(mets))
    try:
        imb = probe.check_mass_balance()
    except Exception:
        return mets, "no_formula"
    if not imb:
        return mets, "balanced"
    if set(imb) - {"charge", "H"}:
        return mets, "unbalanced_on_frame"
    d_h = imb.get("H", 0.0)
    d_q = imb.get("charge", 0.0)
    if abs(d_h - d_q) > 1e-9 or "h_c" not in model.metabolites:
        return mets, "unbalanced_on_frame"
    h = model.metabolites.get_by_id("h_c")
    out = dict(mets)
    out[h] = out.get(h, 0.0) - d_h
    if out[h] == 0.0:
        out.pop(h)
    probe2 = cobra.Reaction("_probe2")
    probe2.add_metabolites(out)
    try:
        if probe2.check_mass_balance():
            return mets, "unbalanced_on_frame"
    except Exception:
        return mets, "no_formula"
    if len([m for m in out if m.id not in FREE]) < 2:
        return mets, "unbalanced_on_frame"
    return out, "protonated"


def _stoich_key(mets: dict) -> frozenset:
    """A reaction's identity up to scale and direction, for duplicate detection."""
    items = sorted((m.id, v) for m, v in mets.items())
    if not items:
        return frozenset()
    scale = abs(items[0][1])
    fwd = frozenset((k, round(v / scale, 6)) for k, v in items)
    rev = frozenset((k, round(-v / scale, 6)) for k, v in items)
    return min(fwd, rev, key=lambda s: sorted(s))


def frame_stoich_keys(model) -> set:
    return {_stoich_key(dict(r.metabolites)) for r in model.reactions}


def insert(model, mnxrs, bridge: MetBridge, release: Release, *, bounds=(-BIG, BIG),
           prefix=HET_PREFIX, skip_duplicates: bool = True):
    """Insert `mnxrs` into `model` in place. Returns `(inserted, rejected, census)`.

    `inserted` maps MNXR to the new reaction id, `rejected` maps MNXR to one of
    `REJECTIONS`, and `census` counts both.
    """
    import cobra

    mnxrs = sorted({str(m) for m in mnxrs})
    rp = release.reac_prop(set(mnxrs))
    seen = frame_stoich_keys(model) if skip_duplicates else set()
    existing = {r.id for r in model.reactions}

    inserted, rejected, new = {}, {}, []
    protonated = oneway = 0
    for mnxr in mnxrs:
        info = rp.get(mnxr)
        if info is None:
            rejected[mnxr] = "no_stoichiometry"
            continue
        if str(info.get("is_transport") or "").strip() == "T":
            rejected[mnxr] = "transport"
            continue
        if str(info.get("is_balanced") or "").strip() != "B":
            rejected[mnxr] = "unbalanced"
            continue
        stoich = parse_equation(info["equation"])
        if not stoich:
            rejected[mnxr] = "unparsable"
            continue

        mets, orphan = {}, False
        for mnxm, coef in stoich.items():
            met, _tier = bridge.get(mnxm, create=False)
            if met is None:
                orphan = True
                break
            mets[met] = mets.get(met, 0.0) + coef
        if orphan:
            rejected[mnxr] = "orphan_metabolite"
            continue
        if len(mets) < len(stoich):
            rejected[mnxr] = "collapsed"
            continue
        mets = {k: v for k, v in mets.items() if v != 0.0}
        if not mets:
            rejected[mnxr] = "degenerate"
            continue
        if len([m for m in mets if m.id not in FREE]) < 2:
            rejected[mnxr] = "trivial"
            continue

        mets, fix = _rebalance(model, mets)
        if fix in ("no_formula", "unbalanced_on_frame"):
            rejected[mnxr] = fix
            continue
        protonated += (fix == "protonated")

        key = _stoich_key(mets)
        rid = prefix + mnxr
        if (skip_duplicates and key in seen) or rid in existing:
            rejected[mnxr] = "already_present"
            continue

        lo, hi = _direction(info.get("classifs"), mets, bounds)
        oneway += (lo, hi) != tuple(float(b) for b in bounds)
        r = cobra.Reaction(rid, name=f"MetaNetX {mnxr}", lower_bound=lo, upper_bound=hi)
        r.add_metabolites(mets)
        new.append(r)
        inserted[mnxr] = rid
        existing.add(rid)
        seen.add(key)

    if new:
        model.add_reactions(new)

    census = {"requested": len(mnxrs), "inserted": len(inserted),
              "proton_corrected": protonated, "decarboxylase_oneway": int(oneway)}
    for why in REJECTIONS:
        census[why] = sum(1 for v in rejected.values() if v == why)
    return inserted, rejected, census


#: One dissipation reaction per cofactor. A sealed model that can run any of these makes
#: that cofactor from nothing, which is a thermodynamically infeasible cycle.
DISSIPATION = {
    "EGC_atp": {"atp_c": -1.0, "h2o_c": -1.0, "adp_c": 1.0, "pi_c": 1.0, "h_c": 1.0},
    "EGC_nadh": {"nadh_c": -1.0, "nad_c": 1.0, "h_c": 1.0},
    "EGC_nadph": {"nadph_c": -1.0, "nadp_c": 1.0, "h_c": 1.0},
    "EGC_fadh2": {"fadh2_c": -1.0, "fad_c": 1.0, "h_c": 2.0},
    "EGC_q8h2": {"q8h2_c": -1.0, "q8_c": 1.0, "h_c": 2.0},
    "EGC_proton": {"h_p": -1.0, "h_c": 1.0},
}


def _seal(model):
    """Close every route by which mass enters the model."""
    for r in model.exchanges:
        r.lower_bound = 0.0
    for r in model.reactions:
        if r.id.startswith(("DM_", "SK_", "EGC_")) or r.id == "ATPM":
            r.lower_bound = 0.0


def energy_cycles(model, tol: float = 1e-6) -> dict:
    """Maximise each cofactor dissipation reaction on a sealed model.

    Returns the ones that fire, which are the cycles.
    """
    import cobra
    out = {}
    with model:
        _seal(model)
        for name, stoich in DISSIPATION.items():
            mets = {}
            for mid, coef in stoich.items():
                if mid not in model.metabolites:
                    mets = None
                    break
                mets[model.metabolites.get_by_id(mid)] = coef
            if mets is None:
                continue
            r = cobra.Reaction(name, lower_bound=0.0, upper_bound=BIG)
            r.add_metabolites(mets)
            model.add_reactions([r])
            model.objective = name
            v = model.slim_optimize()
            if v is not None and v == v and v > tol:
                out[name] = float(v)
    return out


def close_energy_cycles(model, reaction_ids, tol: float = 1e-6,
                        max_rounds: int = 500) -> dict:
    """Constrain the named reactions until no cofactor is made from an empty medium.

    Solves the sealed dissipation problem, finds which of `reaction_ids` carries flux, and
    closes the direction the worst offender used. Repeats until the cycle is gone. Only
    the named reactions are touched, so the curated frame keeps every bound it shipped
    with. Returns reaction id -> the direction closed.
    """
    import cobra
    ids = [r for r in reaction_ids if r in model.reactions]
    closed = {}
    for _ in range(max_rounds):
        live = energy_cycles(model, tol)
        if not live:
            break
        name = max(live, key=live.get)
        with model:
            _seal(model)
            mets = {model.metabolites.get_by_id(k): v
                    for k, v in DISSIPATION[name].items()}
            r = cobra.Reaction(name, lower_bound=0.0, upper_bound=BIG)
            r.add_metabolites(mets)
            model.add_reactions([r])
            model.objective = name
            sol = model.optimize()
            if sol.status != "optimal":
                break
            hot = [(abs(sol.fluxes[i]), i, sol.fluxes[i]) for i in ids
                   if abs(sol.fluxes.get(i, 0.0)) > tol]
        if not hot:
            break
        _mag, rid, flux = max(hot)
        rxn = model.reactions.get_by_id(rid)
        if flux > 0:
            rxn.upper_bound = 0.0
            closed[rid] = "forward"
        else:
            rxn.lower_bound = 0.0
            closed[rid] = "reverse"
    return closed


def unbounded_targets(model, reaction_ids, tol: float = 1e-6) -> dict:
    """Maximise each id alone and report the ones that reach the solver's own ceiling.

    A reaction that optimises to `BIG` is on a loop, not on a pathway.
    """
    out = {}
    for rid in reaction_ids:
        if rid not in model.reactions:
            continue
        with model:
            model.objective = rid
            v = model.slim_optimize()
        if v is not None and v == v and v >= BIG - tol:
            out[rid] = float(v)
    return out
