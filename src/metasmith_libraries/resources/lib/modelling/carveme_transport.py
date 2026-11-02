"""Add a transport boundary, taken NATIVELY from MetaNetX rather than borrowed.

    carveme_transport.py <draft.xml> <reac_prop.tsv> <chem_prop.tsv> <media.tsv> \\
                         <out.xml>

`gem_from_gpr.py` drops every transport reaction on purpose: its equation spans two
compartments and a single-compartment draft has nowhere to put half of one. Once a
biomass equation is in the model (`carveme_biomass.py`), the draft needs a way for
anything to get from an `e` compartment into `c` at all -- but CarveMe's own 1,870
transporters are BiGG-namespaced and mapping them would repeat the exact "which
release" hazard curation round 5 documented for BiGG's reaction annotations. MetaNetX's
`reac_prop.is_transport` is transport and exchange from the SAME source our reactions
already come from, so this reads that column directly and maps nothing.

Every `is_transport=T` row in this MetaNetX release turns out to span exactly two
compartments, `MNXD1` and `MNXD2` (checked over all 11,380: `@MNXD2` appears in
exactly the 11,380 rows flagged `T`, in no others) -- so `MNXD1` is `c` and `MNXD2`
is `e`, matching `gem_from_gpr.py`'s single (implicit `c`) compartment exactly.

KEPT MINIMAL, DELIBERATELY: only reactions that carry a metabolite the medium table
names or the draft already needs, and only when every OTHER metabolite the reaction
would drag in is already available (in the draft, in the medium, or a proton/water).
A model given every transporter can eat anything, and a medium then constrains
nothing -- the whole point of a growth medium is that most of the world is NOT
available, and a gapfill against that draft has to earn the reactions it adds rather
than borrow them from an unconstrained boundary.
"""
import re
import sys

import cobra
import pandas as pd

import media as media_mod
import mnx

#  MetaNetX carries exactly one non-`MNXM`-prefixed compound in this release: `WATER`
# (checked over the whole of chem_prop.tsv). mnx.py's own `_TERM` regex requires the
# `MNXM` prefix and silently drops any equation naming it -- which `gem_from_gpr.py`
# and `cobra_gapfill.py` both inherit and neither can fix without widening what THEY
# parse. This module owns its own equation parser already, so it costs nothing extra
# to recognise `WATER` here, and water crossing a membrane unassisted is real biology,
# not a workaround -- MNXR98641, `1 WATER@MNXD1 = 1 WATER@MNXD2`, is exactly that
# reaction, native to this release.
_TERM = re.compile(r"^\s*(\d+(?:\.\d+)?)\s+(MNXM\S+?|WATER)@(MNXD([12]))\s*$")
COMPARTMENT_OF_MNXD = {"1": "c", "2": "e"}
# Ubiquitous species that riding along in a transporter's equation does not count as
# "dragging in" something new -- the proton gradient (MNXM1 the ion, MNXM01 the PMF
# bookkeeping id for the SAME species used by symport/antiport reactions) and water are
# the currency every transport reaction is allowed to spend without becoming a bigger
# ask than the one metabolite it exists to move.
FREE_PASSENGERS = {"MNXM1", "MNXM01", "WATER"}


def parse_transport_equation(eq: str) -> dict[tuple[str, str], float] | None:
    """(mnxm, compartment) -> signed coefficient for a two-compartment MNXD1/MNXD2 equation."""
    if not isinstance(eq, str) or "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)
    out: dict[tuple[str, str], float] = {}
    for side, sign in ((lhs, -1.0), (rhs, 1.0)):
        for term in side.split("+"):
            term = term.strip()
            if not term:
                continue
            m = _TERM.match(term)
            if not m:
                return None
            coef, mnxm, _mnxd, d = m.group(1), m.group(2), m.group(3), m.group(4)
            key = (mnxm, COMPARTMENT_OF_MNXD[d])
            out[key] = out.get(key, 0.0) + sign * float(coef)
    return {k: v for k, v in out.items() if v != 0.0} or None


def main(argv: list[str]) -> int:
    draft_path, reac_prop_path, chem_prop_path, media_path, out_path = argv

    model = cobra.io.read_sbml_model(draft_path)
    have_c = {m.id[:-2] for m in model.metabolites if m.id.endswith("_c")}

    # Every row of the table counts, regardless of a `medium` column -- this decides
    # which EXCHANGE REACTIONS EXIST, not which ones are open. `media.py.apply()` is
    # what restricts a model to one named medium, later, by bounds; a multi-medium
    # table here just means the boundary supports every medium it names.
    media_table = media_mod.load(media_path)
    medium_mnxm = {
        row.exchange[len("EX_"):-len("_e")] if row.exchange.startswith("EX_") else row.exchange
        for row in media_table.itertuples(index=False)
    }
    needed = have_c | medium_mnxm
    print(f"draft carries {len(have_c)} cytosolic metabolites; medium names "
          f"{len(medium_mnxm)}; transporters are searched for {len(needed)} total")

    frame = pd.read_csv(reac_prop_path, sep="\t", comment="#", header=None,
                        names=mnx.REAC_PROP_COLUMNS, dtype=str, low_memory=False)
    transport = frame[frame.is_transport.str.upper().isin(["T", "TRUE", "1"])]
    print(f"{len(transport)} transport-flagged reactions in this release")

    chem_prop = mnx.load_chem_prop(chem_prop_path)
    formulas = chem_prop.set_index("mnxm").formula.to_dict()

    def get_met(mnxm: str, compartment: str):
        mid = f"{mnxm}_{compartment}"
        if mid in model.metabolites:
            return model.metabolites.get_by_id(mid)
        f = formulas.get(mnxm)
        met = cobra.Metabolite(mid, compartment=compartment, name=mnxm,
                                formula=(f if isinstance(f, str) and f != "*" else None))
        model.add_metabolites([met])
        return met

    # One transporter per metabolite still missing one, cheapest (fewest OTHER
    # metabolites dragged in) first, so a metabolite with both a clean 1:1 symport
    # and a multi-substrate ABC transporter in this release gets the former.
    candidates: dict[str, list[tuple[int, str, dict]]] = {}
    for row in transport.itertuples(index=False):
        stoich = parse_transport_equation(row.equation)
        if stoich is None:
            continue
        mnxms = {mnxm for mnxm, _c in stoich}
        others = mnxms - needed - FREE_PASSENGERS
        for mnxm, compartment in stoich:
            if compartment == "e" and mnxm in needed:
                candidates.setdefault(mnxm, []).append((len(others), row.mnxr, stoich))

    added_transport, added_exchange = [], []
    covered = set()
    for mnxm in sorted(medium_mnxm):
        options = sorted(candidates.get(mnxm, []), key=lambda t: t[0])
        for n_others, mnxr, stoich in options:
            mnxms = {m for m, _c in stoich}
            if mnxms - needed - FREE_PASSENGERS:
                continue  # this option drags in something outside the needed set
            if mnxr in model.reactions:
                continue
            reaction = cobra.Reaction(mnxr)
            reaction.lower_bound, reaction.upper_bound = -1000.0, 1000.0
            reaction.notes["source"] = "carveme_transport"
            model.add_reactions([reaction])
            reaction.add_metabolites({get_met(m, c): coef for (m, c), coef in stoich.items()})
            added_transport.append(mnxr)
            covered.add(mnxm)
            break
        exchange_id = f"EX_{mnxm}_e"
        if exchange_id not in model.reactions:
            exchange = cobra.Reaction(exchange_id)
            exchange.lower_bound, exchange.upper_bound = 0.0, 1000.0
            exchange.notes["source"] = "carveme_transport:exchange"
            model.add_reactions([exchange])
            exchange.add_metabolites({get_met(mnxm, "e"): -1.0})
            added_exchange.append(exchange_id)

    uncovered = medium_mnxm - covered
    print(f"added {len(added_transport)} native MNXR transporters, "
          f"{len(added_exchange)} exchange reactions")
    if uncovered:
        print(f"{len(uncovered)} medium compounds have an exchange but no transporter "
              f"minimal enough to add (no reac_prop entry stayed inside the needed set): "
              f"{sorted(uncovered)}")

    cobra.io.write_sbml_model(model, out_path)
    print(f"{len(model.reactions)} reactions, {len(model.metabolites)} metabolites -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
