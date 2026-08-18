"""Does FBA answer the question ECSPr cannot -- does overexpressing a gene LOWER glycogen?

Background: the eydallin/directionality work established that ECSPr's readout is monotone
in enzyme abundance (Rayleigh), so a fold-increase can never produce a decrease.  This
asks whether a flux-balance model escapes that, on the same three genes:
glgC (Eydallin 2010: 453% of WT glycogen), glgP (60%), malP (61%).

Readout: a demand flux on glycogen_c, maximised with growth pinned -- the closest a
steady-state model gets to an accumulating pool.

THE GROWTH FLOOR IS PINNED TO THE UNPERTURBED OPTIMUM.  Re-deriving max growth inside
each perturbed model lowers the floor whenever the perturbation is costly, which frees
carbon and manufactures a spurious *increase* in glycogen.  That is not a subtlety --
it flips glgC's sign.

Run:  /home/tony/lib/miniforge3/envs/ecspr/bin/python research/fabfos/vs_flux_balence/fba_glycogen.py
"""

import cobra
from cobra.util.solver import linear_reaction_coefficients
from cobra.flux_analysis import pfba

MODEL = "data/fabfos/originals/genomes/e_coli_k12/GEM/iML1515.json"
GROWTH_FRAC = 0.9

# Eydallin 2010 (DNA Res 17:61-71) ASKA overexpression -> glycogen content, % of wild type.
PHENOTYPE = {"glgC": 453.3, "glgA": 327.9, "glgB": 26.4, "glgP": 59.8, "malP": 60.8}

# Reactions each ASKA clone's product catalyses in iML1515.  GLCP/GLCP2 carry the GPR
# "b3428 or b3417", so glgP and malP are indistinguishable on the glycogen arm itself;
# malP's private reactions are the maltodextrin phosphorylases MLTP1/2/3.
GENE_RXNS = {
    "glgC": ["GLGC"], "glgA": ["GLCS1"], "glgB": ["GLBRAN2"],
    "glgP": ["GLCP", "GLCP2"],
    "malP": ["GLCP", "GLCP2", "MLTP1", "MLTP2", "MLTP3"],
}
SIGNATURE = {"glgC": "GLGC", "glgA": "GLCS1", "glgB": "GLBRAN2",
             "glgP": "GLCP", "malP": "GLCP"}


def prepared_model():
    m = cobra.io.load_json_model(MODEL)
    m.add_boundary(m.metabolites.glycogen_c, type="demand", reaction_id="DM_glycogen_c")
    return m


def make_probe(m):
    """Return gly(edits) -> max glycogen deposition, growth pinned at the WT optimum."""
    bio = list(linear_reaction_coefficients(m))[0]
    gmax = m.slim_optimize()

    def gly(edits=(), solution=False):
        with m:
            for rid, lo, hi in edits:
                r = m.reactions.get_by_id(rid)
                if lo is not None:
                    r.lower_bound = lo
                if hi is not None:
                    r.upper_bound = hi
            bio.lower_bound = GROWTH_FRAC * gmax
            m.objective = m.reactions.DM_glycogen_c
            if solution:
                try:
                    return pfba(m)
                except Exception:
                    return None
            v = m.slim_optimize()
            return None if (v is None or v != v) else v

    return gly, bio, gmax


def fmt(x):
    return "infeas" if x is None else f"{x:.6f}"


def main():
    m = prepared_model()
    gly, bio, gmax = make_probe(m)
    wt = gly()
    print(f"iML1515 | glucose minimal (EX_glc__D_e lb "
          f"{m.reactions.EX_glc__D_e.lower_bound:g}) | biomass {bio.id}")
    print(f"WT: max growth {gmax:.6f}/h; growth pinned at {GROWTH_FRAC:g}x = "
          f"{GROWTH_FRAC*gmax:.6f}; max glycogen deposition = {wt:.6f} mmol/gDW/h\n")

    print("[0] stock bounds -- note every one is already +/-1000, i.e. non-binding")
    for g, rxns in GENE_RXNS.items():
        for rid in rxns:
            r = m.reactions.get_by_id(rid)
            print(f"    {g:5s} {rid:9s} lb={r.lower_bound:8g} ub={r.upper_bound:8g}   {r.reaction}")

    print("\n[A] overexpression as CAPACITY RELAXATION (ub *= f) -- the analogue of ECSPr's fold")
    print(f"    {'gene':>6} {'%WT meas':>9} {'f=2':>12} {'f=10':>12} {'f=100':>12}")
    for g in ["glgC", "glgA", "glgB", "glgP", "malP"]:
        cells = []
        for f in (2.0, 10.0, 100.0):
            edits = []
            for rid in GENE_RXNS[g]:
                r = m.reactions.get_by_id(rid)
                edits.append((rid, r.lower_bound * f if r.lower_bound < 0 else None,
                              r.upper_bound * f))
            cells.append(fmt(gly(edits)))
        print(f"    {g:>6} {PHENOTYPE[g]:>9.1f} " + " ".join(f"{c:>12}" for c in cells))
    print(f"    wild type = {wt:.6f}  -> identically zero response, because nothing was binding")

    print("\n[B] deletion (bounds -> 0) -- the framing FBA is actually validated on")
    print(f"    {'gene':>6} {'%WT meas':>9} {'glycogen':>12} {'max growth':>12}")
    for g in ["glgC", "glgA", "glgB", "glgP", "malP"]:
        edits = [(rid, 0, 0) for rid in GENE_RXNS[g]]
        with m:
            for rid in GENE_RXNS[g]:
                m.reactions.get_by_id(rid).knock_out()
            grow = m.slim_optimize()
        print(f"    {g:>6} {PHENOTYPE[g]:>9.1f} {fmt(gly(edits)):>12} {fmt(grow):>12}")
    print(f"    wild type = {wt:.6f} glycogen, growth {gmax:.6f}")

    print("\n[C] overexpression as OBLIGATE FLUX (lb = v) -- 'the enzyme is present AND running'")
    print(f"    {'v':>6} {'GLGC (glgC)':>13} {'GLCS1 (glgA)':>13} {'GLCP (glgP,malP)':>17}")
    for v in [0, 0.1, 0.25, 0.5, 0.75, 1.0, 2.0, 5.0, 20.0]:
        row = [fmt(gly([(rid, v, None)])) for rid in ("GLGC", "GLCS1", "GLCP")]
        print(f"    {v:>6} " + " ".join(f"{c:>13}" for c in row[:2]) + f" {row[2]:>17}")
    print("    the synthetic arm is already carrying 0.8967, so forcing it below that is a no-op;")
    print("    the degradative arm sits at 0, so forcing it bites immediately and linearly.")
    print("    malP's private arm MLTP1/2/3 is infeasible at any v -- no maltodextrin on glucose")
    print(f"    minimal medium, so malP can only act through the shared GLCP.  MLTP1 at v=1: "
          f"{fmt(gly([('MLTP1', 1.0, None)]))}")

    print("\n[D] capacity relaxation DOES work -- iff the capacity was binding")
    print("    WT reference caps the enzyme at a physiological capacity C; overexpression = C*f")
    print(f"    {'rxn':>8} {'C':>6} {'WT (=C)':>12} {'C x2':>12} {'C x10':>12}")
    for rid in ["GLGC", "GLCS1", "GLCP"]:
        for C in [0.2, 0.5, 0.9, 5.0]:
            print(f"    {rid:>8} {C:>6} {fmt(gly([(rid, None, C)])):>12} "
                  f"{fmt(gly([(rid, None, C*2)])):>12} {fmt(gly([(rid, None, C*10)])):>12}")
    print("    GLGC/GLCS1: a binding cap makes overexpression register, and glgC's UP sign appears.")
    print("    GLCP: no cap is ever binding (its optimal flux is 0), so no relaxation ever registers.")

    print("\n[E] where the mass goes -- GLCP forced at 20 vs WT (pFBA)")
    a, b = gly(solution=True), gly([("GLCP", 20.0, None)], solution=True)
    watch = ["EX_glc__D_e", "EX_co2_e", "EX_o2_e", "DM_glycogen_c",
             "GLGC", "GLCS1", "GLCP", "ATPS4rpp", "ATPM", bio.id]
    print(f"    {'reaction':>16} {'WT':>11} {'GLCP=20':>11} {'delta':>11}")
    for r in watch:
        print(f"    {r:>16} {a.fluxes[r]:11.5f} {b.fluxes[r]:11.5f} {b.fluxes[r]-a.fluxes[r]:11.5f}")
    dg, dc = b.fluxes["DM_glycogen_c"] - a.fluxes["DM_glycogen_c"], \
        b.fluxes["EX_co2_e"] - a.fluxes["EX_co2_e"]
    print(f"    glucose uptake unchanged; d(CO2) = {dc:.5f} = {dc/-dg:.3f} x -d(glycogen) "
          f"({-dg:.5f}); a glucosyl unit is 6 carbons.")
    print("    The carbon that did not become glycogen left as CO2, burned to pay the ATP bill")
    print("    of the GLGC->GLCS1->GLCP futile cycle.  That is the 'somewhere else'.")


if __name__ == "__main__":
    main()
