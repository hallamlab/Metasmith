"""Build a draft reconstruction from a GPR evidence table.

    gem_from_gpr.py <gpr.parquet> <mnxr_lookup.parquet> <vocab.parquet> \\
                    <reac_prop.tsv> <chem_prop.tsv> <out.xml>

The GPR table names intermediates (KO, EC, UniProt) per ORF; `mnxr_lookup` turns
those into MNXR reaction ids; `reac_prop` is where the stoichiometry actually
lives. The draft carries a reaction for every MNXR that survives all three, with
the ORFs that evidenced it as its gene rule.

Three things are deliberately NOT done here. Transport reactions are dropped:
their equation spans two compartments and this builds a single-compartment model,
so half a transporter would be a leak. No objective is set -- a GPR table names no
biomass equation, and inventing one would make an unfalsifiable growth number.
And the vocabulary restricts the reaction set to what the metabolism bake has, so
a reaction the rest of this repository cannot say anything about does not enter.
"""
import sys

import cobra
import pandas as pd

import mnx

MNXR_COLUMN = "mnxr"
INTERMEDIATE_COLUMN = "intermediate_id"
ORF_COLUMN_CANDIDATES = ("orf", "orf_id", "gene", "query")


def orf_column(frame: pd.DataFrame) -> str | None:
    for c in ORF_COLUMN_CANDIDATES:
        if c in frame.columns:
            return c
    return None


def main(argv: list[str]) -> int:
    gpr_path, lookup_path, vocab_path, reac_prop_path, chem_prop_path, out_path = argv

    gpr = pd.read_parquet(gpr_path)
    assert MNXR_COLUMN in gpr.columns, (
        f"[{gpr_path}] has no [{MNXR_COLUMN}] column; its schema is "
        f"{list(gpr.columns)} and lib::fabfos_evidence.py::SCHEMA_COLS is the "
        "shape this expects"
    )
    named = set(gpr[MNXR_COLUMN].dropna().astype(str))

    # A GPR row usually carries its own MNXR. The lookup is for the rows that do
    # not -- an annotator that emitted a KO, EC or UniProt id and no reaction --
    # and reading it costs nothing when there are none, because it is only read
    # for the intermediates actually left unresolved.
    unresolved = set()
    if INTERMEDIATE_COLUMN in gpr.columns:
        blank = gpr[gpr[MNXR_COLUMN].isna()]
        unresolved = {str(i) for i in blank[INTERMEDIATE_COLUMN].dropna()}
    if unresolved:
        lookup = pd.read_parquet(lookup_path, columns=["id", "mnxr"])
        recovered = set(lookup[lookup.id.isin(unresolved)].mnxr.astype(str))
        print(f"{len(blank)} GPR rows carry no MNXR; the lookup resolves "
              f"{len(unresolved)} intermediates to {len(recovered)} reactions")
        named |= recovered

    vocab = pd.read_parquet(vocab_path)
    known = set(vocab[vocab.kind == "rxn"].symbol.astype(str))
    wanted = named & known
    print(f"{len(named)} MNXR in the GPR table, {len(wanted)} of them in the bake vocabulary")

    props = mnx.load_reac_prop(reac_prop_path, wanted)
    equations = {}
    dropped = {"unparsed": 0, "transport": 0, "absent": 0}
    for mnxr in sorted(wanted):
        prop = props.get(mnxr)
        if prop is None:
            dropped["absent"] += 1
            continue
        if str(prop.get("is_transport")).upper() in ("T", "TRUE", "1"):
            dropped["transport"] += 1
            continue
        stoich = mnx.parse_mnx_equation(prop.get("equation"))
        if stoich is None:
            dropped["unparsed"] += 1
            continue
        equations[mnxr] = stoich

    metabolites = {m for s in equations.values() for m in s}
    chem = mnx.load_chem_prop(chem_prop_path, metabolites).set_index("mnxm")
    formulas = chem.formula.to_dict()
    names = chem.name.to_dict()

    orf_col = orf_column(gpr)
    rules = {}
    if orf_col is not None:
        for mnxr, group in gpr.groupby(MNXR_COLUMN):
            orfs = sorted({str(o) for o in group[orf_col].dropna()})
            if orfs:
                rules[str(mnxr)] = " or ".join(orfs)

    model = cobra.Model("gem_from_gpr")
    made = {}
    for m in sorted(metabolites):
        f = formulas.get(m)
        made[m] = cobra.Metabolite(
            f"{m}_c", compartment="c", name=str(names.get(m, m)),
            formula=(f if isinstance(f, str) and f != "*" else None),
        )
    model.add_metabolites(list(made.values()))

    reactions = []
    for mnxr, stoich in equations.items():
        r = cobra.Reaction(mnxr)
        r.lower_bound, r.upper_bound = -1000.0, 1000.0
        reactions.append((r, {made[m]: c for m, c in stoich.items()}))
    model.add_reactions([r for r, _ in reactions])
    for r, stoich in reactions:
        r.add_metabolites(stoich)
        rule = rules.get(r.id)
        if rule:
            r.gene_reaction_rule = rule

    cobra.io.write_sbml_model(model, out_path)
    print(f"draft: {len(model.reactions)} reactions, {len(model.metabolites)} metabolites, "
          f"{len(model.genes)} genes; dropped {dropped} -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
