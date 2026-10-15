"""Build an UNRESTRICTED single-compartment MetaNetX reaction universe, for
CarveMe's own gapfill MILP to draw from.

    build_mnx_universe.py <draft.xml> <reac_prop.tsv> <chem_prop.tsv> <out.xml>

This is the deliberate opposite of `cobra_gapfill.py`'s universe: that one keeps
only reactions whose metabolites the draft already carries, on the stated
grounds that a gapfill free to invent metabolites always finds a solution and
the solution means nothing. CarveMe's own `gapfill` has no such restriction --
pointed at `--universe-file`, it does not know or care whether the draft
already has a metabolite -- so a universe built to feed it has to be the real,
unrestricted thing for the comparison in `carveme_gapfill.py`'s docstring to
mean anything. Kept in its own script (its own `env::cobra.env`, not
`env::carveme.env`) because the CarveMe biocontainer ships `reframed`, not
`cobra` -- the two halves of the gapfill step run in different environments and
are wired as separate transforms for exactly that reason.

Transport is excluded here for the same reason `carveme_transport.py` fixed the
transport boundary separately and deliberately: this universe answers "what
internal chemistry can grow the draft", not "what could this model eat", and
widening the transport boundary would let the MILP answer a different, easier
question than the one under test.

MetaNetX carries exactly one non-`MNXM`-prefixed compound in this release,
`WATER` -- mnx.py's own equation parser drops any equation naming it, which
would silently remove most hydrolysis and condensation chemistry from an
unrestricted universe (checked: dropped-as-unparsed fell from ~39% of every
non-transport row scanned to ~0.1% once this parser recognises it). This
script therefore keeps its own small variant rather than widen `mnx.py` for
every one of its other callers.
"""
import re
import sys

import cobra

import mnx

MAX_UNIVERSE = 100_000  # the whole non-transport, parseable release fits under this
_TERM = re.compile(r"^\s*(\d+(?:\.\d+)?)\s+(MNXM\S+?|WATER)@(MNXD\d+)\s*$")


def parse_single_compartment(eq: str) -> dict[str, float] | None:
    if not isinstance(eq, str) or "=" not in eq:
        return None
    lhs, rhs = eq.split("=", 1)
    out: dict[str, float] = {}
    comps = set()
    for side, sign in ((lhs, -1.0), (rhs, 1.0)):
        for term in side.split("+"):
            term = term.strip()
            if not term:
                continue
            m = _TERM.match(term)
            if not m:
                return None
            coef, mnxm, comp = float(m.group(1)), m.group(2), m.group(3)
            comps.add(comp)
            out[mnxm] = out.get(mnxm, 0.0) + sign * coef
    if len(comps) > 1:
        return None
    return {k: v for k, v in out.items() if v != 0.0} or None


def main(argv: list[str]) -> int:
    draft_path, reac_prop_path, chem_prop_path, out_path = argv

    draft = cobra.io.read_sbml_model(draft_path)
    exclude_ids = {r.id for r in draft.reactions}

    props = mnx.load_reac_prop(reac_prop_path)
    chem_prop = mnx.load_chem_prop(chem_prop_path)
    formulas = chem_prop.set_index("mnxm").formula.to_dict()

    universe = cobra.Model("mnx_gapfill_universe")
    made: dict[str, cobra.Metabolite] = {}
    reactions = []
    n_transport = n_unparsed = n_excluded = 0
    for mnxr, prop in props.items():
        if mnxr in exclude_ids:
            n_excluded += 1
            continue
        if str(prop.get("is_transport")).upper() in ("T", "TRUE", "1"):
            n_transport += 1
            continue
        stoich = parse_single_compartment(prop.get("equation"))
        if stoich is None:
            n_unparsed += 1
            continue
        for m in stoich:
            if m not in made:
                f = formulas.get(m)
                made[m] = cobra.Metabolite(
                    f"{m}_c", compartment="c", name=m,
                    formula=(f if isinstance(f, str) and f != "*" else None),
                )
        r = cobra.Reaction(mnxr)
        r.lower_bound, r.upper_bound = -1000.0, 1000.0
        # Stoichiometry goes on the reaction WHILE IT IS STILL UNATTACHED. Setting it
        # after `model.add_reactions` makes every one of ~72,000 calls walk the whole
        # model's own bookkeeping instead of just the reaction's -- the naive order
        # took over ten minutes and was killed; this one is the fix, not a style choice.
        r.add_metabolites({made[m]: c for m, c in stoich.items()})
        reactions.append(r)
        if len(reactions) >= MAX_UNIVERSE:
            print(f"universe capped at {MAX_UNIVERSE} before scanning the whole file")
            break

    universe.add_metabolites(list(made.values()))
    universe.add_reactions(reactions)

    print(f"universe: {len(universe.reactions)} internal reactions over "
          f"{len(universe.metabolites)} metabolites (excluded {n_excluded} already "
          f"in the draft, {n_transport} transport, dropped {n_unparsed} unparsed)")
    cobra.io.write_sbml_model(universe, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
