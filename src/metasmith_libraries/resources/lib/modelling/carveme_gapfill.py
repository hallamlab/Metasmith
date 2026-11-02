"""Gapfill a draft against an unrestricted MetaNetX universe, through CarveMe's
own MILP, on the free solver.

    carveme_gapfill.py <draft.xml> <universe.xml> <media.tsv> <medium_name> <out.xml>

`<universe.xml>` is `build_mnx_universe.py`'s output -- built in `env::cobra.env`
because this image ships `reframed`, not `cobra`. This script never imports
`cobra` at all, on purpose: it is the half of the gapfill step that genuinely
needs CarveMe, and mixing the two dependencies into one script would make it
require BOTH environments, which `env::` files do not do.

THE SOLVER PROBLEM. `resources/env/cobra.env`'s comment already covers why
cplex is not an option; CarveMe's own `gapfill` CLI has no `--solver` flag and
falls through to `carveme.config`'s `solver = cplex`, which reframed never even
imports unless asked. `reframed.solvers.solvers` registers exactly one backend
in this image, `scip`, and `default_solver` starts `None` -- so
`reframed.solvers.set_default_solver("scip")` before touching
`carveme.cli.gapfill` is the fix, run here unconditionally.

`load_cbmodel`/`save_cbmodel` need `flavor="fbc2"` -- `flavor="cobra"` loads the
model without erroring but never finds the FBC objective, so `model.biomass_reaction`
raises "No biomass reaction identified" even though this repo's own SBML always
carries one (confirmed against `draft_full.xml` before trusting this).

REFRAMED PREFIXES EVERY BARE ID. Any reaction or metabolite id not already
starting with `R_`/`M_` gets one on load, so `Growth` becomes `R_Growth` and
`MNXM123_c` becomes `M_MNXM123_c` for the DURATION of this script -- which is
also why `EX_MNXM123_e` (this repo's own exchange-naming convention, from
`carveme_transport.py`) lines up exactly with what CarveMe's own
`Environment.from_compounds` expects, `R_EX_{compound}_e`, with no translation
needed on the medium side. The OUTPUT SBML still carries the reframed prefix,
though, and `strip_reframed_prefix` undoes it by plain text substitution rather
than a second cobra round-trip -- there is no cobra in this container to do it
with. See that function's own docstring for the exact substitution and why it
has to be attribute-general rather than a list of id patterns.
"""
import re
import sys

import pandas as pd

import media as media_mod


def build_carveme_media_db(media_path, medium_name, out_path) -> None:
    table = media_mod.load(media_path)
    rows = table if "medium" not in table.columns else table[table.medium.astype(str) == str(medium_name)]
    compounds = [
        row.exchange[len("EX_"):-len("_e")] for row in rows.itertuples(index=False)
        if row.exchange.startswith("EX_") and row.exchange.endswith("_e")
    ]
    pd.DataFrame({
        "medium": [medium_name] * len(compounds),
        "description": [f"{medium_name}, metasmith MNXM namespace"] * len(compounds),
        "compound": compounds,
        "name": compounds,
    }).to_csv(out_path, sep="\t", index=False)
    print(f"medium [{medium_name}]: {len(compounds)} compounds -> {out_path}")


_ID_ATTR = re.compile(r'="([RM])_')


def strip_reframed_prefix(model_path: str, out_path: str) -> None:
    """Every id/reference reframed prefixed appears only as an ATTRIBUTE VALUE
    (`id="R_Growth"`, `species="M_MNXM1_c"`, `reaction="R_MNXR..."`, ...) --
    never as element text or inside a tag name -- so `="R_`/`="M_` is exact for
    every attribute that carries one, `metaid` included, rather than a
    name-specific guess that has to be kept in sync with every attribute SBML
    ever puts an id in. An earlier version tried exactly that (a handful of
    literal `"R_MNXR`/`"M_MNXM` substrings) and missed `id="R_Growth"` --
    the biomass reaction is not named MNXR-anything -- along with every
    gene-product and fbc-association reference.
    """
    with open(model_path, encoding="utf-8") as fh:
        text = fh.read()
    text = _ID_ATTR.sub('="', text)
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(text)


def main(argv: list[str]) -> int:
    draft_path, universe_path, media_path, medium_name, out_path = argv

    import reframed.solvers as _solvers
    _solvers.set_default_solver("scip")
    from carveme.cli.gapfill import maincall
    from reframed import load_cbmodel

    media_db_path = "carveme_gapfill_media_db.tsv"
    build_carveme_media_db(media_path, medium_name, media_db_path)

    before = load_cbmodel(draft_path, flavor="fbc2")
    before_reactions = set(before.reactions)

    prefixed_out = "carveme_gapfill_raw_output.xml"
    maincall(
        inputfile=draft_path,
        media=[medium_name],
        mediadb=media_db_path,
        universe_file=universe_path,
        outputfile=prefixed_out,
        flavor="fbc2",
        verbose=True,
    )
    strip_reframed_prefix(prefixed_out, out_path)

    after = load_cbmodel(prefixed_out, flavor="fbc2")
    added = sorted(set(after.reactions) - before_reactions)
    print(f"CarveMe gapfill added {len(added)} reactions: {added}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
