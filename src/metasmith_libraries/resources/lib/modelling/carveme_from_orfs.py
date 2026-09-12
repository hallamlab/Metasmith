"""Reconstruct a genome-scale model directly from predicted proteins through
CarveMe's own pipeline -- metaGEM's own reconstruction step (metaGEM is a
Snakemake workflow, not a tool, and its reconstruction step *is* this call),
and the parity lane against this library's hand-built GPR -> biomass ->
transport -> gapfill chain. Everything happens inside CarveMe's own bundled
bacteria universe -- no MetaNetX universe, no borrowed biomass mapping, no
native MNXR transport, unlike the GPR route.

`carve --fbc2 -g <medium>` is metaGEM's own single invocation, but gap filling
the SAME in-process model object `carve` just built raises inside SCIP:

    [var.c:7252] ERROR: cannot change the bounds of a fixed variable

The carving/scoring step fixes some reactions' bounds to (0, 0); gapfill's own
indicator-variable MILP formulation then tries to relax one of them inside the
SAME solver session, and SCIP refuses. Reconstructing first, saving to SBML,
and reloading before gapfilling -- through `carveme.cli.gapfill`, a second
CarveMe entry point rather than the `-g` flag on the first -- gives gapfill a
fresh reframed model and a fresh SCIP session, and the error does not recur.
This is the SAME split `carveme_gapfill.py` already uses for the GPR route,
for an unrelated reason (it receives its draft as a file, not an object); here
it is also what makes the free solver work at all.

The cost of the split: `carveme.cli.gapfill.maincall` calls `multiGapFill`
with no gene scores (confirmed by reading its source -- unlike `carve`'s own
internal call, which passes the diamond-derived `scores`), so gapfilling here
adds the cheapest reactions by count alone, not the most homology-supported
ones. That is a real deviation from metaGEM's exact behaviour, traded for a
gapfill that completes at all on a free solver with no CPLEX licence.

    carveme_from_orfs.py <orfs.faa> <media.tsv> <medium_name.txt> <out.xml>

`<medium_name.txt>` is a `modelling::medium_name` deferred input -- a bare
file whose stripped content is the row of `<media.tsv>` to apply, the same
way `fetch_bigg_model.py` reads its accession out of a file rather than
taking it on argv directly. This is what lets a driver choose the medium
without editing this transform.

Output ids stay in CarveMe's OWN BiGG namespace (R_/M_-prefixed) -- the same
prefix ANY `carve`/`load_cbmodel`+`save_cbmodel` round trip produces, not an
artifact of this repo's MNXM pipeline -- so there is no `strip_reframed_prefix`
step here, unlike `carveme_gapfill.py`, whose universe is MNXM-namespaced.

`carve_and_gapfill` below is shared with `carveme_from_orfs_cplex.py`, the
CPLEX parity variant -- the only difference between the two callers is
whether `reframed.solvers.set_default_solver("scip")` runs first. See that
module's own docstring for why it leaves the default alone instead.
"""
import sys

from carveme_gapfill import build_carveme_media_db


def read_medium_name(medium_name_path: str) -> str:
    with open(medium_name_path) as fh:
        medium_name = fh.read().strip()
    assert medium_name, f"[{medium_name_path}] is empty; there is no medium to select"
    return medium_name


def carve_and_gapfill(orfs_path: str, media_path: str, medium_name: str, out_path: str) -> None:
    from carveme.cli.carve import maincall as carve_maincall
    from carveme.cli.gapfill import maincall as gapfill_maincall

    draft_path = "carveme_from_orfs_draft.xml"
    carve_maincall(
        inputfile=orfs_path,
        input_type="protein",
        outputfile=draft_path,
        flavor="fbc2",
        verbose=True,
    )

    media_db_path = "carveme_from_orfs_media_db.tsv"
    build_carveme_media_db(media_path, medium_name, media_db_path)

    # universe / universe_file both left None: `gapfill.maincall` falls back to
    # CarveMe's own bundled default (bacteria) universe exactly the way
    # `carve.maincall` does -- confirmed by reading both sources.
    gapfill_maincall(
        inputfile=draft_path,
        media=[medium_name],
        mediadb=media_db_path,
        outputfile=out_path,
        flavor="fbc2",
        verbose=True,
    )


def main(argv: list[str]) -> int:
    orfs_path, media_path, medium_name_path, out_path = argv
    medium_name = read_medium_name(medium_name_path)

    import reframed.solvers as _solvers
    _solvers.set_default_solver("scip")

    carve_and_gapfill(orfs_path, media_path, medium_name, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
