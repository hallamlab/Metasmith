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


def ensure_diamond_index() -> None:
    """Build CarveMe's BiGG protein index, which the 1.6.1 biocontainer does not ship.

    The image carries `carveme/data/generated/bigg_proteins.faa` and NOT the
    `bigg_proteins.dmnd` that `carve` scores genes against. Two separate things have to
    be true for that to matter, and both are:

      1. CarveMe DOES build the index itself on first use -- but that build lives in the
         command-line entry point's first-run check, not in `maincall`. This module calls
         `maincall` directly, for the solver-session reasons in the module docstring, so
         it never reaches the builder.
      2. Even reached, the builder writes beside the fasta inside site-packages, which is
         read-only in a container.

    The failure is SILENT. `carve` prints "Failed to run diamond." and returns without
    raising and without writing an SBML, so the next thing that fails is gapfill, on a
    draft file that was never created. Measured: every carve invocation in both lanes.

    CarveMe resolves the path as `project_dir + config.get('generated', 'diamond_db')`
    INSIDE maincall, reading a module-level ConfigParser, so redirecting it is a
    supported in-process change rather than a patch. The value is concatenated onto
    `project_dir`, not joined, so it must be relative TO that directory -- hence
    `os.path.relpath`, which yields the `../..` escape that reaches our writable copy.
    """
    import os
    import subprocess
    from pathlib import Path

    import carveme

    project_dir = Path(carveme.project_dir)
    shipped = project_dir / carveme.config.get("generated", "diamond_db")
    if shipped.exists():
        return

    fasta = project_dir / carveme.config.get("generated", "fasta_file")
    assert fasta.exists(), (
        f"the image ships neither the diamond index [{shipped}] nor the protein fasta "
        f"[{fasta}] it is built from; this needs a different image, not this workaround"
    )

    generated = Path.cwd() / "carveme_generated"
    generated.mkdir(parents=True, exist_ok=True)
    index = generated / "bigg_proteins.dmnd"
    if not index.exists():
        subprocess.run(
            ["diamond", "makedb", "--in", str(fasta), "--db", str(generated / "bigg_proteins")],
            check=True,
        )
    assert index.exists(), f"diamond makedb reported success and wrote no [{index}]"
    carveme.config.set("generated", "diamond_db", os.path.relpath(index, project_dir))


def carve_and_gapfill(orfs_path: str, media_path: str, medium_name: str, out_path: str) -> None:
    from carveme.cli.carve import maincall as carve_maincall
    from carveme.cli.gapfill import maincall as gapfill_maincall

    ensure_diamond_index()

    draft_path = "carveme_from_orfs_draft.xml"
    # The four scoring weights below are NOT optional and NOT tuning. `carve_maincall`
    # declares them `default_score=None, uptake_score=None, soft_score=None,
    # ref_score=None`, and CarveMe's CLI never lets those None values reach it: it
    # declares each as an argparse option with a real numeric default, all four hidden
    # behind argparse.SUPPRESS, and passes them in on every call. Calling maincall
    # directly -- which this module does deliberately, see the module docstring -- skips
    # that, an unscored reaction's coefficient stays None, and the objective is built with
    # a None coefficient. SCIP raises AttributeError on NoneType.terms; CPLEX's stricter C
    # binding raises TypeError on a non-float in the input sequence. Neither message names
    # the cause.
    #
    # These are the CLI's own values, read from its parser rather than chosen:
    #   --default-score -1.0   --uptake-score 0.0   --soft-score 1.0   --reference-score 0.0
    #
    # Same root-cause CLASS as the missing diamond index above: everything the CLI does
    # between parsing and calling maincall is setup this module has to replicate. That
    # list, read from the source in full so it does not have to be discovered a third
    # time, is: derive input_type (we pass it), derive flavor (we pass it), set the
    # default solver (each caller of this function does its own), run the first-run index
    # check (ensure_diamond_index above), and pass these four scores. Nothing else.
    carve_maincall(
        inputfile=orfs_path,
        input_type="protein",
        outputfile=draft_path,
        flavor="fbc2",
        default_score=-1.0,
        uptake_score=0.0,
        soft_score=1.0,
        ref_score=0.0,
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
