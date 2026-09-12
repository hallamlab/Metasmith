"""The metaGEM-parity reconstruction (see `carveme_from_orfs.py`), on CPLEX
instead of the free SCIP solver -- CPLEX is what metaGEM itself gap fills
with, and `multiGapFill`'s cheapest-reactions-by-count fallback (see that
module's docstring) is a SCIP-only deviation this variant does not need.

    carveme_from_orfs_cplex.py <orfs.faa> <media.tsv> <medium_name.txt> <out.xml>

Deliberately does NOT call `reframed.solvers.set_default_solver("scip")`.
PYTHONPATH here already carries the private, size-uncapped CPLEX 22.2.0.0
Python runtime (`modelling::cplex_installation`, bind-mounted and put on
PYTHONPATH by this transform), so `import cplex` succeeds and reframed's own
`solvers.solver_order` (`['gurobi', 'cplex', 'scip', 'optlang']`) picks cplex
with nothing further to do here -- the same choice CarveMe's own
`carveme.config` (`solver = cplex`) already defaults to and which
`resources/env/carveme.env` documents as falling through today only because
nothing is installed. Forcing scip here would silently defeat the whole
point of this variant.
"""
import sys

from carveme_from_orfs import carve_and_gapfill, read_medium_name


def main(argv: list[str]) -> int:
    orfs_path, media_path, medium_name_path, out_path = argv
    medium_name = read_medium_name(medium_name_path)
    carve_and_gapfill(orfs_path, media_path, medium_name, out_path)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
