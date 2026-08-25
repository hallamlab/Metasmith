"""The ecspr command line -- four measuring verbs, and nothing that edits a network.

    ecspr two-point --gpr ... --conditions ... --tests ...   two terminals, merged sink
    ecspr ground    --gpr ... --conditions ... --tests ...   universal leakage ground
    ecspr conditions --gpr ... --background ... --column gene  a sweep, as two tables
    ecspr draw      --gpr <pool> --like-tests ...            the null pool, as two tables
    ecspr score     --results ... --null ...                 delta, z, rank, the gate
    ecspr selftest                                           can this env measure at all

ECSPr MEASURES; IT DOES NOT EDIT. There is no weight builder, no metabolite
resolver, no add/delete policy and no background flag, because each of those is an
API for manipulating a network and a network manipulation belongs to whoever is
designing the experiment. What comes in is a GPR table; what goes out is a
measurement.

TWO TABLES, NOT ONE. ``--conditions`` says what each network IS -- an ordered set
algebra over the staged GPR tables -- and ``--tests`` says what is measured on it.
They are separate because one construction carries several probes, and because the
flux-balance arm asks a different question of the same construction. A test names
its network; a condition never names a terminal.

A PROBE STILL INFERS ITS SHAPE FROM ITS INPUTS. Given ``--source`` and ``--sinks``
and no tables it measures every staged GPR table as one network, which is the
one-shot form. There is no mode flag and no batch verb -- two entry points into one
probe is exactly the drift this package exists to end.

``--orientation as-written|reversed`` flips the baked direction reference and
nothing else. It cannot be spelled ``--direction``: that is already the PATH to the
direction-ratios parquet, and the bake's own identity block calls this field
``orientation``.
"""
from __future__ import annotations

import argparse
import sys

from . import __version__


def _add_basis(p):
    p.add_argument("--atom-pairs", required=True,
                   help="the baked atom-transfer table (parquet)")
    p.add_argument("--direction", default=None,
                   help="the baked direction-ratios parquet (path, not a direction)")
    p.add_argument("--element", default="C")
    p.add_argument("--orientation", choices=("as-written", "reversed"),
                   default="as-written",
                   help="which way round to read the baked direction reference")


def _add_inputs(p):
    p.add_argument("--gpr", nargs="+", required=True,
                   help="GPR table(s), concatenated; typically host background + study")
    p.add_argument("--conditions", default=None,
                   help="conditions table (long): what each network IS")
    p.add_argument("--tests", default=None,
                   help="ecspr_tests table: what is measured on each network")
    p.add_argument("--source", default=None, help="source metabolite (MNXM)")
    p.add_argument("--weighting", choices=("belief", "uniform"), default="belief",
                   help="belief-conserving evidence allocation, or 1.0 per unit")


def _add_output(p):
    p.add_argument("--out", required=True, help="results table (.parquet or .tsv)")
    p.add_argument("--shard-dir", default=None,
                   help="per-TEST shards for resume (default: <out>.shards). The key "
                        "changed with the condition/test split, so a directory written "
                        "before it will not resume")
    p.add_argument("--no-resume", action="store_true",
                   help="do not shard, and do not resume from shards on disk")
    p.add_argument("--log", default=None, help="append progress to this file as well")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="ecspr", description=__doc__.split("\n")[0],
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--version", action="version", version=f"ecspr {__version__}")
    p.add_argument("--where", action="store_true",
                   help="print the ecspr package actually imported, and exit")
    sub = p.add_subparsers(dest="verb")

    tp = sub.add_parser("two-point", help="source -> merged sink; effective conductance")
    _add_basis(tp); _add_inputs(tp); _add_output(tp)
    tp.add_argument("--sinks", nargs="*", default=[],
                    help="sink metabolites, merged into one ground")
    tp.set_defaults(readout_hub=[], readouts="sinks")
    tp.set_defaults(_run="two_point")

    gr = sub.add_parser("ground", help="universal leakage ground; a draw per metabolite")
    _add_basis(gr); _add_inputs(gr); _add_output(gr)
    gr.add_argument("--precursors", "--sinks", nargs="*", default=[], dest="sinks",
                    help="biomass precursors; these get --port instead of --leak")
    gr.add_argument("--leak", type=float, default=1e-6,
                    help="leak conductance per metabolite (default 1e-6)")
    gr.add_argument("--port", type=float, default=1.0,
                    help="conductance to ground for a precursor (default 1.0)")
    gr.add_argument("--readouts", choices=("sinks", "all"), default="sinks",
                    help="which metabolites to write out; 'all' emits the whole draw "
                         "vector. A conditions table's readout_hub column overrides it. "
                         "Reading a metabolite is NOT porting it -- see --precursors")
    gr.add_argument("--readout-hub", nargs="*", default=[],
                    help="explicit readout metabolites for the one-shot form")
    gr.set_defaults(_run="ground")

    cd = sub.add_parser("conditions",
                        help="build a sweep's conditions and tests from a background")
    cd.add_argument("--gpr", nargs="+", required=True,
                    help="the GPR tables the sweep selects across")
    cd.add_argument("--background", required=True, metavar="TABLE:COLUMN:VALUES",
                    help="what is always in -- typically the host's genome_unit. Written "
                         "the way a step is, so the builder's output and a hand-written "
                         "table are the same thing")
    cd.add_argument("--table", required=True, help="the table the perturbation comes from")
    cd.add_argument("--column", required=True,
                    help="the column the sweep iterates -- `gene` for a named arm")
    cd.add_argument("--family", default="gof_each",
                    choices=("gof_each", "gof_all", "lof_each", "lof_named"))
    cd.add_argument("--values", nargs="*", default=[],
                    help="restrict to these; default is every distinct value, in the "
                         "table's own order")
    cd.add_argument("--factor", type=float, default=1.0,
                    help="dosage: what one union of a row is worth (default 1.0)")
    cd.add_argument("--prefix", default="", help="prepended to every id")
    cd.add_argument("--baseline-id", default="baseline")
    cd.add_argument("--element", default="C")
    cd.add_argument("--source", required=True, help="source metabolite (MNXM)")
    cd.add_argument("--sinks", nargs="*", default=[])
    cd.add_argument("--readout-hub", nargs="*", default=[])
    cd.add_argument("--probe", default="ground", choices=("ground", "two-point"))
    cd.add_argument("--out", required=True, help="the conditions table")
    cd.add_argument("--out-tests", required=True, help="the ecspr_tests table")
    cd.add_argument("--log", default=None)
    cd.set_defaults(_run="conditions")

    dr = sub.add_parser("draw", help="draw a size-matched null pool, as the two tables")
    dr.add_argument("--gpr", nargs="+", required=True, help="the pool to draw units from")
    dr.add_argument("--like-tests", required=True,
                    help="observed ecspr_tests: supplies the measurement specs and the "
                         "size strata")
    dr.add_argument("--like-conditions", default=None,
                    help="observed conditions; --background names one of them")
    dr.add_argument("--background", default=None,
                    help="a condition_id from --like-conditions whose steps every null "
                         "construction starts from. Without it a draw is the picked "
                         "units ALONE, on a graph with no host, and every draw abstains")
    dr.add_argument("-n", "--n", type=int, required=True, dest="n",
                    help="draws per size stratum")
    dr.add_argument("--seed", type=int, required=True)
    dr.add_argument("--draw-column", default="orf",
                    help="the pool column a draw selects on (default orf, the schema's "
                         "nominator)")
    dr.add_argument("--pool-table", default=None,
                    help="the name the null conditions give the pool table; defaults to "
                         "the basename of the single --gpr path")
    dr.add_argument("--size", type=int, default=None,
                    help="stratum size for tests carrying no n_units")
    dr.add_argument("--out", required=True, help="the null conditions table")
    dr.add_argument("--out-tests", required=True, help="the null tests table")
    dr.add_argument("--log", default=None)
    dr.set_defaults(_run="draw")

    sc = sub.add_parser("score", help="delta, z, percentile rank, and the control gate")
    sc.add_argument("--results", required=True, help="observed results table")
    sc.add_argument("--null", required=True, help="null results table")
    sc.add_argument("--baseline", default=None,
                    help="the test_id the deltas are taken against, for a run whose "
                         "tests table leaves `read_against` blank. The COLUMN wins where "
                         "both are given, and disagreeing with it is refused")
    sc.add_argument("--tests", default=None,
                    help="observed ecspr_tests; supplies read_against, is_control and "
                         "the strata")
    sc.add_argument("--null-tests", default=None,
                    help="the null tests table, if its ids do not name their stratum")
    sc.add_argument("--out", required=True)
    sc.add_argument("--gate-out", default=None,
                    help="write the null-vs-control spread table here as well")
    sc.add_argument("--log", default=None)
    sc.set_defaults(_run="score")

    st = sub.add_parser("selftest",
                        help="assert this install can measure, and exit nonzero if not")
    st.set_defaults(_run="selftest")
    return p


def _logger(path):
    fh = open(path, "a", buffering=1) if path else None

    def log(msg):
        print(msg, file=sys.stderr, flush=True)
        if fh:
            fh.write(f"{msg}\n")
    return log


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.where:
        import ecspr
        print(ecspr.__file__)
        return 0
    if not getattr(args, "verb", None):
        parser.print_help()
        return 2
    if args._run == "selftest":
        from .selftest import run
        return run()
    # Imported here, not at module scope: `--where` and `--help` must work even when
    # scipy or pyarrow is missing, because "which ecspr am I running" is the first
    # question asked when they are.
    from . import commands
    fn = getattr(commands, args._run)
    return fn(args, _logger(getattr(args, "log", None))) or 0


if __name__ == "__main__":
    sys.exit(main())
