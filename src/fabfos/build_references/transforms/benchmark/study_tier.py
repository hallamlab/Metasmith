import shlex
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::python_for_data_science.env"))
extract   = model.AddRequirement(lib.GetType("bench::study_extractions"))
bridge    = model.AddRequirement(lib.GetType("ref::mnxr_lookup"))
pairs     = model.AddRequirement(lib.GetType("ref::atom_pairs"))
vocab     = model.AddRequirement(lib.GetType("ref::metabolism_vocab"))
direction = model.AddRequirement(lib.GetType("ref::direction_ratios"))
metanetx  = model.AddRequirement(lib.GetType("fabfos_data::metanetx"))
universe_m = model.AddRequirement(lib.GetType("buildlib::bench_universe.py"))
ev_lib    = model.AddRequirement(lib.GetType("lib::fabfos_evidence.py"))
hosts_gem = model.AddRequirement(lib.GetType("ref::gpr_table_gem"))
bl        = model.AddRequirement(lib.GetType("buildlib::benchmark"))
out       = model.AddProduct(lib.GetType("bench::study_benchmark"))

CHANNEL = "manual_gpr"
LANE_SET = "curated"

EXTENSIONS = ("attribution", "feature", "universe", "cohort")

Y_COLS = ("condition_id", "element", "mnxm", "expected_dir", "basis", "tier")

Y_INPUT_NAMES = ("extraction.tsv", "atom_pairs.parquet", "vocab.parquet",
                 "gpr_manual.parquet")

CONDITION_COLS = (
    "study", "condition_id", "cohort", "arm", "tier", "host", "element",
    "n_add", "n_del", "is_control", "control_kind", "read_against",
    "measured_dir", "citation", "note",
)

ELEMENTS = ("C", "N", "P", "S")

STUDIES = {
    "laser":      dict(reader="obs_mnxr", cohort="gof",      arm="gof", host="e_coli_k12"),
    "keio":       dict(reader="gene_del", cohort="lof",      arm="lof", host="e_coli_k12"),
    "eydallin":   dict(reader="gene_ovx_row", cohort="eydallin", arm="gof",
                       host="e_coli_ag1", elements=("C",),
                       directions={"glycogen_excess": "up",
                                   "glycogen_deficient": "down"}),
    "aromatic":   dict(reader="gene_row", cohort="aromatic", arm="gof", host="e_coli_epi300"),
    "pg_anionic": dict(reader="gene_row", cohort="pg_anionic", arm="gof", host="e_coli_epi300"),
    "forsberg":   dict(reader="gene_row", cohort="forsberg", arm="gof", host="e_coli_epi300"),
    "fa_supply":  dict(reader="gene_row", cohort="fa_supply", arm="gof", host="e_coli_epi300"),
    # Registered under the library's name until the lane was consolidated per paper.
    # The KEY is written into the published tables' `source`, `build_id`, `unit_id` and
    # `<study>:BASELINE` ids, so the products checked in under
    # `data/fabfos/benchmarks/fang/` still carry `aska_ffa` there until the next build.
    "fang":       dict(reader="gene_ovx", cohort="fang", arm="gof", host="e_coli_k12"),
    "scales_tol":  dict(reader="gene_ovx_row", cohort="scales_tol", arm="gof",
                        host="e_coli_bw25113",
                        directions={"tolerant_both": "up", "tolerant_15": "up",
                                    "tolerant_30": "up", "neutral": "flat"}),
    "scales_prod": dict(reader="gene_ovx_row", cohort="scales_prod", arm="gof",
                        host="e_coli_lw06", elements=("C",),
                        directions={"not_enriched": "flat"}),
    # `gene_del` rather than `gene_ovx_row`, and therefore NO `directions` map, even though
    # this screen reports metabolites moving in both directions. The direction a `gene_del`
    # row asserts is the CONDUCTANCE's -- a knockout removes a route -- and that is uniform.
    # Which METABOLITE rose or fell is per (gene, metabolite) and cannot be a per-gene
    # column at all; it lives in `data/fabfos/benchmarks/fuhrer/Y/`, keyed on the metabolite
    # it is about. Collapsing it to one label per gene is the mistake `gene_ovx_row`'s own
    # docstring warns about, made worse by 7,534 readouts per gene.
    "fuhrer":     dict(reader="gene_del", cohort="fuhrer", arm="lof",
                       host="e_coli_bw25113", elements=("C",)),
}

CONTROL_KINDS = ("baseline", "structural", "on_path", "declared")

Y_FORBIDDEN_MARKS = ("solve", "score", "ieff", "conductance", "baseline", "axes",
                     "result", "report", "ecspr_out")


def audit_y_inputs(context) -> list:
    opened = {
        "extraction": str(context.Input(extract).container),
        "atom_pairs": str(context.Input(pairs).container),
        "vocab": str(context.Input(vocab).container),
        "direction": str(context.Input(direction).container),
        "bridge": str(context.Input(bridge).container),
        "hosts_gem": str(context.Input(hosts_gem).container),
        "metanetx": str(context.Input(metanetx).container),
        "bench_universe": str(context.Input(universe_m).container),
    }
    bad = []
    for name, path in opened.items():
        low = path.lower()
        hit = [m for m in Y_FORBIDDEN_MARKS if m in low]
        if hit:
            bad.append(f"{name} at {path} matches {hit}")
    return bad


def protocol(context: ExecutionContext):
    iout = context.Output(out)
    ibl = context.Input(bl)
    tainted = audit_y_inputs(context)
    if tainted:
        raise SystemExit(
            "Y's input path reads something that is or may be an ECSPr RESULT:\n  "
            + "\n  ".join(tainted)
            + "\nA key derived from the implementation cannot fail. Y may read the "
              "network's structure -- which metabolites a reaction's atoms flow into is "
              "a fact about the model -- but never a score.")
    cmd = f"""
            python3 {ibl.container}/study_tier.py \
            --extract {context.Input(extract).container} \
            --bridge {context.Input(bridge).container} \
            --vocab {context.Input(vocab).container} \
            --pairs {context.Input(pairs).container} \
            --direction {context.Input(direction).container} \
            --hosts-gem {context.Input(hosts_gem).container} \
            --metanetx {context.Input(metanetx).container} \
            --universe-m {context.Input(universe_m).container} \
            --ev-lib {context.Input(ev_lib).container} \
            --extensions {shlex.quote(repr(EXTENSIONS))} \
            --condition-cols {shlex.quote(repr(CONDITION_COLS))} \
            --y-cols {shlex.quote(repr(Y_COLS))} \
            --elements {shlex.quote(repr(ELEMENTS))} \
            --studies {shlex.quote(repr(STUDIES))} \
            --channel {CHANNEL} \
            --lane-set {LANE_SET} \
            --out {iout.container}
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    WANT = {"extraction.tsv", "gpr_manual.parquet", "conditions.tsv", "README.md", "Y"}
    problems = []
    for study in STUDIES:
        d = iout.local / study
        if not d.is_dir():
            problems.append(f"{study}: no folder")
            continue
        have = {p.name for p in d.glob("*")}
        if have - WANT:
            problems.append(f"{study}: unexpected {sorted(have - WANT)}")
        if WANT - have:
            problems.append(f"{study}: missing {sorted(WANT - have)}")
    for p in problems:
        Log.Error(f"study_tier: {p}")
    Log.Info(f"study_tier: {len(STUDIES) - len({p.split(':')[0] for p in problems})}"
             f"/{len(STUDIES)} studies clean")
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=not problems and (iout.local / "BUILD.json").exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=extract,
    labels=["local"],
    resources=Resources(cpus=2, memory=Size.GB(8), duration=Duration(hours=1)),
)
