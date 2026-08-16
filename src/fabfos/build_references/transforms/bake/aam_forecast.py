"""Predict which (reaction, element) a member will be silent about, before one runs.

THIS IS THE STEP THAT COLLAPSES THREE MAPPER PASSES INTO ONE. The partial lane's target
set used to be "reactions that ended with nothing", which is a fact about a RUN -- so it
could only be computed by subtracting three finished member tables, and the members had to
run a third time over the result. This lane answers the same question from the reaction
string and from the previous bake's own records, which moves the partial lane upstream of
every mapper and takes nine member lanes down to three.

IT MAY ONLY EVER ADD SUBMISSIONS. That is what makes a pre-filter sound here where a proxy
would not be: layers are additive and ordered and `aam_layers`' gates REFUSE rather than
warn, so a reduced submission built for a reaction that maps fine is never claimed by
anything. Over-predict and you pay mapper time; under-predict and you lose exactly the
coverage the partial lane exists to add.

THE MECHANISMS ARE NAMED. Three are exact -- our two size caps and RXNMapper's context
window, which is a property of the string -- and three are read from the prior run:
`prior_hang`, `prior_timeout`, `prior_empty`. A prediction with a mechanism is a claim a
reader can go and check; a prediction with a score is not.

THE PRIOR LOGS ARE A GIVEN, AND A PARTIAL ONE. That run was killed with Indigo two thirds
through, so roughly half the universe has no record at all -- and absence of a record is
not evidence of success. The summary reports that denominator as a first-class number so a
recall figure taken over the recorded half cannot be read as one over the universe.
"""
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))
algebra     = model.AddRequirement(lib.GetType("interm::aam_algebra"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))
counts      = model.AddRequirement(lib.GetType("lookup::element_counts"))
prior       = model.AddRequirement(lib.GetType("fabfos_data::prior_bake_logs"))
bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_fc      = model.AddProduct(lib.GetType("interm::aam_forecast"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    iwl  = context.Input(worklist)
    irs  = context.Input(rescue)
    ialg = context.Input(algebra)
    irx  = context.Input(reactions)
    iec  = context.Input(counts)
    ipl  = context.Input(prior)
    ilib = context.Input(bakelib)
    iout = context.Output(out_fc)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p _lookups fc
        ln -sfn {irx.container} _lookups/reactions.parquet

        {py} -m ecspr.bake.aam.forecast build --lookups _lookups \
            --worklist {iwl.container} \
            --rescued {irs.container}/rescued.parquet \
            --element-counts {iec.container} \
            --forced {ialg.container}/forced_pairs.parquet \
            --prior-logs {ipl.container} \
            --out fc/forecast.parquet \
            --out-summary fc/summary.tsv

        cp fc/forecast.parquet {iout.container}

        {py} -m ecspr.bake.evidence collect --root _ev --tool forecast \
            --file fc/summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_fc: iout.local}, {ev: iev.local}],
        # NON-EMPTY, unlike the partial lane's product. An empty forecast would mean no
        # reaction anywhere is expected to be silent, which contradicts the 2,088
        # RXNMapper silences the prior run recorded -- so it is a read that went wrong
        # rather than a universe that got easier.
        success=(iout.local.exists() and iout.local.stat().st_size > 0
                 and (iev.local / "forecast").is_dir()
                 and any((iev.local / "forecast").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    # Four table reads and a per-reaction rule evaluation. No mapper, no rdkit parse.
    resources=Resources(cpus=2, memory=Size.GB(16), duration=Duration(hours=1)),
)
