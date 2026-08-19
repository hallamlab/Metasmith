from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image       = model.AddRequirement(lib.GetType("env::rdkit.env"))

pairs       = model.AddRequirement(lib.GetType("interm::aam_pairs"))
worklist    = model.AddRequirement(lib.GetType("interm::aam_worklist"))
rescue      = model.AddRequirement(lib.GetType("interm::aam_rescue"))
forecast    = model.AddRequirement(lib.GetType("interm::aam_forecast"))
reactions   = model.AddRequirement(lib.GetType("lookup::reactions"))

bakelib     = model.AddRequirement(lib.GetType("buildlib::ecspr"))

out_ledger  = model.AddProduct(lib.GetType("interm::aam_ledger"))
out_vocab   = model.AddProduct(lib.GetType("ref::metabolism_vocab"))
out_pairs   = model.AddProduct(lib.GetType("ref::atom_pairs"))
ev          = model.AddProduct(lib.GetType("evidence::tool_output"))


def protocol(context: ExecutionContext):
    ipr  = context.Input(pairs)
    iwl  = context.Input(worklist)
    ires = context.Input(rescue)
    ifc  = context.Input(forecast)
    irx  = context.Input(reactions)
    ilib = context.Input(bakelib)
    ilg  = context.Output(out_ledger)
    ivoc = context.Output(out_vocab)
    ienc = context.Output(out_pairs)
    iev  = context.Output(ev)
    libdir = ilib.container.parent

    py = f"PYTHONPATH={libdir} OMP_NUM_THREADS=1 python3"

    cmd = f"""
        set -e
        mkdir -p ref

        # ---- close the ledger ------------------------------------------------------
        # Every MNXR gets an outcome, including the ones no lane ever attempted. This is
        # what lets the tier-4 gate report a miss WITH ITS REASON instead of a number
        # nobody can act on.
        {py} -m ecspr.bake.aam.worklist close \
            --worklist {iwl.container} --pairs {ipr.container}/aam_pairs.parquet \
            --rescued {ires.container}/rescued.parquet \
            --forecast {ifc.container} \
            --redox-emptied {ipr.container}/emptied.txt \
            --out ref/ledger.parquet --out-summary ref/ledger_summary.tsv
        cp ref/ledger.parquet {ilg.container}

        # ---- the bake: vocabulary + encoded pairs ---------------------------------
        # Two of the trio. The third is written by `direction_ensemble` against the
        # vocabulary minted here, so the identity block is computed once and inherited
        # rather than agreed on twice. `pairs` bakes AND selftests in one invocation and
        # exits non-zero on a failed round trip -- a merged pair of atoms RAISES the
        # network's conductance, so it reads downstream as an improvement.
        {py} -m ecspr.bake.metabolism pairs \
            --aam-pairs {ipr.container}/aam_pairs.parquet --reactions {irx.container} \
            --out-vocab {ivoc.container} --out-pairs {ienc.container}

        # The LEDGER is the half a human reads: what every reaction in the universe did,
        # next to the counts that explain it. The layer tables are `aam_stack`'s evidence
        # and the refusals are `aam_redox`'s, where each is produced.
        {py} -m ecspr.bake.evidence collect --root _ev --tool reference \
            --file ref/ledger.parquet ref/ledger_summary.tsv
        mkdir -p {iev.container}
        cp -r _ev/. {iev.container}/
    """
    context.ExecWithEnv() \
        .ifContainerDo(env=image, cmd=cmd) \
        .ifVirtualEnvDo(env=image, cmd=cmd)

    return ExecutionResult(
        manifest=[{out_ledger: ilg.local},
                  {out_vocab: ivoc.local},
                  {out_pairs: ienc.local},
                  {ev: iev.local}],
        success=(all(p.exists() and p.stat().st_size > 0
                     for p in (ilg.local, ivoc.local, ienc.local))
                 and (iev.local / "reference").is_dir()
                 and any((iev.local / "reference").iterdir())),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=image,
    resources=Resources(cpus=2, memory=Size.GB(48), duration=Duration(hours=1)),
)
