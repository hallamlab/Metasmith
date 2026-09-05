from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::polars.env"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
report  = model.AddRequirement(lib.GetType("taxonomy::kraken2_report"), parents={survey})
script  = model.AddRequirement(lib.GetType("lib::kraken_abundance.py"))
counts  = model.AddProduct(lib.GetType("amplicon::asv_table"))

RANK = "S"

def protocol(context: ExecutionContext):
    ireports=context.InputGroup(report)
    iscript=context.Input(script)
    iout=context.Output(counts)

    # A kraken2 report and an ASV table are the same shape -- samples by taxa,
    # counts -- so this produces `amplicon::asv_table` rather than a new type, and
    # the six ecology transforms lifted out of the aspire gate read it unchanged.
    #
    # The row label is the staged file's stem. Nothing in this library names a
    # sample above a kraken2 report -- `amplicon::survey` groups them but carries
    # no per-sample identity the way `ncbi::genome_name` does above an assembly --
    # so there is no better name to reach for. See research/kbase/curation/r5.
    samples = " ".join(f"{p.container.stem}={p.container}" for p in ireports)
    _cmd = f"""\
            python {iscript.container} {iout.container} {RANK} {samples}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)

    return ExecutionResult(
        manifest=[{counts: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=survey,
    resources=Resources(
        cpus=2,
        memory=Size.GB(16),
        duration=Duration(hours=1),
    ),
)
