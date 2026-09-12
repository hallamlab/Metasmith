from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::polars.env"))
survey  = model.AddRequirement(lib.GetType("amplicon::survey"))
name    = model.AddRequirement(lib.GetType("sequences::sample_name"), parents={survey})
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
    # The row label is the sample's own name, recovered through lineage. The two
    # grouped slots must NOT be paired by position: `InputGroup(report)[i]` and
    # `InputGroup(name)[i]` arrive in independent task-arrival order and are deduped
    # separately, so only declared ancestry relates them.
    pairs = []
    for p in ireports:
        src = context.SourceOf(p, name)
        assert src is not None, (
            f"no sample_name in the lineage of [{p.local.name}] -- every kraken2 "
            "report must descend from one, and every row of that input must have "
            "a parent or the lineage is dropped for all of them"
        )
        with open(src.local) as f:
            label = f.read().strip()
        assert label, f"sample_name [{src.local.name}] is empty"
        pairs.append(f"{label}={p.container}")
    samples = " ".join(pairs)
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
