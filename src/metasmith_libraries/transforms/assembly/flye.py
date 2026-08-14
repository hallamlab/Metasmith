from metasmith.python_api import *
import json

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::flye.env"))
oreads  = model.AddRequirement(lib.GetType("sequences::long_reads"))
reads   = model.AddRequirement(lib.GetType("sequences::clean_long_reads"), parents={oreads})
stats   = model.AddRequirement(lib.GetType("sequences::read_qc_stats"), parents={oreads})
out     = model.AddProduct(lib.GetType("sequences::flye_assembly"))

def protocol(context: ExecutionContext):
    ireads = context.Input(reads)
    istats = context.Input(stats)
    iout = context.Output(out)

    with open(istats.local) as j:
        read_stats = json.load(j)
    print("read stats")
    for k, v in read_stats.items():
        if k.startswith("_"): continue
        print(f"    {k}: {v}")
    q = read_stats["mean_quality"]
    assert q >= 0, f"invalid q score [{q}]"
    q = min(33, q)
    p = 10**(-q/10)
    # try to guess at the right parameters here
    # default to --nano-raw, which tells flye be conservative in OLC matches
    # only use hifi if reads look good
    if q >= 20:
        preset = "--pacbio-hifi"
        err = f"--read-error {p:0.6f}"
    else:
        preset = "--nano-raw"
        err = ""

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    # memory defaults to 0.9 of available [--memory 0.9]
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
            flye --meta {err} {threads} \
                {preset} {ireads.container} \
                --out-dir long_reads_assembly
            mv long_reads_assembly/assembly.fasta {iout.container}
        """
    )
    
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=oreads,
    resources=Resources(
        cpus=8,
        memory=Size.GB(32),
        duration=Duration(hours=18),
    )
)
