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
    # Quality alone cannot tell ONT from PacBio below Q20, so a run names its platform
    # with `long_read_platform`; unnamed, sub-Q20 reads are taken to be ONT. The tiers
    # are flye 2.9.6's own: hifi <1% error (Q20), nano-hq <3% (Q15) and, by its help,
    # R9 Guppy5+ reads in nano-hq with --read-error raised; nano-raw is 10-20% (<Q10).
    platform = (context.params.get("long_read_platform") or "").lower() or None
    assert platform in {None, "ont", "pacbio"}, f"unknown long_read_platform [{platform}]"
    err = ""
    if q >= 20 and platform != "ont":
        preset = "--pacbio-hifi"
        err = f"--read-error {p:0.6f}"
    elif platform == "pacbio":
        preset = "--pacbio-raw"
    elif q >= 10:
        preset = "--nano-hq"
        if q < 15:
            err = f"--read-error {p:0.6f}"
    else:
        preset = "--nano-raw"
    Log.Info(f"mean read quality [{q}], platform [{platform}], flye mode [{preset}]")

    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--threads {threads}"
    _cmd = f"""
            flye --meta {err} {threads} \
                {preset} {ireads.container} \
                --out-dir long_reads_assembly
            mv long_reads_assembly/assembly.fasta {iout.container}
        """
    context.ExecWithEnv(env=image, cmd=_cmd)
    
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
