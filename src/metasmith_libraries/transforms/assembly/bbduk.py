from metasmith.python_api import *
import json
import re

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::bbtools.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
reads   = model.AddRequirement(lib.GetType("sequences::short_reads"), parents={meta})
rstats  = model.AddRequirement(lib.GetType("sequences::read_qc_stats"), parents={reads})
out     = model.AddProduct(lib.GetType("sequences::clean_short_reads"))
disc    = model.AddProduct(lib.GetType("sequences::discarded_short_reads"))

def protocol(context: ExecutionContext):
    ireads=context.Input(reads)
    imeta=context.Input(meta)
    irstats=context.Input(rstats)
    iout=context.Output(out)
    idisc=context.Output(disc)

    with open(imeta.local) as j:
        read_meta = json.load(j)
    parity = read_meta["parity"]
    assert parity in {"single", "paired"}, f"unknown parity: [{parity}]"
    Log.Info(f"metadata parity [{parity}]")
    if parity == "paired":
        parg = "int=t"
    else:
        parg = ""

    with open(irstats.local) as j:
        d = json.load(j)
        seq_len = d["N50"]
        mean_qual = d["mean_quality"]
        phred_encoding = d["phred_encoding"].lower()
        phred_scale = 33 if phred_encoding in {"sanger"} else 64
    assert seq_len>0, "fastqc detected read length was 0"
    Log.Info(f"mean quality [{mean_qual}]")
    minlen = max((seq_len//3)+1, 20)
    minlen = min(51, minlen)
    Log.Info(f"N50 read length [{seq_len}], using min length of [{minlen}]")
    # https://doi.org/10.1128/msystems.00804-20 "Sequence data preprocessing" section
    setting = f"ktrim=r k=23 mink=11 hdist=1 tpe tbo maxns=4 qtrim=r trimq=0 usejni=t minlen={minlen} maq=3"
    # note the qin and qout settings, bbduk defaults to older illumina phred64
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"threads={threads}"
    # bbduk's bundled JVM defaults to ~85% of the *node's* RAM, which blows
    # past the SLURM cgroup limit on shared nodes. Pin -Xmx to the allocation
    # we were actually given (85% of it — matches bbtools' own headroom budget).
    mem_gb = context.params.get('memory')
    xmx = f"-Xmx{int(mem_gb*0.85)}g" if mem_gb else ""
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            bbduk.sh {xmx} {threads} \
            {parg} ref=/bbmap/resources/adapters.fa \
            qin={phred_scale} qout=33 {setting} \
            in={ireads.container} \
            out=temp.{iout.container.name} \
            outm={idisc.container.name}
            sleep 1
            [[ $(zcat temp.{iout.container.name} | wc --chars) -ne 0 ]] && mv temp.{iout.container.name} {iout.container} || echo "filtered reads were empty"
        """
    )
    
    return ExecutionResult(
        manifest=[
            {
                out: iout.local,
                disc: idisc.local,
            },
        ],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=2,
        memory=Size.GB(32),
        duration=Duration(hours=12),
    )
)
