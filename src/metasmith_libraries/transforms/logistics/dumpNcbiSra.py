from metasmith.python_api import *
import json
import sys
import pandas as pd
from pathlib import Path

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
img_sqk = model.AddRequirement(lib.GetType("env::seqkit.env"))
image   = model.AddRequirement(lib.GetType("env::sra-tools.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
dep     = model.AddRequirement(lib.GetType("ncbi::sra_accession"), parents={meta})
cache   = model.AddRequirement(lib.GetType("ncbi::sra_cache"), parents={meta})

long    = model.AddProduct(lib.GetType("sequences::long_reads"))
model.NewProductGroup()
short_pe= model.AddProduct(lib.GetType("sequences::short_reads_pe"))
model.NewProductGroup()
short_se= model.AddProduct(lib.GetType("sequences::short_reads_se"))

def protocol(context: ExecutionContext):
    iacc = context.Input(dep)
    imeta = context.Input(meta)
    icache = context.Input(cache)

    with open(iacc.local) as f:
        acc_value = f.readline()
    Log.Info(f"recieved SRA accession was [{acc_value}]")
    with open(imeta.local) as j:
        read_meta = json.load(j)
    length_class = read_meta["length_class"]
    match length_class:
        case "long":
            is_long = True
        case "short":
            is_long = False
        case _:
            Log.Error(f"unknown [length_class] = [{length_class}]")
    parity = read_meta["parity"]
    is_pe = False
    match parity:
        case "paired":
            is_pe = True
        case "single":
            is_pe = False
        case _:
            Log.Error(f"unknown [parity] = [{parity}]")

    with open(iacc.local) as f:
        acc = f.readline().strip()
    Log.Info(f"accession: [{acc}]")
    Path(acc).symlink_to(icache.local)

    Log.Info(f"length_class=[{length_class}], parity=[{parity}]")
    if is_long:
        reads = long
        Log.Info(f"dumping as long reads")
    else:
        if is_pe:
            reads = short_pe
            Log.Info(f"dumping as short paired end reads")
        else:
            reads = short_se
            Log.Info(f"dumping as short single reads")
    ireads = context.Output(reads)
    # sed cleans redundant headers from the 3rd line to be just a "+":
    # @SRR3926590.1/1 1 length=36
    # NGAATTGGTGGAAACAGCTCAAGGCTAACCCCCTGG
    # +SRR3926590.1/1 1 length=36               <--
    # $I)'>@II<BI7I*?.IIGI1+II>655I<:6>76+
    # @SRR3926590.2/1 2 length=36
    # NAAACATCCTCCGGTCTGCGCCCCTGTGCCGCTAGG
    # +SRR3926590.2/1 2 length=36               <--
    # $5I8IAIII3IIII7A8HID=?<:;3/@>8D430I7
    # @SRR3926590.3/1 3 length=36
    # NAGCCCTGGAGAAGATTCCCGATATTGTGGCGGATC
    # +SRR3926590.3/1 3 length=36               <--
    # $IIIIII?III*H9IIIA;II>;IBIF+<2@94+.&
    cpus = context.params.get('cpus')
    if cpus is None:
        threads_param = ""
    else:
        threads_param = f"-p {cpus}"

    # with open(icache.local) as f:
    #     acc_value = f.readline().strip()
    #     print("<debug>", acc_value, acc)
    # echo "{acc_value}" >{ireads.container}

    # @$si/$ri spot_index/read_index to minimize headers
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
        du -shL {acc}
        echo "dumping"
        fastq-dump --split-spot --offset 33 --defline-seq '@$si/$ri' --defline-qual '+' --stdout \
            {acc} \
        | pigz {threads_param} -c >{ireads.container}
        """,
        shell="sh",
    )

    return ExecutionResult(
        manifest=[
            {
                reads: ireads.local
            }
        ],
        success=ireads.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=meta,
    resources=Resources(
        cpus=4,
        memory=Size.GB(64),
        duration=Duration(hours=6),
    )
)
