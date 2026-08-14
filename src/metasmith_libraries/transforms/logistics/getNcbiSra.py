from metasmith.python_api import *
import json
import sys

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
image   = model.AddRequirement(lib.GetType("env::sra-tools.env"))
meta    = model.AddRequirement(lib.GetType("sequences::read_metadata"))
dep     = model.AddRequirement(lib.GetType("ncbi::sra_accession"), parents={meta})
out     = model.AddProduct(lib.GetType("ncbi::sra_cache"))

def protocol(context: ExecutionContext):
    iacc = context.Input(dep)
    iout = context.Output(out)

    with open(iacc.local) as f:
        acc_value = f.readline()
    Log.Info(f"recieved SRA accession was [{acc_value}]")

    # echo "{acc_value}" >{acc_value}
    context.ExecWithEnv().ifContainerDo(
        env = image,
        cmd = f"""
        echo "downloading"
        prefetch {acc_value} --max-size 1T
        du -sh {acc_value}
        mv {acc_value} {iout.container}
        """,
        shell="sh",
    )
    return ExecutionResult(
        manifest=[
            {
                out:iout.local,
            }
        ],
        success=iout.local.exists()
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=dep,
    labels=["local"],
    resources=Resources(
        cpus=1,
        memory=Size.GB(64),
        duration=Duration(hours=12),
    )
)
