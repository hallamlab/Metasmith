from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image     = model.AddRequirement(lib.GetType("env::foldseek.env"))
image_py  = model.AddRequirement(lib.GetType("env::polars.env"))
structs   = model.AddRequirement(lib.GetType("sequences::predicted_structures"))
out_3di   = model.AddProduct(lib.GetType("sequences::structure_3di_tokens"))


# Converts each predicted structure into its Foldseek 3Di token string and
# collects {sequence_id, aa_sequence, di3_sequence} into a single parquet.
COLLECT = r'''
import sys
from pathlib import Path
import pandas as pd

descriptor_tsv = Path(sys.argv[1])
out_parquet    = Path(sys.argv[2])

# Foldseek structureto3didescriptor output: name, aa_seq, 3di_seq, ... (tab-separated)
# Preserve the full stem as sequence_id; only strip a trailing single-letter
# chain suffix like "_A" (PDB chain) since `split("_")[0]` over-truncates IDs
# that themselves contain underscores.
rows = []
with open(descriptor_tsv) as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) < 3:
            continue
        name = Path(parts[0]).stem
        if "_" in name:
            head, tail = name.rsplit("_", 1)
            sid = head if len(tail) == 1 and tail.isalpha() else name
        else:
            sid = name
        rows.append({"sequence_id": sid, "aa_sequence": parts[1], "di3_sequence": parts[2]})

pd.DataFrame(rows).to_parquet(out_parquet, index=False)
'''


def protocol(context: ExecutionContext):
    istruct = context.Input(structs)
    iout    = context.Output(out_3di)

    context.LocalShell(f"mkdir -p structures && tar -xzf {istruct.local} -C structures")

    collect = Path("collect_3di.py")
    with open(collect, "w") as f:
        f.write(COLLECT)

    # foldseek structureto3didescriptor takes PDB/mmCIF files as positional
    # args (not a list file) and writes a TSV. Shell-glob expansion handles
    # arbitrary file counts; the absent-extension globs are silenced if empty.
    context.ExecWithEnv().ifContainerDo(
        env=image,
        binds=[
            (context.external_cwd/"structures", "/in"),
            (context.external_cwd, "/work"),
        ],
        cmd="""
            cd /work
            shopt -s nullglob
            foldseek structureto3didescriptor /in/*.pdb /in/*.cif /work/descriptor.tsv
        """,
    )

    # Collect descriptor TSV into the output parquet using a generic python env
    context.ExecWithEnv().ifContainerDo(
        env=image_py,
        cmd=f"""
            pip install --quiet --no-cache-dir pandas pyarrow
            python {collect.name} descriptor.tsv {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[{out_3di: iout.local}],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=structs,
    resources=Resources(
        cpus=2,
        memory=Size.GB(4),
        duration=Duration(hours=2),
    ),
)
