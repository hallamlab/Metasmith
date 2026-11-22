from metasmith.python_api import *
from pathlib import Path

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

image = model.AddRequirement(lib.GetType("env::deepec.env"))
orfs = model.AddRequirement(lib.GetType("sequences::orf_chunk"))
out_predictions = model.AddProduct(lib.GetType("annotation::deepec_predictions_chunk"))


# DeepEC 0.4.1's one-hot encoder knows these 21 letters and drops, silently, any sequence holding
# another. Prodigal ends every complete gene with '*', so unsanitised input left only partial genes.
ALPHABET = set("ACDEFGHIKLMNPQRSTVWYX")


def sanitise(src: Path, dst: Path):
    with open(src) as fin, open(dst, "w") as fout:
        seq = []
        def flush():
            if seq:
                s = "".join(seq).upper().rstrip("*")
                fout.write("".join(c if c in ALPHABET else "X" for c in s) + "\n")
                seq.clear()
        for line in fin:
            if line.startswith(">"):
                flush()
                fout.write(">" + (line[1:].split() or ["orf"])[0] + "\n")
            else:
                seq.append(line.strip())
        flush()


def protocol(context: ExecutionContext):
    iorfs = context.Input(orfs)
    iout = context.Output(out_predictions)

    threads = context.params.get("cpus", 8)
    sanitise(Path(iorfs.local), Path("deepec_input.faa"))

    context.ExecWithEnv(
        env=image,
        cmd=f"""
            deepec \
                -p {threads} \
                -i deepec_input.faa \
                -o deepec_output
        """,
    )

    # DeepEC 0.4.1 always writes this merged table and moves its intermediates into log_files/.
    result = Path("deepec_output/DeepEC_Result.txt")
    if not result.exists():
        Log.Error(f"DeepEC wrote no [{result}]")
        return ExecutionResult(manifest=[], success=False)
    if context.LocalShell(f"cp {result} {iout.local}") != 0:
        return ExecutionResult(manifest=[], success=False)

    return ExecutionResult(
        manifest=[
            {
                out_predictions: iout.local,
            },
        ],
        success=iout.local.exists(),
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=orfs,
    resources=Resources(
        cpus=8,
        memory=Size.GB(64),
        duration=Duration(hours=4),
    ),
)
