"""
Length-filter "binner" for long-read metagenome assemblies.

Streams a hifiasm-meta assembly and emits one FASTA per surviving contig as
`sequences::chromosomal_contig`. Each emitted contig is treated downstream as
a putative_genome (single-organism) so CheckM and GTDB-Tk can run on it
without going through an actual binner.

Threshold is a parameter (`min_length`, default 500_000 bp). No quality
filtering beyond length.
"""
from pathlib import Path
from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
asm = model.AddRequirement(lib.GetType("sequences::hifiasm_meta_assembly"))
contig = model.AddProduct(lib.GetType("sequences::chromosomal_contig"), parents={asm})


def _iter_records(fasta_path: Path):
    header = None
    seq_lines: list[str] = []
    with open(fasta_path) as f:
        for line in f:
            if line.startswith(">"):
                if header is not None:
                    yield header, "".join(seq_lines)
                header = line[1:].rstrip()
                seq_lines = []
            else:
                seq_lines.append(line.rstrip())
        if header is not None:
            yield header, "".join(seq_lines)


def protocol(context: ExecutionContext):
    iasm = context.Input(asm)
    min_length = context.params.get("min_length", 500_000)

    outputs = []
    kept = 0
    for i, (header, sequence) in enumerate(_iter_records(iasm.local)):
        if len(sequence) < min_length:
            continue
        out_contig = context.Output(contig, i=kept)
        with open(out_contig.local, "w") as f:
            f.write(f">{header}\n")
            for j in range(0, len(sequence), 80):
                f.write(sequence[j:j + 80] + "\n")
        outputs.append({contig: out_contig.local})
        kept += 1

    Log.Info(f"selected [{kept}] chromosomal contigs (min_length={min_length})")
    return ExecutionResult(
        manifest=outputs,
        success=kept > 0,
    )


TransformInstance(
    protocol=protocol,
    model=model,
    group_by=asm,
    resources=Resources(
        cpus=1,
        memory=Size.GB(4),
        duration=Duration(hours=1),
    ),
)
