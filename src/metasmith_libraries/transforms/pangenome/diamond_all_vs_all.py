"""Pooled all-vs-all DIAMOND blastp across a pangenome's proteomes.

Concatenates every genome's ORFs into one FASTA, rewriting each header to
`<genome>__<protein_id>` (genome = the .faa filename stem), then runs a single
sensitive all-vs-all blastp against the pooled DB. The output blast6 table holds
both self-hits (the BSR denominator) and all cross-genome hits.
"""
from pathlib import Path
from metasmith.python_api import *

lib   = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()
pan   = model.AddRequirement(lib.GetType("pangenome::pangenome"))
orfs  = model.AddRequirement(lib.GetType("sequences::orfs"), parents={pan})
image = model.AddRequirement(lib.GetType("env::diamond.env"))
out   = model.AddProduct(lib.GetType("pangenome::all_vs_all_blast"))

def protocol(context: ExecutionContext):
    orf_paths = context.InputGroup(orfs)

    pooled = Path("pooled.faa")
    seen = set()
    with open(pooled, "w") as out_f:
        for op in orf_paths:
            genome = op.local.stem
            base, n = genome, 1
            while genome in seen:
                n += 1
                genome = f"{base}-{n}"
            seen.add(genome)
            with open(op.local) as in_f:
                for line in in_f:
                    if line.startswith(">"):
                        orig = line[1:].split()[0]
                        out_f.write(f">{genome}__{orig}\n")
                    else:
                        out_f.write(line)

    iout = context.Output(out)
    threads = context.params.get("cpus", 8)
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""
            diamond makedb --in pooled.faa -d pooled_db --threads {threads}
            diamond blastp \
                --query pooled.faa \
                --db pooled_db \
                --out {iout.container} \
                --threads {threads} \
                --outfmt 6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore \
                --max-target-seqs 50 \
                --evalue 1e-3 \
                --sensitive
        """,
    )
    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=pan,
    resources=Resources(
        cpus=8,
        memory=Size.GB(16),
        duration=Duration(hours=2),
    ),
)
