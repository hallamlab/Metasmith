"""Check for organellar sequence contamination in the chromosome assembly using BLASTn."""

from metasmith.python_api import *

lib      = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model    = Transform()
assembly = model.AddRequirement(lib.GetType("sequences::assembly"))
orgref   = model.AddRequirement(lib.GetType("transcriptomics::organellar_reference"))
image    = model.AddRequirement(lib.GetType("env::blast.env"))
out      = model.AddProduct(lib.GetType("transcriptomics::contamination_report"))

def protocol(context: ExecutionContext):
    iassembly = context.Input(assembly)
    iorgref   = context.Input(orgref)
    iout      = context.Output(out)

    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"""\
            makeblastdb \
                -in {iassembly.container} \
                -dbtype nucl \
                -out chr_db
            blastn \
                -query {iorgref.container} \
                -db chr_db \
                -outfmt "6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore qlen slen" \
                -evalue 1e-10 \
                -out {iout.container}
        """,
    )

    return ExecutionResult(
        manifest=[{out: iout.local}],
        success=iout.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=assembly,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=1),
    ),
)
