from pathlib import Path
from metasmith.python_api import *

lib     = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model   = Transform()
pan     = model.AddRequirement(lib.GetType("pangenome::pangenome"))
# Every name in this pangenome, and every genome descending from one of them.
# Stating the genome as a descendant of the name (rather than of the pangenome)
# is what makes the pairing below answerable: the two slots arrive as two
# independently-ordered groups, so the only thing relating them is lineage.
name    = model.AddRequirement(lib.GetType("ncbi::genome_name"), parents={pan})
gbk     = model.AddRequirement(lib.GetType("sequences::gbk"), parents={name})
image   = model.AddRequirement(lib.GetType("env::ppanggolin.env"))
matrix  = model.AddProduct(lib.GetType("pangenome::ppanggolin_matrix"))
pg      = model.AddProduct(lib.GetType("pangenome::ppanggolin_raw"))

def protocol(context: ExecutionContext):
    dep_paths=context.InputGroup(gbk)

    # PPanGGOLiN keys its whole run on these names and refuses a duplicate, and
    # they become the matrix columns the heatmap labels its axes with. They come
    # from the user, via the name each genome descends from -- reading them out
    # of the GenBank header instead is what made two Caulobacter vibrioides
    # strains collide on `Caulobacter-vibrioides` and abort the job. No header
    # field is both unique and readable.
    #
    # The manifest is whitespace-delimited and the heatmap turns hyphens back
    # into spaces for display, so a name is hyphen-joined on the way in.
    gb_list = Path("genbank_manifest.list")
    seen = {}
    with open(gb_list, "w") as f:
        for p in dep_paths:
            src = context.SourceOf(p, name)
            assert src is not None, (
                f"no genome_name in the lineage of [{p.local.name}] -- every "
                "assembly_accession must descend from one, and every row of "
                "that input must have a parent or the lineage is dropped for "
                "all of them"
            )
            label = "-".join(src.local.read_text().split())
            assert label, f"the genome_name for [{p.local.name}] is empty"
            assert label not in seen, (
                f"two genomes are both named [{label}] -- ppanggolin refuses "
                "duplicates, so give them distinct names"
            )
            seen[label] = p
            f.write(label+"\t"+str(p.container)+"\n")

    ipg = context.Output(pg)
    threads = context.params.get('cpus')
    threads = "" if threads is None else f"--cpu {threads}"
    context.ExecWithEnv().ifContainerDo(
        env=image,
        cmd=f"ppanggolin all --anno {gb_list} --identity 0.3 --coverage 0.8 {threads} --output {ipg.container}",
    )
    imatrix = context.Output(matrix)
    context.LocalShell(f"cp {ipg.local}/matrix.csv {imatrix.local}")
    return ExecutionResult(
        manifest=[
            {
                pg: ipg.local,
                matrix: imatrix.local
            },
        ],
        success=ipg.local.exists() and imatrix.local.exists(),
    )

TransformInstance(
    protocol=protocol,
    model=model,
    group_by=pan,
    resources=Resources(
        cpus=4,
        memory=Size.GB(8),
        duration=Duration(hours=3),
    )
)
