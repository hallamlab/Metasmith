from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads           = model.AddRequirement(lib.GetType("std::reads"))
assembly        = model.AddRequirement(lib.GetType("std::assembly"))
bam             = model.AddRequirement(lib.GetType("std::binary_alignment_map"), parents={assembly, reads})
csi             = model.AddRequirement(lib.GetType("std::binary_alignment_map_csi"), parents={assembly, reads})
image_samtools  = model.AddRequirement(lib.GetType("std::oci_image_bedtools"))
out_contig      = model.AddProduct(lib.GetType("std::per_contig_coverage"))
# out_bp          = model.AddProduct(lib.GetType("std::per_contig_coverage"))

def protocol(context: ExecutionContext):
    bam_path     = context.Get(bam)
    asm_path     = context.Get(assembly)
    cov_path     = context.Get(out_contig)

    cov_tsv = "per_bp_coverage.tsv"
    context.ExecWithContainer(
        image = image_samtools,
        cmd = f"""
            bedtools genomecov -ibam {bam_path.container} -bg >./{cov_tsv}
        """
    )

    contig2length = {}
    with open(asm_path.local) as fa:
            current = None
            length = 0
            def _submit():
                contig2length[current] = length
            for l in fa:
                if l[0] == ">":
                    if current is not None: _submit()
                    current = l[1:-1].split(" ")[0]
                    length = 0
                else:
                    length += len(l)-1 # minus 1 for "\n"
            _submit()
    with open(f"./{cov_tsv}") as f:
        with open(cov_path.local, "w") as of:
            last_k = None
            entry = []
            def _submit():
                nonlocal entry
                total = contig2length[last_k]
                c = 0.0
                for span, val in entry:
                    c += (span/total)*val
                # assume no overlap, so total == total span of contig
                of.write("\t".join(str(x) for x in [last_k, c, total])+"\n")
                entry = []

            for l in f:
                k, s, e, val = l[:-1].split("\t")
                s, e, val = [int(x) for x in [s, e, val]]
                if k != last_k:
                    if last_k is not None: _submit()
                    last_k = k
                entry.append((e-s, val))
            _submit()
    return ExecutionResult(success=cov_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out_contig: "cov_per_contig.tsv",
    },
)
