from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

reads           = model.AddRequirement(lib.GetType("std::reads"))
assembly        = model.AddRequirement(lib.GetType("std::assembly"))
bam             = model.AddRequirement(lib.GetType("std::binary_alignment_map"), parents={assembly, reads})
csi             = model.AddRequirement(lib.GetType("std::binary_alignment_map_csi"), parents={assembly, reads})
image_samtools  = model.AddRequirement(lib.GetType("std::oci_image_bedtools"))
out_contig      = model.AddProduct(lib.GetType("std::per_contig_coverage"))
out_bp          = model.AddProduct(lib.GetType("std::per_bp_coverage"))

def protocol(context: ExecutionContext):
    bam_path     = context.Input(bam)
    asm_path     = context.Input(assembly)
    cov_path     = context.Input(out_contig)
    cov_bp_path  = context.Input(out_bp)
    cpus         = context.params.get("cpus")

    Log.Info("calculating coverage")
    cov_tsv = cov_bp_path.local.stem # stem to remove .gz
    _header = "\t".join(["contig", "start", "end", "fold_coverage"])
    context.ExecWithContainer(
        image = image_samtools,
        cmd = f"""
            echo "{_header}" >{cov_tsv}
            bedtools genomecov -ibam {bam_path.container} -bg >>{cov_tsv}
        """
    )

    Log.Info("compressing")
    cpus_string = ""
    if cpus is not None:
        cpus_string = f"-p {cpus}"
    with LiveShell() as shell:
        shell.Exec(f"pigz {cpus_string} -k {cov_tsv}", timeout=None)

    Log.Info("summarizing per contig")
    contig2length = {}
    with open(asm_path.local) as fa:
            current = None
            length = 0
            def _submita():
                contig2length[current] = length
            for l in fa:
                if l[0] == ">":
                    if current is not None: _submita()
                    current = l[1:-1].split(" ")[0]
                    length = 0
                else:
                    length += len(l)-1 # minus 1 for "\n"
            _submita()
    with open(cov_tsv) as f:
        with open(cov_path.local, "w") as of:
            of.write("\t".join(["contig", "fold_coverage", "contig_length"])+"\n")
            last_k = None
            entry = []
            seen = set()
            def _submit():
                if last_k is None: return
                nonlocal entry
                total = contig2length[last_k]
                seen.add(last_k)
                c = 0.0
                for span, val in entry:
                    c += (span/total)*val
                # assume no overlap, so total == total span of contig
                of.write("\t".join(str(x) for x in [last_k, c, total])+"\n")
                entry = []

            f.readline() # header
            for l in f:
                k, s, e, val = l[:-1].split("\t")
                s, e, val = [int(x) for x in [s, e, val]]
                if k != last_k:
                    _submit()
                    last_k = k
                entry.append((e-s, val))
            _submit()

            # write no coverage contigs
            for k, l in contig2length.items():
                if k in seen: continue
                of.write("\t".join(str(x) for x in [k, 0, l])+"\n")

    return ExecutionResult(success=cov_path.local.exists())

TransformInstance(
    protocol = protocol,
    group_by=assembly,
    model = model,
    output_signature = {
        out_contig: "cov_per_contig.tsv",
        out_bp:     "cov_per_bp.tsv.gz",
    },
)
