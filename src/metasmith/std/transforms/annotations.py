from metasmith.python_api import *

lib = TransformInstanceLibrary.ResolveParentLibrary(__file__)
model = Transform()

assembly = model.AddRequirement(lib.GetType("std::assembly"))
cds       = model.AddRequirement(lib.GetType("std::coding_sequences"), parents={assembly})
gff       = model.AddRequirement(lib.GetType("std::gene_features"), parents={assembly})
bakta     = model.AddRequirement(lib.GetType("std::bakta_annotations"), parents={assembly})
kofamscan = model.AddRequirement(lib.GetType("std::kofamscan_annotations"), parents={assembly})
cazy      = model.AddRequirement(lib.GetType("std::cazy_annotations"), parents={assembly})
busco     = model.AddRequirement(lib.GetType("std::busco_annotations"), parents={assembly})
busco_map = model.AddRequirement(lib.GetType("std::busco_map"))
image     = model.AddRequirement(lib.GetType("std::oci_image_script_runner"))
out       = model.AddProduct(lib.GetType("std::functional_annotations"))

def protocol(context: ExecutionContext):
    bakta_path     = context.Get(bakta)
    cds_path       = context.Get(cds)
    gff_path       = context.Get(gff)
    kofamscan_path = context.Get(kofamscan)
    cazy_path      = context.Get(cazy)
    busco_path     = context.Get(busco)
    busco_map_path = context.Get(busco_map)
    out_path       = context.Get(out)

    context.ExecWithContainer(
        image,
        cmd = f"""
            cd {out_path.container.parent}
            mkdir annotations/
            cd annotations
            mkdir raw_outputs/
            cd raw_outputs/
            cp {cds_path.container} coding_sequences.fasta
            cp {gff_path.container} gene_features.gff3
            cp {kofamscan_path.container} kofamscan.tsv
            cp -r {bakta_path.container} bakta/
            cp {busco_path.container} busco.tsv
            cp {cazy_path.container} cazy.tsv
            cd ../

            awk 'BEGIN {{ FS="\t"; OFS="\t" }} \
                {{ \
                    n = split($2, parts, "_"); \
                    species_id = "NA"; \
                    for (i = 1; i <= n; i++) {{ \
                        if (parts[i] ~ /^[0-9]+$/ && length(parts[i]) > 3) {{ \
                            species_id = parts[i]; \
                            break; \
                        }} \
                    }} \
                    print $1, $2, $3, $11, species_id \
                }}' \
                raw_outputs/busco.tsv | sort -k1,1 > busco_clean.tsv
            awk 'BEGIN {{OFS="\t"}} \
                FNR==NR {{ tax[$1]=$2; next }} \
                FNR==1 {{ print "qseqid","busco_sseqid","busco_pident","busco_evalue","species_id","taxonomy"; next }} \
                        {{ print $0,(tax[$5]?tax[$5]:"NA") }}' \
                {busco_map_path.container} busco_clean.tsv \
            > busco_annotated.tsv
            rm -f busco_clean.tsv

            awk -F'\t' 'BEGIN {{ OFS="\t"; print "qseqid","cazy_sseqid","cazy_pident","cazy_length","cazy_evalue","cazy_score","cazy_family","cazy_family_name" }} \
                {{ \
                    cazy_family = "unknown"; \
                    family_desc = "unknown_function"; \
                    n = split($2, parts, "|"); \
                    if (n > 0) {{ \
                        family_part = parts[n]; \
                        if (family_part ~ /GH[0-9]+(_[0-9]+)?/) {{ \
                            match(family_part, /GH[0-9]+(_[0-9]+)?/); \
                            cazy_family = substr(family_part, RSTART, RLENGTH); \
                            family_desc = "glycoside_hydrolase"; \
                        }} \
                        else if (family_part ~ /GT[0-9]+/) {{ \
                            match(family_part, /GT[0-9]+/); \
                            cazy_family = substr(family_part, RSTART, RLENGTH); \
                            family_desc = "glycosyltransferase"; \
                        }} \
                        else if (family_part ~ /CBM[0-9]+/) {{ \
                            match(family_part, /CBM[0-9]+/); \
                            cazy_family = substr(family_part, RSTART, RLENGTH); \
                            family_desc = "carbohydrate_binding_module"; \
                        }} \
                        else if (family_part ~ /AA[0-9]+(_[0-9]+)?/) {{ \
                            match(family_part, /AA[0-9]+(_[0-9]+)?/); \
                            cazy_family = substr(family_part, RSTART, RLENGTH); \
                            family_desc = "auxiliary_activity"; \
                        }} \
                        else if (family_part ~ /PL[0-9]+(_[0-9]+)?/) {{ \
                            match(family_part, /PL[0-9]+(_[0-9]+)?/); \
                            cazy_family = substr(family_part, RSTART, RLENGTH); \
                            family_desc = "polysaccharide_lyase"; \
                        }} \
                        else if (family_part ~ /CE[0-9]+(_[0-9]+)?/) {{ \
                            match(family_part, /CE[0-9]+(_[0-9]+)?/); \
                            cazy_family = substr(family_part, RSTART, RLENGTH); \
                            family_desc = "carbohydrate_esterase"; \
                        }} \
                    }} \
                    print $1, $2, $3, $4, $11, $12, cazy_family, family_desc; \
            }}' raw_outputs/cazy.tsv > cazy_annotated.tsv
        """
    )
    return ExecutionResult(success=out_path.local.exists())

TransformInstance(
    protocol = protocol,
    model = model,
    output_signature = {
        out: "annotations/",
    },
)
