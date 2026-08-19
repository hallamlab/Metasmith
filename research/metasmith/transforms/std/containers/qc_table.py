from helpers import parse_args

assembly, cds, busco, cazy, output_file = parse_args(
    (
        ("assembly", "Genome assembly"),
        ("cds", "Coding sequences"),
        ("busco", "Raw BUSCO output"),
        ("cazy", "Raw CAZy output"),
        ("out", "QC metrics table for functional annotation"),
    )
)


with open(assembly) as ass:
    assembly_content = ass.read()
with open(cds) as f:
    cds_content = f.read()

with open(busco) as f:
    busco_content = f.read()
with open(cazy) as f:
    cazy_content = f.read()


def compute_fasta_len(fasta: str) -> int:
    return sum(
        len(line.strip())
        for line in fasta.splitlines()
        if not line.strip().startswith(">")
    )


assembly_len = compute_fasta_len(assembly_content)
cds_len = compute_fasta_len(cds_content)
percent_genes = cds_len / assembly_len


num_orfs = cds_content.count(">")


busco_annotation_entries = sum(1 for line in busco_content.splitlines() if line.strip())
busco_hitrate = busco_annotation_entries / num_orfs
cazy_annotation_entries = sum(1 for line in cazy_content.splitlines() if line.strip())
cazy_hitrate = cazy_annotation_entries / num_orfs


output_header = "assembly_length\tcds_length\tpercent_genes\tnum_orfs\tbusco_hits\tbusco_hitrate\tcazy_hits\tcazy_hitrate\n"
output_content = f"{assembly_len}\t{cds_len}\t{percent_genes}\t{num_orfs}\t{busco_annotation_entries}\t{busco_hitrate}\t{cazy_annotation_entries}\t{cazy_hitrate}"
with open(output_file, 'w') as out:
    out.writelines([output_header, output_content])
