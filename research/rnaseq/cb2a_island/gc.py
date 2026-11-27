import sys, re
import pandas as pd
C = sys.argv[1]
seq = "".join(l.strip() for l in open([f for f in __import__("glob").glob(C+"/GCF_*_genomic.fna")][0]) if not l.startswith(">")).upper()
L = len(seq); g = lambda s: (s.count("G")+s.count("C"))/max(1,len(s))
a, b = 1586582, 1646942
print(f"genome {L} bp GC {g(seq):.4f}; island {a}-{b} ({b-a+1} bp) GC {g(seq[a-1:b]):.4f}")
for w0 in range(a-70000, b+40000, 10000):
    print(f"  {w0:>8}-{w0+10000:<8} GC {g(seq[w0-1:w0+9999]):.3f}{'  <island' if a<=w0+5000<=b else ''}")
gff = pd.read_csv(C+"/genomic.gff", sep="\t", comment="#", header=None, names="seq src type start end score strand phase attr".split())
near = gff[(gff.end > a-60000) & (gff.start < b+40000) & gff.type.isin(["tRNA","CDS","tmRNA","ncRNA","mobile_genetic_element","repeat_region"])].copy()
near["tag"] = near.attr.str.extract(r"locus_tag=([^;]+)"); near["product"] = near.attr.str.extract(r"product=([^;]+)")
pat = r"(?i:integrase|transposase|recombinase|phage|excisionase|resolvase|conjugal|conjugative|relaxase|mobiliz|plasmid|toxin|antitoxin|partition)|\bTr[ab][A-Z]|\bIS[0-9]"
hit = near[(near.type != "CDS") | near["product"].str.contains(pat, na=False)]
print(hit[["type","start","end","strand","tag","product"]].to_string())
# GC3 codon usage island vs genome
cds = gff[gff.type=="CDS"].copy()
def gc3(r):
    s = seq[r.start-1:r.end]
    if r.strand == "-": s = s[::-1].translate(str.maketrans("ACGT","TGCA"))
    s3 = s[2::3]; return g(s3)
cds["gc3"] = cds.apply(gc3, axis=1)
isl = cds[(cds.start>=a)&(cds.end<=b)]
print(f"GC3 genome median {cds.gc3.median():.3f}; island median {isl.gc3.median():.3f} (n={len(isl)})")
