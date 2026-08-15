#!/usr/bin/env python3
"""Cluster quality MAGs per sample at 95%/99% ANI -- the skani_dedup transform's
logic, run standalone over the unified catalogue."""
import sys, glob, os
from pathlib import Path
G="/scratch/phyberos/gmcf3495/gapfill"
TH=[95.0,99.0]
class UF:
    def __init__(s,it): s.p={x:x for x in it}
    def find(s,x):
        while s.p[x]!=x: s.p[x]=s.p[s.p[x]]; x=s.p[x]
        return x
    def union(s,a,b):
        ra,rb=s.find(a),s.find(b)
        if ra!=rb: s.p[ra]=rb

def one(sample):
    wd=f"{G}/skani/{sample}"
    bl=f"{wd}/bins.list"
    if not os.path.exists(bl): return None
    stems=[Path(l.strip()).stem for l in open(bl) if l.strip()]
    ani={}
    p=f"{wd}/skani_ani.tsv"
    if os.path.exists(p):
        with open(p) as f:
            h=f.readline().rstrip("\n").split("\t")
            try: ai,ri,qi=h.index("ANI"),h.index("Ref_file"),h.index("Query_file")
            except ValueError: ai,ri,qi=2,0,1
            for line in f:
                t=line.rstrip("\n").split("\t")
                if len(t)<=max(ai,ri,qi): continue
                try: v=float(t[ai])
                except ValueError: continue
                a,b=Path(t[ri]).stem,Path(t[qi]).stem
                if a==b: continue
                ani[(a,b)]=v; ani[(b,a)]=v
    cid={s:{} for s in stems}; cen={s:{} for s in stems}; mean95={s:0.0 for s in stems}
    for t in TH:
        uf=UF(stems)
        for (a,b),v in ani.items():
            if v>=t and a in uf.p and b in uf.p: uf.union(a,b)
        cl={}
        for s in stems: cl.setdefault(uf.find(s),[]).append(s)
        for i,(root,mem) in enumerate(sorted(cl.items()),1):
            c=f"c{int(t)}_{i:05d}"
            if len(mem)==1: med,ms=mem[0],100.0
            else:
                sc={m: (sum(ani.get((m,o),0.0) for o in mem if o!=m)/(len(mem)-1)) for m in mem}
                med=max(sc,key=lambda x:sc[x]); ms=sc[med]
            for m in mem: cid[m][t]=c; cen[m][t]=(m==med)
            if t==95.0:
                mean95[med]=ms
                for m in mem:
                    if m!=med and len(mem)>1:
                        mean95[m]=sum(ani.get((m,o),0.0) for o in mem if o!=m)/(len(mem)-1)
    out=f"{wd}/cluster_table.tsv"
    with open(out,"w") as f:
        f.write("bin_id\tcluster_95\tis_centroid_95\tcluster_99\tis_centroid_99\tmean_intra_ani_95\n")
        for s in sorted(stems):
            f.write(f"{s}\t{cid[s][95.0]}\t{int(cen[s][95.0])}\t{cid[s][99.0]}\t{int(cen[s][99.0])}\t{mean95[s]:.3f}\n")
    n95=len({cid[s][95.0] for s in stems})
    return len(stems), n95

if __name__=="__main__":
    samples=sys.argv[1:] or sorted(os.path.basename(d) for d in glob.glob(f"{G}/skani/*"))
    tb=tc=0
    for s in samples:
        r=one(s)
        if r: print(f"{s:7} bins={r[0]:4} species_clusters_95={r[1]}"); tb+=r[0]; tc+=r[1]
    print(f"\nTOTAL quality MAGs={tb}  species-level clusters (per-sample)={tc}")
