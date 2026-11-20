#!/bin/bash
#SBATCH --job-name=pratama_hyb_n50
#SBATCH --account=def-shallam
#SBATCH --cpus-per-task=3
#SBATCH --mem=4G
#SBATCH --time=01:00:00
#SBATCH --output=/scratch/phyberos/e5_tax/e3meta/n50.%j.out
cd /scratch/phyberos/e5_tax/e3meta
python3 - <<'PY'
import csv
from concurrent.futures import ProcessPoolExecutor
def stats(path):
    L=[]; n=0
    with open(path) as f:
        for l in f:
            if l[0]==">":
                if n: L.append(n)
                n=0
            else: n+=len(l.strip())
    if n: L.append(n)
    L.sort(reverse=True)
    def n50(M):
        T=sum(M); c=0
        for x in M:
            c+=x
            if c*2>=T: return x
        return 0
    out=[len(L), sum(L), n50(L)]
    for t in (1000,5000):
        M=[x for x in L if x>=t]; out+= [len(M), sum(M), n50(M)]
    return out+[L[0]]
jobs=[]
minion={}
for s,l,name,shard in csv.reader(open("hybrid_spades.tsv"),delimiter="\t"):
    minion[s]=l
    jobs.append((s,"hybrid_metaspades_e3w9",f"/scratch/phyberos/e5_tax/e3_hybrid_spades/{s}.fna"))
    jobs.append((s,"opera_ms_e3r3",f"/scratch/phyberos/e5_tax/e3_r3_opera/{s}.fna"))
for s,p in csv.reader(open("opera_paths.tsv"),delimiter="\t"):
    jobs.append((s,"opera_ms_e5",p))
with ProcessPoolExecutor(3) as ex:
    res=list(ex.map(stats,[j[2] for j in jobs]))
with open("pratama_hybrid_n50.tsv","w") as out:
    out.write("short_run\tminion_run\tlane\tcontigs\ttotal_bp\tn50\tcontigs_1k\tbp_1k\tn50_1k\tcontigs_5k\tbp_5k\tn50_5k\tmax_contig\n")
    for (s,lane,_),r in sorted(zip(jobs,res)):
        out.write("\t".join([s,minion[s],lane]+[str(x) for x in r])+"\n")
PY
cat pratama_hybrid_n50.tsv
