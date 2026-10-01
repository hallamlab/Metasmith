import sys
R="/scratch/phyberos/pratama2026/metasmith/runs/nP0Jxo8W/results/"
out=open(sys.argv[2],"w")
out.write("lane\trun\tcontigs\ttotal\tn50\tge5k_bp\tge5k_n\n")
for line in open(sys.argv[1]):
    lane,path,run=line.rstrip("\n").split("\t")
    L=[];n=0
    for l in open(R+path):
        if l[0]==">":
            if n: L.append(n)
            n=0
        else: n+=len(l)-1
    if n: L.append(n)
    L.sort(reverse=True);T=sum(L);c=0;n50=0
    for x in L:
        c+=x
        if c>=T/2: n50=x;break
    g=[x for x in L if x>=5000]
    out.write(f"{lane}\t{run}\t{len(L)}\t{T}\t{n50}\t{sum(g)}\t{len(g)}\n");out.flush()
