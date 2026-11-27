import sys
L = 4121253
for paf in sys.argv[1:]:
    iv, m, b = [], 0, 0
    for l in open(paf):
        f = l.split("\t")
        if int(f[11]) < 20: continue
        iv.append((int(f[2]), int(f[3]))); m += int(f[9]); b += int(f[10])
    iv.sort(); merged = []
    for s, e in iv:
        if merged and s <= merged[-1][1]: merged[-1][1] = max(merged[-1][1], e)
        else: merged.append([s, e])
    cov = sum(e - s for s, e in merged)
    gaps = [(merged[i][1], merged[i+1][0]) for i in range(len(merged)-1) if merged[i+1][0]-merged[i][1] > 5000]
    print(f"{paf}: covered {cov/L*100:.1f}%  identity {m/b:.4f}  gaps>5kb: " + ", ".join(f"{s/1e6:.3f}-{e/1e6:.3f}Mb({(e-s)//1000}kb)" for s, e in gaps))
