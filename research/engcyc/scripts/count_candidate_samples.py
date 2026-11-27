import re, sys, collections
NORM = [r'\.(fastq|fq|sff|bam)(\.gz)?$', r'\.(anqdpt?|filter-METAGENOME|sorted)$', r'([._-](R?[12]|pe\.?[12]|se\.?[12]|FR?[12]|forward|reverse|singletons?|unpaired))$', r'_001$', r'_L00[1-8]$', r'_S\d+$']
def norm(n):
    for _ in range(3):
        for r in NORM: n = re.sub(r, '', n, flags=re.I)
    return n
targets = [l.rstrip('\n').split('\t') for l in open(sys.argv[1])]
paths = [l.rstrip('\n') for l in open('lims_reads.txt')]
for label, prefix, *excl in targets:
    ex = re.compile(excl[0], re.I) if excl and excl[0] else None
    stems = set(); files = 0
    for p in paths:
        if not p.startswith('sharknado/LimsData/' + prefix): continue
        if ex and ex.search(p): continue
        n = p.rsplit('/', 1)[1]
        if n.startswith('._'): continue
        files += 1; stems.add(norm(n))
    print(f'{len(stems):5d} {files:6d}  {label}  [{prefix}]  e.g. {sorted(stems)[:3]}')
