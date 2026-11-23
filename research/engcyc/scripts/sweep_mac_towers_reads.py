import sqlite3, re, gzip, sys, collections
db, out = sys.argv[1], sys.argv[2]
c = sqlite3.connect(f'file:{db}?mode=ro', uri=True)
rows = c.execute('select id,parent,name,isdir from nodes').fetchall()
par = {i: p for i, p, n, d in rows}; name = {i: n for i, p, n, d in rows}
memo = {}
def path(i):
    parts = []
    while True:
        if i in memo: parts.append(memo[i]); break
        parts.append(name[i])
        p = par[i]
        if p == i: break
        i = p
    return '/'.join(reversed(parts))
READ = re.compile(r'\.(fastq|fq|sff|sra|bam|fast5|pod5|bax\.h5|bas\.h5)(\.gz|\.bz2|\.xz|\.zip)?$', re.I)
KW = re.compile(r'sakinaw|explosiv|demolit|munition|petawawa|\brdx|hmx|\btnt|oil.?sand|tar.?sand|tailing|athabasca|\bmft|syncrude|suncor|naphthen|(^|/)LP[-_]', re.I)
fr = gzip.open(f'{out}/mac_read_files.tsv.gz', 'wt'); fk = gzip.open(f'{out}/mac_keyword_hits.tsv.gz', 'wt'); fd = gzip.open(f'{out}/mac_dirs.tsv.gz', 'wt')
groups = collections.defaultdict(lambda: [0, collections.Counter()])
for i, p, n, d in rows:
    pa = path(i)
    if d:
        memo[i] = pa
        if pa.count('/') <= 5: fd.write(pa + '\n')
    if KW.search(pa): fk.write(f'{pa}\t{d}\n')
    if not d:
        m = READ.search(n)
        if m:
            fr.write(pa + '\n')
            parts = pa.split('/')
            for depth in (3, 4, 5, 6):
                if len(parts) > depth:
                    g = groups[(depth, '/'.join(parts[:depth]))]; g[0] += 1; g[1][m.group(1).lower()] += 1
fr.close(); fk.close(); fd.close()
with open(f'{out}/mac_groups.tsv', 'w') as f:
    for (d, k), (n, kinds) in sorted(groups.items()):
        f.write(f'{d}\t{k}\t{n}\t' + ','.join(f'{a}:{b}' for a, b in kinds.most_common()) + '\n')
