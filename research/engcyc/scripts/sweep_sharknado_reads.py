import gzip, re, sys, collections
src, outdir = sys.argv[1], sys.argv[2]
READ = re.compile(r'\.(fastq|fq|sff|sra|bam|fast5|pod5|bax\.h5|bas\.h5|fastq\.tar|fq\.tar)(\.gz|\.bz2|\.xz|\.zip|\.dsrc|\.ora)?$', re.I)
KW = re.compile(r'sakinaw|explosiv|demolit|munition|\btnt\b|_tnt|tnt_|rdx|hmx|oil.?sand|tar.?sand|tailing|athabasca|\bmft\b|_mft|syncrude|suncor|naphthen|juan|(^|/)LP[-_]?\d', re.I)
MATE = re.compile(r'([._-](R?[12]|read[12]|[12]_sequence|sequence))?([._-]\d{3})?$', re.I)
groups = collections.defaultdict(lambda: [0, set(), set(), collections.Counter()])
reads = gzip.open(f'{outdir}/read_files.tsv.gz', 'wt')
kw = gzip.open(f'{outdir}/keyword_hits.tsv.gz', 'wt')
for line in gzip.open(src, 'rt', errors='replace'):
    path, _, isdir = line.rstrip('\n').rpartition('\t')
    if KW.search(path):
        kw.write(f'{path}\t{isdir}\n')
    if isdir == '1':
        continue
    m = READ.search(path)
    if not m:
        continue
    reads.write(path + '\n')
    parts = path.split('/')
    for depth in (3, 4, 5):
        if len(parts) <= depth:
            continue
        key = (depth, '/'.join(parts[:depth]))
        g = groups[key]
        g[0] += 1
        stem = READ.sub('', parts[-1])
        g[1].add(MATE.sub('', stem))
        g[2].add('/'.join(parts[:-1]))
        g[3][m.group(1).lower()] += 1
reads.close(); kw.close()
with open(f'{outdir}/groups.tsv', 'w') as f:
    f.write('depth\tgroup\tfiles\tsample_stems\tdirs\tkinds\n')
    for (d, k), (n, stems, dirs, kinds) in sorted(groups.items()):
        f.write(f'{d}\t{k}\t{n}\t{len(stems)}\t{len(dirs)}\t' + ','.join(f'{a}:{b}' for a, b in kinds.most_common()) + '\n')
