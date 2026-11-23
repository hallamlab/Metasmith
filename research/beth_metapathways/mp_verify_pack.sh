#!/bin/bash
# Usage: mp_verify_pack.sh SAMPLE
# Verifies a finished analysis_wf sample, then packs its unpacked PGDB trees into one tar.
# Prints the literal removal commands for the caller to run; removes nothing itself.
set -euo pipefail
S=$1
B=/scratch/phyberos/beth_mp
RUN=$B/mp4/runs/$S
R=$RUN/$S
P=$R/results/pgdb
SUM=$(ls -t $RUN/logs/analysis_wf/*/summary.json | head -1)
python3 - "$SUM" <<'PY'
import json, sys, collections
s = json.load(open(sys.argv[1]))
c = collections.Counter(t.get('status') for t in s['tasks'])
print('summary', s['status'], dict(c))
bad = [t['label'] for t in s['tasks'] if t.get('status') not in ('SUCCESS', 'SKIPPED', 'ALREADY_COMPUTED')]
print('not_ok', bad)
PY
echo "orfs $(grep -c '>' $R/orf_prediction/$S.faa) qced $(grep -c '>' $R/orf_prediction/$S.qced.faa) trna $(grep -vc '^#' $R/orf_prediction/$S.tRNA.gff || true) rrna $(grep -vc '^#' $R/orf_prediction/$S.rRNA.gff || true)"
for f in $R/blast_results/*.parsed.txt; do printf '%s=%s ' $(basename $f .FASTout.parsed.txt | sed "s/^$S\.//") $(tail -n +2 $f | cut -f1 | sort -u | wc -l); done; echo
echo "mag_eligible $(cut -f2 $B/mp4/mag_maps/$S.tsv | sort -u | wc -l) mag_built $(ls $P/MAGs/*/*cyc.tar.bz2 2>/dev/null | wc -l)"
ok=1
for a in $P/community/*cyc.tar.bz2 $P/MAGs/*/*cyc.tar.bz2; do
  n=$(tar -tjf $a | grep -cE '/(genes|proteins|enzrxns|reactions|pathways)\.dat$' || true)
  d=$(dirname $a); pw=$(ls $d/*_pwy.tsv 2>/dev/null | head -1)
  echo "pgdb $(basename $(dirname $a))/$(basename $a) flat=$n genes=$(grep -c '^UNIQUE-ID' $d/1.0/data/genes.dat 2>/dev/null || echo NA) pwy=$(( $(wc -l < $pw) - 1 ))"
  [ "$n" -eq 5 ] || ok=0
done
[ $ok -eq 1 ] || { echo "VERIFY FAILED: archive missing flat files"; exit 2; }
echo "inodes_before sample=$(find $RUN | wc -l) pgdb=$(find $P | wc -l)"
cd $P
DIRS=$(ls -d community/1.0 MAGs/*/1.0 2>/dev/null)
[ -n "$DIRS" ] || { echo "nothing to pack"; exit 0; }
TAR=$P/unpacked_pgdb_trees.tar
[ ! -e $TAR ] || { echo "tar exists: $TAR"; exit 3; }
tar -cf $TAR $DIRS
diff <(find $DIRS | sort) <(tar -tf $TAR | sed 's:/$::' | sort) > /dev/null || { echo "TAR LISTING MISMATCH"; exit 4; }
N=$(tar -tf $TAR | wc -l)
{ echo "$(date -Is) packed into $TAR ($N entries, $(du -h $TAR | cut -f1)), listing verified against find"; echo "$DIRS" | sed 's/^/  /'; } > $R/PACKED.txt
echo "packed $N entries; run:"
for d in $DIRS; do echo "rm -r -- $P/$d"; done
