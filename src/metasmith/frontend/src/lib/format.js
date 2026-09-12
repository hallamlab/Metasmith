// Sizes as a person reads them.
//
// Decimal units, not binary: these are file sizes beside a `ls -l` and beside
// nextflow's own trace, both of which say 13.5 MB for the same file. Being
// right about 2^20 and disagreeing with everything the user can compare against
// is the worse answer.

const UNITS = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

export function bytes(n) {
  if (n == null || !Number.isFinite(n)) return '—'
  if (n < 1000) return `${n} B`
  let v = n
  let i = 0
  while (v >= 1000 && i < UNITS.length - 1) {
    v /= 1000
    i += 1
  }
  // one decimal below 10, none above: "9.4 MB" and "132 MB" both read at a
  // glance, "9.44 MB" and "132.1 MB" do not
  return `${v < 10 ? v.toFixed(1) : Math.round(v)} ${UNITS[i]}`
}
