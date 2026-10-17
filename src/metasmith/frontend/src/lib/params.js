// Between the params rows a person edits and the mapping the server stores.
//
// Only one side coerces types: the server, which reads a value that looks like
// a JSON scalar as that scalar and leaves everything else a string. The page
// sends text and gets values back, so the only job here is making a value that
// came back round-trip through a text box unchanged.

function wouldCoerce(s) {
  try {
    return typeof JSON.parse(s) !== 'string'
  } catch {
    return false
  }
}

export function renderValue(v) {
  if (v === null || v === undefined) return ''
  if (typeof v !== 'string') return String(v)
  // `50` stored as the string "50" has to stay quoted in the box, or saving it
  // again would hand back the number
  return wouldCoerce(v) ? JSON.stringify(v) : v
}

export function paramRows(params) {
  return Object.entries(params ?? {}).map(([key, value], i) => ({
    id: i + 1,
    key,
    value: renderValue(value),
  }))
}

// The server holds names to the same pattern. `_` nests: process_tries is
// params.process.tries.
export const PARAM_NAME = /^[A-Za-z][A-Za-z0-9_]*$/

// `slurmAccount=def-x` typed whole into the name box is a name and a value.
export function splitPair(row) {
  const key = row.key ?? ''
  const at = key.indexOf('=')
  if (at < 0 || (row.value ?? '').trim()) return row
  return { ...row, key: key.slice(0, at).trim(), value: key.slice(at + 1).trim() }
}

export function paramProblems(rows) {
  const problems = []
  const seen = new Set()
  for (const r of rows ?? []) {
    const k = (r.key ?? '').trim()
    if (!k) continue
    if (!PARAM_NAME.test(k)) problems.push(`[${k}] is not a param name: letters, digits and _ only`)
    else if (seen.has(k)) problems.push(`[${k}] is given twice`)
    seen.add(k)
  }
  return problems
}

// A row with no name is still being typed, and a row with no value inherits
// the agent's default. Neither is sent.
export function toParams(rows) {
  const out = {}
  for (const r of rows ?? []) {
    const k = (r.key ?? '').trim()
    const v = (r.value ?? '').trim()
    if (!k || !v) continue
    out[k] = v
  }
  return out
}

export function sameParams(a, b) {
  const x = a ?? {}
  const y = b ?? {}
  const kx = Object.keys(x)
  const ky = Object.keys(y)
  if (kx.length !== ky.length) return false
  return kx.every((k) => k in y && String(x[k]) === String(y[k]))
}
