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

// Rows with no name are rows still being typed, and are not params. The server
// drops them too -- this is so a page can tell whether it has anything to send.
export function toParams(rows) {
  const out = {}
  for (const r of rows ?? []) {
    const k = (r.key ?? '').trim()
    if (!k) continue
    out[k] = (r.value ?? '').trim()
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
