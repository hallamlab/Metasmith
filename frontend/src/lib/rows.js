// What an input row of the recipe is, on this side of the wire.
//
// The server's `ops.inputs` and `ops.samples` own the real answers -- what a row
// writes into the library, what makes it a sample array. These are the page's
// copies of the ones that decide *layout*, and they live here rather than in a
// view because both the recipe card and the workflow view ask them, and a row
// that draws as an array in one place and not the other is a bug nobody can see.

// not a global regex: `test` on one carries `lastIndex` between calls, so the
// same row would answer differently depending on what was asked before it
export const TOKEN = /\{[^{}]*\}/
export const hasToken = (s) => TOKEN.test(String(s ?? ''))

// A field that names exactly one column and nothing else -- no surrounding
// path, no second token -- is not a pattern to type, it is a choice from a list.
export const WHOLE_TOKEN = /^\{([^{}]*)\}$/
export const wholeToken = (s) => WHOLE_TOKEN.exec(String(s ?? '').trim())?.[1] ?? null

// What a value row holds: an ordered list of `{key, value}`. One unkeyed entry
// is a plain typed value and writes its text verbatim; anything else writes the
// JSON object the pairs describe. `ops.inputs.entries` is the same read, and a
// row written before the list existed carries a single `value` string -- read
// here, never written back.
export function entries(d) {
  const raw = d?.values
  if (!Array.isArray(raw) || !raw.length) return [{ key: '', value: d?.value ?? '' }]
  return raw
    .filter((e) => e && typeof e === 'object')
    .map((e) => ({ key: String(e.key ?? ''), value: e.value ?? '' }))
}

// An array row is not a fourth kind of thing: it is an ordinary row one of whose
// fields names a column, so nothing has to be kept in step with anything.
export const isArrayRow = (d) =>
  d?.mode === 'value'
    ? entries(d).some((e) => hasToken(e.value))
    : hasToken(d?.path)
