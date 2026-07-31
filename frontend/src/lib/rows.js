// What an input row of the recipe is, on this side of the wire.
//
// The server's `ops.inputs` and `ops.samples` own the real answers -- what a row
// writes into the library, what makes it a sample array, what to call it. These
// are the page's copies of the two that decide *layout*, and they live here
// rather than in a view because both the recipe card and the workflow view ask
// them, and a row that draws as an array in one place and not the other is a bug
// nobody can see.

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

// A row has nothing to be called until it is filled in, and an empty string in
// another row's lineage reads as a bug. A value row has nothing it is *called*
// at all -- the library names its file and that name is a uuid nobody types --
// so what was typed into its first box is the label, clamped, because a value is
// not a name and a read-pair descriptor is three lines long.
const clamp = (s) => (s.length > 40 ? `${s.slice(0, 40)}…` : s)

export function rowLabel(d) {
  let text = ''
  if (d?.mode === 'value') {
    const [first] = entries(d)
    // clamped after the key is folded in, as `ops.samples.row_label` does it,
    // so the two never disagree about where a long line is cut
    text = String(first?.value ?? '').trim().split('\n')[0]
    if (first?.key) text = text ? `${first.key}: ${text}` : first.key
    text = clamp(text)
  } else {
    text = d?.path ?? ''
  }
  return text || (d?.dtype ? `a new ${d.dtype}` : 'a new row')
}
