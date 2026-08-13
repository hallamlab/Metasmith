// What an input row of the recipe is, on this side of the wire.
//
// The server's `ops.rows` and `ops.samples` own the real answers -- what a row
// writes into the library, and what it binds. These are the page's
// copies of the ones that decide *layout*, and they live here rather than in a
// view because both the recipe card and the workflow view ask them, and a row
// that draws one way in one place and another in the other is a bug nobody can
// see.
//
// There is no syntax here any more. Whether a field holds its own text or reads
// a column of the sheet is decided by whether a sheet is attached, and by
// nothing written inside the field -- so braces are ordinary characters and
// there is nothing to parse.

// What a value row holds: an ordered list of `{key, value, column}`. One unkeyed
// entry is a plain typed value and writes its text verbatim; anything else
// writes the JSON object the pairs describe. `ops.rows.entries` is the same
// read, and a row written before the list existed carries a single `value`
// string -- read here, never written back.
export function entries(d) {
  const raw = d?.values
  if (!Array.isArray(raw) || !raw.length)
    return [{ key: '', value: d?.value ?? '', column: '' }]
  return raw
    .filter((e) => e && typeof e === 'object')
    .map((e) => ({
      key: String(e.key ?? ''),
      value: e.value ?? '',
      column: String(e.column ?? ''),
    }))
}

// The sheet column each field of a row binds, in the order the fields are drawn.
// A file row has one field; a value row has one per entry, and its *keys* are
// never in here -- a key names the field in the object the row writes and is
// literal in both states.
export const boundColumns = (d) =>
  d?.mode === 'value' ? entries(d).map((e) => e.column) : [String(d?.column ?? '')]

// Whether this row would register anything under a sheet. A field bound to
// nothing is a blank in the recipe, not a constant -- see `ops.samples`.
export const isBound = (d) => boundColumns(d).every((c) => c.trim() !== '')
