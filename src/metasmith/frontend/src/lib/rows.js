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

/**
 * A row's canonical form: one shape, one key order, whatever it arrived as.
 *
 * Runs on both sides of the wire -- on the way in from the record, and again on
 * the way back out in `requestBody`. That symmetry is what lets a recipe be
 * fingerprinted at all: a row read off disk and the same row built by
 * `addInput` are otherwise two different strings for one recipe, and the
 * fingerprint would move across a reload that changed nothing.
 *
 * Read defensively, because a request body is stored verbatim and has no schema
 * behind it -- the same tolerance `ops.rows` extends on the server. `mint`
 * supplies an id for a row that somehow has none.
 */
export function normalize(list, mint) {
  return (list ?? [])
    .filter((d) => d && typeof d === 'object')
    .map((d) => ({
      id: String(d.id ?? mint()),
      mode: d.mode === 'value' ? 'value' : 'file',
      path: d.path ?? '',
      // A file row's half of the sheet binding -- the same field a value row's
      // entries each carry, and one `column_of` reads both on the server.
      // Dropping it here (as this did) cost a file row its column on every
      // reload, and the next save wrote the loss back.
      column: d.column ?? '',
      // Carried, never shown, never set on a new row. A value row states no name
      // any more, but a recipe written when it did is the only thing that can
      // still say where its *array* items are -- those are not in the record's
      // row map, only in its generation list, so dropping this before a sync has
      // run would re-mint every one of them. Delete it a release after `minted`
      // is populated everywhere.
      name: d.name ?? '',
      // What a value row holds is a list of keyed entries. A row written when it
      // was one string reads as one unkeyed entry here and is written back in
      // the list form, so a recipe migrates on its first save -- and, since one
      // unkeyed entry renders to exactly the text it always did, nothing in the
      // library moves when it does.
      values: entries(d),
      dtype: d.dtype ?? '',
      parents: [...(d.parents ?? [])],
    }))
}
