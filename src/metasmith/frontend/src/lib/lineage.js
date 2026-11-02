// What a row of the recipe descends from — for both halves of it.
//
// Two layers, and the split between them is the point of the file.
//
// Layer one is a graph and nothing else. A row is `{key, parents: [key], …}`,
// only those two fields are ever read, and everything else rides along
// untouched — so a caller hands in its own view rows and gets view rows back.
// It knows nothing about inputs, outputs, types or labels. That is what lets
// the recipe's two halves answer "what may this descend from" and "what order
// do these read in" out of one implementation rather than two that drifted:
// inputs offered anything that would not make a loop, outputs offered only
// what was declared above them, and with two outputs and no lineage the first
// one's button was permanently dead.
//
// Layer two is one wire format. The request file names an output's parents by
// *position*, and refuses a position later than the row naming it (the
// assertion is in `agents/targets.py`). That rule is right for a file and
// wrong for a browser: a position is the identity and the ordinal at once, so
// re-sorting the list invalidates every handle the page is holding — including
// the one inside the closure that triggered the re-sort. So index space stops
// at the wire. In memory an output carries a minted id, exactly like an input
// row does, and the ordering is computed once on the way out. Nothing above
// this file has to remember to renumber, because nothing above this file ever
// sees a number.

// -- layer one: the graph ---------------------------------------------------

// The one place a row reference is spelled. `#` is safe as the marker: a
// library path never starts with one, which is what let row references and
// registered paths live in the same list.
export const refKey = (id) => `#${id}`
export const refId = (key) => String(key).slice(1)

export const byKey = (rows) => new Map(rows.map((r) => [r.key, r]))

/**
 * Every key each row descends from, however far up.
 *
 * A row states one level, so this is the fixpoint over them — and it is what
 * keeps a cycle out of the menus `candidates` fills.
 */
export function ancestorsOf(rows) {
  const out = new Map(rows.map((r) => [r.key, new Set(r.parents ?? [])]))
  // a set only ever gains members and there are finitely many, so a loop
  // already on disk cannot spin this
  for (;;) {
    let grew = false
    for (const set of out.values()) {
      for (const p of [...set]) {
        for (const up of out.get(p) ?? []) {
          if (set.has(up)) continue
          set.add(up)
          grew = true
        }
      }
    }
    if (!grew) break
  }
  return out
}

/**
 * Listed in the order the data descends: parents above the things made from
 * them, so a pangenome leads the assemblies it was built from rather than
 * turning up wherever its path happened to sort.
 *
 * Sorting by raw ancestor *count* used to stand in for this, but it isn't
 * actually a topological order on arrival: a fresh row with no parents yet has
 * a count of zero, so it would sort ahead of any older row that already has
 * lineage, landing wherever the depths happened to fall rather than at the
 * bottom it was added to.
 *
 * Kahn's algorithm, seeded by insertion order instead, is the honest version:
 * a row is eligible the moment every one of its parents has already been
 * placed, and among eligible rows the one that arrived first goes next. A row
 * with no parents is eligible immediately and -- having arrived after
 * everything already eligible -- keeps the position it was added to. A row
 * only moves when it is actually given a parent that sits later in the list,
 * which is the real reorder `animate:flip` is for.
 *
 * The property that follows, and that both the display and the serialiser lean
 * on: a list already in causal order comes back unchanged. That is what makes
 * this safe to call on every render *and* on every write.
 */
export function topoOrder(rows) {
  const remaining = new Map(rows.map((r) => [r.key, new Set(r.parents ?? [])]))
  const out = []
  while (remaining.size) {
    const next = rows.find((r) => remaining.has(r.key) && remaining.get(r.key).size === 0)
    // a cycle -- only ever possible in data loaded off disk, since `candidates`
    // refuses to offer one interactively -- leaves nothing eligible; rather
    // than drop rows or loop forever, flush what's left in arrival order
    if (!next) {
      for (const r of rows) if (remaining.has(r.key)) out.push(r)
      break
    }
    out.push(next)
    remaining.delete(next.key)
    for (const parents of remaining.values()) parents.delete(next.key)
  }
  return out
}

/**
 * What a row may be given as a parent, as rows — the wording of an option is
 * presentation and stays with whoever is drawing it.
 *
 * Three things are excluded, and the third is the one worth saying out loud: a
 * row cannot descend from something that descends from *it*. Nothing
 * downstream defines a cycle -- `AsSamples` walks up and then back down, so a
 * loop makes every branch the whole library.
 *
 * Pass `anc` when you already have the fixpoint; the default is here so a
 * caller cannot get a subtly different answer by forgetting to.
 */
export function candidates(rows, key, anc = ancestorsOf(rows)) {
  const self = rows.find((r) => r.key === key)
  const have = new Set(self?.parents ?? [])
  return rows.filter((r) => r.key !== key && !have.has(r.key) && !anc.get(r.key)?.has(key))
}

// -- layer two: outputs, on the wire ----------------------------------------

let seq = 0

/** An id for an output, in memory only. `o` keeps it clear of the input row
 *  ids (`d…` minted here, `i…` adopted, `imported…` from a share): the recipe
 *  marks both halves off one highlight map, so a shared key would light the
 *  wrong row. */
export const mintTargetId = () => `o${(seq++).toString(36)}`

/**
 * The stored form to the page's: positions resolved to ids.
 *
 * Targets were a list of bare type names before they could carry lineage, so
 * both spellings still arrive from disk. A parent that is not an int, or that
 * indexes nothing, is dropped rather than carried -- a bad write should cost a
 * link, never the readability of the file.
 */
export function targetsFromWire(list, mint = mintTargetId) {
  const entries = (list ?? []).map((t) =>
    typeof t === 'string'
      ? { id: mint(), type: t, wire: [] }
      : { id: mint(), type: t?.type ?? '', wire: t?.parents ?? [] },
  )
  return entries.map((e) => {
    const seen = new Set()
    for (const p of e.wire) {
      const id = Number.isInteger(p) ? entries[p]?.id : undefined
      if (id !== undefined && id !== e.id) seen.add(id)
    }
    return { id: e.id, type: e.type, parents: [...seen] }
  })
}

/**
 * The page's form to the stored one: ordered, then numbered.
 *
 * This is the single place the recipe's outputs are put in an order, and the
 * single place a parent becomes a number. Both fall out of it together, which
 * is why they live in one function: the file's rule is "a parent is declared
 * earlier", and satisfying it *is* writing the list in topological order.
 */
export function targetsToWire(targets) {
  const list = targets ?? []
  const known = new Set(list.map((t) => t.id))
  const rows = list.map((t) => ({
    key: t.id,
    parents: (t.parents ?? []).filter((p) => p !== t.id && known.has(p)),
    target: t,
  }))
  const ordered = topoOrder(rows)
  const at = new Map(ordered.map((r, i) => [r.key, i]))
  return ordered.map((r) => ({
    type: r.target.type ?? '',
    parents: r.parents.map((p) => at.get(p)).sort((a, b) => a - b),
  }))
}
