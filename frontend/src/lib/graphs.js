// What the panel draws, built from the type index the browser already holds.
//
// No route and no fetch: the index is shipped whole precisely so that toggling a
// library is instant. What a node *is*, and which ones are plumbing, are content
// rules and stay here; where they sit on the page is `POST /api/dag/layout`,
// which runs the same engine that draws the plan. Three shapes come out of here,
// all in the same `{nodes, edges}` form that MiniGraph sends for placement:
//
//   transformGraph -- one tool: what it needs above it, what it makes below
//   libraryGraph   -- a library: its tools and the types that join them
//   typeGraph      -- a type: what produces it above, what consumes it below
//
// Nodes are `{id, kind, label, sub?}` where kind is 'type', 'transform' or
// 'more'; an id appears once, because a duplicate key aborts the Svelte render
// for the whole page, and because a type node shared between two tools is the
// difference between a chain and a pile of unconnected pairs.

import { splitType } from './types.js'

// Container images, environments and bundled scripts are requirements, but never
// ones a person registers -- the resource libraries supply them. Same namespaces
// the DAG renderer blacklists and the inspector hides. `env` is the newer name
// for what `containers` was, and was missing here while being blacklisted
// everywhere else, so an environment showed up as an ordinary input of every
// tool that declared one.
const PLUMBING = new Set(['containers', 'env', 'lib'])

// `splitType` rather than a split of its own -- a bare word with no `::` used to
// read as its own namespace here, so `'containers'` alone was plumbing. Nothing
// can reach that (every key the index mints is `namespace::name`), and a bare
// word is not a type name.
export const isPlumbing = (type) => PLUMBING.has(splitType(type).ns)

const typeId = (name) => `t:${name}`
const transformId = (i) => `x:${i}`

function typeNode(name) {
  return { id: typeId(name), kind: 'type', label: name, name }
}

function transformNode(index, i, extra = {}) {
  const tr = index.transforms[i]
  return {
    id: transformId(i),
    kind: 'transform',
    // `namespace::name`, which is how the drawing is told to stack the library
    // above the tool's own name -- the same half-size line a type's prefix gets.
    // It used to travel as `sub` and land to the *right* of the name, which was
    // the one place on the page where a namespace was not drawn as a namespace.
    label: tr.library_name ? `${tr.library_name}::${tr.name}` : tr.name,
    index: i,
    ...extra,
  }
}

const visible = (index, i, enabled) =>
  !enabled || enabled.has(index.transforms[i]?.library)

function dedupe(list) {
  const seen = new Set()
  return list.filter((t) => t && !seen.has(t) && (seen.add(t), true))
}

/**
 * One tool: the types it requires above it, the types it produces below.
 *
 * Plumbing is drawn here and nowhere else. A library graph is about what its
 * tools make of each other, and a container node hanging off every one of forty
 * transforms says nothing about that -- but *this* view is the one place the
 * question "what does this tool actually need to run" is being asked, and the
 * answer includes its environment. It used to be collapsed into a "+2 supplied"
 * aside on the tool's own node, which named a count rather than the thing.
 */
export function transformGraph(index, i) {
  const tr = index?.transforms?.[i]
  if (!tr) return { nodes: [], edges: [] }
  const inputs = dedupe(tr.inputs ?? [])
  const outputs = dedupe(tr.outputs ?? [])

  const nodes = []
  const seen = new Set()
  const add = (node) => {
    if (seen.has(node.id)) return
    seen.add(node.id)
    nodes.push(node)
  }

  // the grouping input carries no mark of its own: `caption` below already says
  // "one run per <type>" in words, at the top of the panel this is drawn in
  for (const t of inputs) add(typeNode(t))
  add(transformNode(index, i))
  for (const t of outputs) add(typeNode(t))

  const lineage = requirementLineage(tr)
  const caption = [
    tr.group_by ? `one run per ${tr.group_by}` : null,
    lineage.length ? `${lineage.length} lineage constraint(s) between inputs` : null,
  ].filter(Boolean)

  return {
    nodes,
    edges: [
      ...inputs.map((t) => ({ from: typeId(t), to: transformId(i) })),
      ...outputs.map((t) => ({ from: transformId(i), to: typeId(t) })),
      ...lineage,
    ],
    caption: caption.length ? caption.join(' · ') : null,
  }
}

/**
 * The constraints *between* a tool's inputs: not "it needs three files" but
 * "the reads must belong to the metadata, and the stats to those reads".
 *
 * A third of the standard library declares these, and without them a tool that
 * only runs on a properly related set looks exactly like one that takes any
 * three files. The index ships them as slot positions (`requires`); type nodes
 * here are keyed by name, so two slots of one type collapse onto one box and the
 * constraint between them has nowhere to be drawn -- those are dropped rather
 * than drawn as a self-loop.
 */
function requirementLineage(tr) {
  const slots = tr.requires ?? []
  const out = []
  const drawn = new Set()
  slots.forEach((slot, k) => {
    if (!slot?.as || isPlumbing(slot.as)) return
    for (const p of slot.parents ?? []) {
      const parent = slots[p]
      if (!parent?.as || isPlumbing(parent.as) || parent.as === slot.as) continue
      const key = `${parent.as} -> ${slot.as}`
      if (drawn.has(key)) continue
      drawn.add(key)
      out.push({ from: typeId(parent.as), to: typeId(slot.as), kind: 'lineage' })
    }
  })
  return out
}

/**
 * A library: every tool in it and the types that join them.
 *
 * Two kinds of edge. A tool is joined to a type it *named*, and a type is joined
 * to another type it can stand in for -- the second is the whole reason the
 * index carries `as`/`match` rather than names alone: a tool producing a
 * flye assembly feeds one requiring an assembly, and by name that chain is
 * invisible. The library is drawn whether or not it is enabled: looking at one
 * is not the same as switching it on.
 */
export function libraryGraph(index, path) {
  if (!index) return { nodes: [], edges: [] }
  const mine = index.transforms
    .map((tr, i) => ({ tr, i }))
    .filter(({ tr }) => tr.library === path)
  if (!mine.length) return { nodes: [], edges: [] }

  const nodes = []
  const seen = new Set()
  const add = (node) => {
    if (seen.has(node.id)) return node.id
    seen.add(node.id)
    nodes.push(node)
    return node.id
  }
  const edges = []
  const drawn = new Set()
  const join = (from, to, extra = {}) => {
    const key = `${from} -> ${to} ${extra.kind ?? ''}`
    if (from === to || drawn.has(key)) return
    drawn.add(key)
    edges.push({ from, to, ...extra })
  }

  for (const { tr, i } of mine) {
    const inputs = dedupe(tr.inputs ?? []).filter((t) => !isPlumbing(t))
    const outputs = dedupe(tr.outputs ?? []).filter((t) => !isPlumbing(t))
    const supplied = (tr.inputs ?? []).filter(isPlumbing).length
    add(
      transformNode(index, i, {
        sub: supplied ? `+${supplied} supplied` : null,
      }),
    )
    for (const t of inputs) join(add(typeNode(t)), transformId(i))
    for (const t of outputs) join(transformId(i), add(typeNode(t)))
  }

  // the type-system bridges, between types that are both already on the graph
  const here = new Set(mine.map(({ i }) => i))
  for (const node of [...nodes]) {
    if (node.kind !== 'type') continue
    for (const entry of index.by_type?.[node.name]?.consumed_by ?? []) {
      if (!here.has(entry.i) || entry.match === 'exact') continue
      if (!entry.as || entry.as === node.name || !seen.has(typeId(entry.as))) continue
      join(node.id, typeId(entry.as), { kind: 'satisfies' })
    }
  }

  return { nodes, edges, caption: `${mine.length} transform(s) in this library` }
}

/**
 * A type: what could produce it above, what could take it below.
 *
 * Capped per side, because a general type like an assembly is taken by most of
 * the library and a graph of forty boxes says less than the count does. What is
 * left out is stated rather than dropped.
 */
export function typeGraph(index, type, enabled, cap = 8) {
  if (!index || !type) return { nodes: [], edges: [] }
  const entry = index.by_type?.[type]
  const producers = (entry?.produced_by ?? []).filter((e) => visible(index, e.i, enabled))
  const consumers = (entry?.consumed_by ?? []).filter((e) => visible(index, e.i, enabled))

  const nodes = []
  const edges = []
  const centre = typeNode(type)
  const side = (list, above) => {
    const shown = list.slice(0, cap)
    for (const e of shown) {
      const tr = index.transforms[e.i]
      if (!tr) continue
      nodes.push(
        transformNode(index, e.i, {
          // the name the tool actually declared, when it is not the one asked
          // about: without it the tool looks like it named this type and did not.
          // Its library is already the stacked line above, so this stays empty
          // otherwise rather than repeating it.
          sub: e.as && e.as !== type ? `${above ? 'makes' : 'takes'} ${e.as}` : null,
        }),
      )
      edges.push(
        above
          ? { from: transformId(e.i), to: centre.id }
          : { from: centre.id, to: transformId(e.i) },
      )
    }
    const rest = list.length - shown.length
    if (rest > 0) {
      const id = `more:${above ? 'up' : 'down'}`
      nodes.push({ id, kind: 'more', label: `+${rest} more`, sub: 'listed below' })
      edges.push(above ? { from: id, to: centre.id } : { from: centre.id, to: id })
    }
  }

  side(producers, true)
  nodes.push(centre)
  side(consumers, false)

  return {
    nodes,
    edges,
    caption: `${producers.length} produce · ${consumers.length} consume`,
  }
}
