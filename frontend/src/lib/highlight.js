// What is related to what, and how strongly — for every frame that draws a DAG.
//
// This used to live inside `DagRail`, as a boolean per node (`is this one the
// focus, or the pointer, or in the set somebody handed down?`) and a rule that
// re-derived "related" from it: an edge lit whenever either end was on. Three
// different meanings had already been merged into that boolean by then, so the
// rule could only be wrong. A parent chip meaning "these two rows and the line
// between them" lit every other line touching either row; the plan lit a node
// and nothing it came from, because nothing ever put its parents in the set.
//
// So: relations are computed here, from the graph, by name. A frame says which
// relation it means and at what strength, and `DagRail` paints what it is told
// and decides nothing. Adding a direction toggle to the plan is then a choice
// between two functions in this file rather than a new case in the drawing.
//
// The graph is `{nodes: [{id, …}], edges: [{from, to, …}]}` — the shape
// `lib/graphs.js` builds and `POST /api/dag/layout` returns. Nothing here reads
// anything but `from` and `to`, so either side works.

// Ranked, weakest first. A node reached two ways keeps the strongest reason:
// the node you are pointing at is a parent of nothing worth dimming it for.
export const RELATED = 'related'
export const POINTED = 'pointed'
export const SELECTED = 'selected'

const RANK = { [RELATED]: 1, [POINTED]: 2, [SELECTED]: 3 }

// The one place an edge's identity is spelled. `DagRail` keys its edges the
// same way; a second spelling of this is a highlight that silently never fires.
export const edgeKey = (from, to) => `${from} ${to}`

const EMPTY = () => ({ nodes: new Set(), edges: new Set() })

/** Immediate parents of `id`, and the edges from them. */
export function parents(graph, id) {
  const out = EMPTY()
  if (!graph || id == null) return out
  for (const e of graph.edges ?? []) {
    if (e.to !== id) continue
    out.nodes.add(e.from)
    out.edges.add(edgeKey(e.from, e.to))
  }
  return out
}

/** Immediate children of `id`, and the edges to them. */
export function children(graph, id) {
  const out = EMPTY()
  if (!graph || id == null) return out
  for (const e of graph.edges ?? []) {
    if (e.from !== id) continue
    out.nodes.add(e.to)
    out.edges.add(edgeKey(e.from, e.to))
  }
  return out
}

/** Both directions, one hop. */
export function neighbours(graph, id) {
  const a = parents(graph, id)
  const b = children(graph, id)
  return {
    nodes: new Set([...a.nodes, ...b.nodes]),
    edges: new Set([...a.edges, ...b.edges]),
  }
}

/**
 * One link and nothing else: two nodes and the single edge joining them.
 *
 * The recipe's parent chips mean exactly this — "this row, that row, this
 * line" — and every other edge touching either row is somebody else's
 * business. It takes no graph because there is nothing to look up.
 */
export function link(from, to) {
  return { nodes: new Set([from, to]), edges: new Set([edgeKey(from, to)]) }
}

/** A single node, no relations. */
export function only(id) {
  return id == null ? EMPTY() : { nodes: new Set([id]), edges: new Set() }
}

/**
 * Compose groups into what `DagRail` paints: `{nodes: Map<id, role>,
 * edges: Map<key, role>}`, the strongest role winning a collision.
 *
 * Each group is `{role, nodes?, edges?}` with any iterables; a falsy group is
 * skipped, so a caller can write `marks(sel && {…}, ptr && {…})` without
 * assembling a list first.
 */
export function marks(...groups) {
  const nodes = new Map()
  const edges = new Map()
  const put = (map, key, role) => {
    const was = map.get(key)
    if (was === undefined || RANK[role] > RANK[was]) map.set(key, role)
  }
  for (const g of groups) {
    if (!g) continue
    for (const n of g.nodes ?? []) put(nodes, n, g.role)
    for (const e of g.edges ?? []) put(edges, e, g.role)
  }
  return { nodes, edges }
}

/**
 * The shape every frame here wants: one group at a time.
 *
 * Whichever of the pointer and the selection is live -- the pointer if there is
 * one -- names a node, that node is marked, and `relation` decides what else
 * lights up around it. Two groups on screen at once was the "2 separate
 * highlights, the dimmer one is wrong" complaint: a stale selection glowing
 * beside whatever the pointer is on reads as the drawing having lost track.
 */
export function around(graph, { selected = null, pointed = null, relation = neighbours } = {}) {
  const id = pointed ?? selected
  if (id == null) return marks()
  const role = pointed != null ? POINTED : SELECTED
  const near = relation ? relation(graph, id) : EMPTY()
  return marks({ role: RELATED, ...near }, { role, ...only(id) })
}
