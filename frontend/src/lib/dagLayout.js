// A layered graph layout, top to bottom, written out rather than installed.
//
// The page is served from a bundle and never reaches a CDN -- the icons and the
// config editor are hand-rolled for the same reason -- so this is the classic
// Sugiyama pipeline in miniature: break cycles, put every node on a row so all
// edges point downward, thread long edges through invisible nodes on the rows
// they cross, order each row to cut crossings, then place.
//
// The invisible nodes are not an optimisation. Without them a producer feeding
// something four rows down is drawn as one diagonal straight through whatever
// happens to sit between, and a graph of two dozen boxes reads as spaghetti;
// with them the edge is routed in a channel the layout reserved for it.
//
// The simplification that keeps this cheap is that boxes are a fixed width, so
// nothing has to be measured and there is no render-measure-render round trip.
// Graphs here are small -- the largest transform directory in the standard
// library is eleven tools -- which is well inside what a median heuristic
// handles cleanly.

const DEFAULTS = {
  nodeWidth: 132,
  colGap: 12,
  rowGap: 42,
  typeHeight: 26,
  transformHeight: 36,
  laneWidth: 2, // what a routed edge reserves on a row it only passes through
  laneGap: 5, // ...and how much clearance either side of it
}

/**
 * @param {{nodes: Array, edges: Array}} graph nodes need `id`; edges `from`/`to`
 * @returns {{nodes, edges, width, height}} nodes gain x/y/w/h; edges gain `d`
 */
export function layout(graph, options = {}) {
  const opt = { ...DEFAULTS, ...options }
  const real = (graph.nodes ?? []).map((n) => ({
    ...n,
    w: n.w ?? opt.nodeWidth,
    h: n.h ?? (n.kind === 'transform' ? opt.transformHeight : opt.typeHeight),
  }))
  if (!real.length) return { nodes: [], edges: [], width: 0, height: 0 }

  const at = new Map(real.map((n, i) => [n.id, i]))
  const edges = (graph.edges ?? [])
    .filter((e) => at.has(e.from) && at.has(e.to))
    .map((e) => ({ ...e, a: at.get(e.from), b: at.get(e.to) }))

  const back = breakCycles(real.length, edges)
  const forward = edges.filter((_, i) => !back.has(i))
  const layer = assignLayers(real.length, forward)

  // every node the layout works with, real boxes first so indices line up
  const all = real.map((n) => ({ ...n }))
  const layerOf = [...layer]
  const links = [] // adjacency the ordering and placement passes run over
  const route = new Map() // edge index -> the invisible nodes it threads through

  edges.forEach((e, ei) => {
    if (back.has(ei)) return
    const span = layerOf[e.b] - layerOf[e.a]
    if (span <= 1) {
      links.push([e.a, e.b])
      return
    }
    const chain = []
    let prev = e.a
    for (let l = layerOf[e.a] + 1; l < layerOf[e.b]; l++) {
      const v = all.length
      all.push({ id: `·${ei}:${l}`, kind: 'lane', w: opt.laneWidth, h: 0 })
      layerOf.push(l)
      chain.push(v)
      links.push([prev, v])
      prev = v
    }
    links.push([prev, e.b])
    route.set(ei, chain)
  })

  const rows = []
  layerOf.forEach((l, i) => (rows[l] ??= []).push(i))
  for (let l = 0; l < rows.length; l++) rows[l] ??= []

  const parents = all.map(() => [])
  const children = all.map(() => [])
  for (const [a, b] of links) {
    children[a].push(b)
    parents[b].push(a)
  }

  order(rows, parents, children)
  const centre = place(rows, all, parents, children, opt)

  // rows are as tall as their tallest box; y is just the running total
  const y = all.map(() => 0)
  let top = 0
  for (const row of rows) {
    const tall = Math.max(0, ...row.map((v) => all[v].h))
    for (const v of row) y[v] = top + (tall - all[v].h) / 2
    top += tall + opt.rowGap
  }

  const shift = Math.min(...all.map((n, i) => centre[i] - n.w / 2))
  const box = all.map((n, i) => ({
    ...n,
    x: centre[i] - n.w / 2 - shift,
    y: y[i],
    cx: centre[i] - shift,
    cy: y[i] + n.h / 2,
  }))

  const placed = box.slice(0, real.length)
  const width = Math.max(...box.map((n) => n.x + n.w))
  const height = Math.max(...box.map((n) => n.y + n.h))

  return {
    nodes: placed,
    edges: edges.map((e, ei) => ({
      ...e,
      back: back.has(ei),
      d: back.has(ei)
        ? loop(box[e.a], box[e.b])
        : thread(box[e.a], (route.get(ei) ?? []).map((v) => box[v]), box[e.b]),
    })),
    width,
    height,
  }
}

// Depth-first, marking every edge that closes back onto the stack. A library
// graph is not guaranteed acyclic -- one tool can consume and produce the same
// type, and a pair of them can round-trip -- and a longest-path walk over a
// cycle never terminates, so this has to come first.
function breakCycles(count, edges) {
  const out = Array.from({ length: count }, () => [])
  const back = new Set()
  edges.forEach((e, i) => {
    if (e.a === e.b) back.add(i)
    else out[e.a].push(i)
  })
  const OPEN = 1
  const DONE = 2
  const state = new Uint8Array(count)
  for (let root = 0; root < count; root++) {
    if (state[root]) continue
    // iterative: a deep chain of types would otherwise blow the stack
    const stack = [{ v: root, k: 0 }]
    state[root] = OPEN
    while (stack.length) {
      const frame = stack[stack.length - 1]
      if (frame.k >= out[frame.v].length) {
        state[frame.v] = DONE
        stack.pop()
        continue
      }
      const ei = out[frame.v][frame.k++]
      const w = edges[ei].b
      if (state[w] === OPEN) back.add(ei)
      else if (state[w] !== DONE) {
        state[w] = OPEN
        stack.push({ v: w, k: 0 })
      }
    }
  }
  return back
}

// Longest path from the sources, so an edge never points sideways or up.
function assignLayers(count, forward) {
  const indegree = new Array(count).fill(0)
  const adjacency = Array.from({ length: count }, () => [])
  for (const e of forward) {
    adjacency[e.a].push(e.b)
    indegree[e.b] += 1
  }
  const layer = new Array(count).fill(0)
  const queue = []
  for (let i = 0; i < count; i++) if (!indegree[i]) queue.push(i)
  for (let qi = 0; qi < queue.length; qi++) {
    const v = queue[qi]
    for (const w of adjacency[v]) {
      layer[w] = Math.max(layer[w], layer[v] + 1)
      if (--indegree[w] === 0) queue.push(w)
    }
  }
  return layer
}

// The median heuristic: a node wants to sit where its neighbours on the row
// above (or below) sit. Six sweeps is well past the point of diminishing
// returns at these sizes.
function order(rows, parents, children) {
  const pos = new Map()
  const reindex = () => rows.forEach((row) => row.forEach((v, k) => pos.set(v, k)))
  reindex()
  for (let pass = 0; pass < 6; pass++) {
    const down = pass % 2 === 0
    const indices = rows.map((_, i) => i)
    for (const li of down ? indices : indices.reverse()) {
      const row = rows[li]
      const key = new Map(
        row.map((v) => {
          const near = (down ? parents[v] : children[v]).map((u) => pos.get(u))
          if (!near.length) return [v, pos.get(v)]
          near.sort((a, b) => a - b)
          const m = near.length >> 1
          return [v, near.length % 2 ? near[m] : (near[m - 1] + near[m]) / 2]
        }),
      )
      const before = new Map(row.map((v, k) => [v, k]))
      row.sort((a, b) => key.get(a) - key.get(b) || before.get(a) - before.get(b))
      reindex()
    }
  }
}

// Coordinates are centres, so a routed edge's lane can be a sliver between two
// full-width boxes. Rows are packed, then nodes are pulled toward the average of
// what they are joined to -- keeping their order and their gaps -- which is what
// straightens a chain into a column instead of a staircase.
function place(rows, nodes, parents, children, opt) {
  // A lane is a line, not a box: charging it a full column of clearance is what
  // makes routed edges cost more width than they are worth on a wide graph.
  const gap = (a, b) =>
    (nodes[a].w + nodes[b].w) / 2 +
    (nodes[a].kind === 'lane' || nodes[b].kind === 'lane' ? opt.laneGap : opt.colGap)
  const centre = nodes.map(() => 0)

  // packed left to right, in absolute coordinates, then each row centred inside
  // the widest one -- which is also the ceiling the straightening below works to
  let widest = 0
  for (const row of rows) {
    if (!row.length) continue
    let x = nodes[row[0]].w / 2
    row.forEach((v, k) => {
      if (k) x += gap(row[k - 1], v)
      centre[v] = x
    })
    widest = Math.max(widest, x + nodes[row[row.length - 1]].w / 2)
  }
  for (const row of rows) {
    if (!row.length) continue
    const last = row[row.length - 1]
    const slack = (widest - (centre[last] + nodes[last].w / 2)) / 2
    for (const v of row) centre[v] += slack
  }

  // Straightening is bounded by the widest row rather than free: pulling a node
  // toward its parents only enforces a minimum gap, so without a ceiling the
  // rows drift apart from each other and a graph six rows deep ends up twice as
  // wide as anything actually on it.
  for (let pass = 0; pass < 6; pass++) {
    const down = pass % 2 === 0
    const indices = rows.map((_, i) => i)
    for (const li of down ? indices : indices.reverse()) {
      const row = rows[li]
      if (!row.length) continue
      const want = row.map((v) => {
        const near = down ? parents[v] : children[v]
        if (!near.length) return centre[v]
        return near.reduce((sum, u) => sum + centre[u], 0) / near.length
      })
      let edge = nodes[row[0]].w / 2
      row.forEach((v, k) => {
        centre[v] = Math.max(want[k], edge)
        edge = centre[v] + (k + 1 < row.length ? gap(v, row[k + 1]) : 0)
      })
      edge = widest - nodes[row[row.length - 1]].w / 2
      for (let k = row.length - 1; k >= 0; k--) {
        const v = row[k]
        centre[v] = Math.min(centre[v], edge)
        edge = centre[v] - (k ? gap(row[k - 1], v) : 0)
      }
    }
  }
  return centre
}

// A curve from the bottom of one box, through whatever lanes the edge was given
// on the rows it crosses, to the top of the next. Control points are vertical at
// every waypoint, which is what makes a threaded edge read as one line rather
// than as a series of kinks.
function thread(from, lanes, to) {
  const points = [
    { x: from.cx, y: from.y + from.h },
    ...lanes.map((l) => ({ x: l.cx, y: l.cy })),
    { x: to.cx, y: to.y },
  ]
  let d = `M ${round(points[0].x)} ${round(points[0].y)}`
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1]
    const b = points[i]
    const bend = Math.max(8, (b.y - a.y) / 2)
    d += ` C ${round(a.x)} ${round(a.y + bend)}, ${round(b.x)} ${round(b.y - bend)}, ${round(b.x)} ${round(b.y)}`
  }
  return d
}

// A cycle: drawn bowed out to the side so it reads as one, rather than as an
// edge pointing the wrong way.
function loop(from, to) {
  const side = Math.max(from.w, to.w) * 0.75
  const x = from.cx + side
  return (
    `M ${round(from.cx)} ${round(from.y)}` +
    ` C ${round(x)} ${round(from.y - 20)}, ${round(x)} ${round(to.y + to.h + 20)},` +
    ` ${round(to.cx)} ${round(to.y + to.h)}`
  )
}

const round = (n) => Math.round(n * 10) / 10
