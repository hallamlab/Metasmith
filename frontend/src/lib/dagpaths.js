// Bake a laid-out DAG's edges into SVG paths, in the browser.
//
// A port of the three pure-geometry functions in `metasmith/models/dag_draw.py`
// -- `_jog_roles`, `_pixel_path` and `_round_corners`, plus the `_svg_path`
// emitter. Nothing here places anything: rows, lanes and the routed corridor
// all arrive already decided from `POST /api/dag/layout`, which runs the one
// layout engine metasmith has. What is ported is only the last step, turning a
// routed polyline in (row, lane) grid coordinates into pixels.
//
// That step is ported rather than fetched because the server bakes at *its*
// nominal row pitch, and one of the three surfaces drawing this cannot use
// that: a recipe's rows are not a uniform height -- a value row wraps, an array
// row grows a count note -- so its y positions are measured off the DOM after
// the fact. Handing that surface a finished path is what made it invent curves
// of its own, with lanes mirrored against every other drawing on the page.
//
// `tests/unit/test_dag_geometry_wire.py` re-bakes the same wire payload in
// Python and pins the result against what `render_svg` emits, which is the
// guard that keeps this file honest without a JS test runner.

// of a row pitch: how far apart the two directions of travel sit
export const BAND = 0.1

// Which band each point of `points` belongs in: -1 up, +1 down, 0 none.
//
// A jog gets its band from what it is *doing*, not from which half-row it sits
// on. The routing emits the departure pair only when the rail lane differs from
// the source's and the arrival pair only when it differs from the target's, so
// the pairs are identified by index. Between adjacent rows both jogs land on
// the same half-row, and reading it positionally would make every one of them a
// departure.
function jogRoles(edge, src, dst) {
  const hasDep = edge.lane !== src.lane
  const hasArr = edge.lane !== dst.lane
  const roles = new Array(edge.points.length).fill(0)
  let i = 1
  if (hasDep) {
    // with no arrival pair the rail *is* the target's lane, so on adjacent rows
    // this single jog is the arrival and belongs under, not over
    roles[1] = roles[2] = !hasArr && dst.row - src.row === 1 ? 1 : -1
    i = 3
  }
  if (hasArr) roles[i] = roles[i + 1] = 1
  return roles
}

// Trim each right-angle corner back by `bevel` along both of its legs.
//
// For a right angle the tangent length and the radius are the same number, so
// `bevel` is both, and the two trimmed points plus the corner they replaced are
// three corners of a square -- which is why the arc centre is `start + end -
// corner`. Each corner asks for `bevel`, then any leg whose two corners together
// want more than its length shares it out between them; splitting per leg rather
// than capping at half a leg matters at the ends, where a leg is consumed by one
// corner rather than two.
function roundCorners(points, bevel) {
  const pts = []
  for (const p of points) {
    // an edge spanning adjacent rows can route both jogs at the same half-row,
    // emitting a repeated point; a zero-length leg has no direction to trim along
    const last = pts[pts.length - 1]
    if (!last || last[0] !== p[0] || last[1] !== p[1]) pts.push(p)
  }
  if (pts.length < 3) return { points: pts, arcs: new Map() }

  const legs = []
  for (let i = 0; i < pts.length - 1; i++) {
    legs.push(Math.hypot(pts[i + 1][0] - pts[i][0], pts[i + 1][1] - pts[i][1]))
  }
  const cut = pts.map((_, i) => (i === 0 || i === pts.length - 1 ? 0 : bevel))
  for (let i = 0; i < legs.length; i++) {
    const want = cut[i] + cut[i + 1]
    if (want > legs[i]) {
      const k = legs[i] / want
      cut[i] *= k
      cut[i + 1] *= k
    }
  }

  const out = [pts[0]]
  const arcs = new Map()
  for (let i = 1; i < pts.length - 1; i++) {
    const [ax, ay] = pts[i - 1]
    const [bx, by] = pts[i]
    const [cx, cy] = pts[i + 1]
    const d = cut[i]
    const la = legs[i - 1]
    const lc = legs[i]
    const start = [bx + ((ax - bx) * d) / la, by + ((ay - by) * d) / la]
    const end = [bx + ((cx - bx) * d) / lc, by + ((cy - by) * d) / lc]
    const prev = out[out.length - 1]
    // already there when the flat run between two corners has collapsed
    if (start[0] !== prev[0] || start[1] !== prev[1]) out.push(start)
    const here = out[out.length - 1]
    if (end[0] === here[0] && end[1] === here[1]) continue
    // y grows downward, so an SVG sweep of 1 is a clockwise turn on screen
    const turn = (bx - ax) * (cy - by) - (by - ay) * (cx - bx)
    arcs.set(out.length - 1, { radius: d, sweep: turn > 0 ? 1 : 0 })
    out.push(end)
  }
  out.push(pts[pts.length - 1])
  return { points: out, arcs }
}

function svgPath(points, arcs) {
  const f = (v) => v.toFixed(1)
  const d = [`M ${f(points[0][0])},${f(points[0][1])}`]
  for (let i = 1; i < points.length; i++) {
    const [x, y] = points[i]
    const arc = arcs.get(i - 1)
    if (!arc) d.push(`L ${f(x)},${f(y)}`)
    else d.push(`A ${f(arc.radius)},${f(arc.radius)} 0 0 ${arc.sweep} ${f(x)},${f(y)}`)
  }
  return d.join(' ')
}

/**
 * The pixel grid a drawing is baked on.
 *
 * `rowY` replaces the engine's uniform row pitch with measured positions, one
 * per row index. `xOffset` shifts every lane right, which is how a surface
 * reserves a wider gutter than its current lane count needs so that adding the
 * first branch does not shift every row sideways.
 */
export function makeGrid(geo, { rowY = null, xOffset = 0 } = {}) {
  const laneX = (geo.lane_x ?? []).map((x) => x + xOffset)
  const rows = rowY && rowY.length ? rowY : null

  const nominalY = (row) => geo.margin + (row + 0.5) * geo.row_pitch
  const y = (row) => {
    if (!rows) return nominalY(row)
    const lo = Math.floor(row)
    const frac = row - lo
    const a = rows[Math.min(Math.max(lo, 0), rows.length - 1)]
    if (!frac) return a
    const b = rows[Math.min(lo + 1, rows.length - 1)]
    return a + frac * (b - a)
  }
  // The band offset is a fraction of the gap this jog actually sits in, not of
  // the engine's nominal pitch. Under measured rows a tight pair would otherwise
  // get a jog that overshoots into its neighbour.
  const gap = (row) => {
    if (!rows) return geo.row_pitch
    const lo = Math.max(Math.floor(row), 0)
    const hi = Math.min(lo + 1, rows.length - 1)
    return hi > lo ? rows[hi] - rows[lo] : geo.row_pitch
  }

  return {
    x: (lane) => laneX[Math.floor(lane)] ?? 0,
    y,
    gap,
    laneX,
    lanePitch: geo.lane_pitch,
    markerD: geo.marker_d,
  }
}

/**
 * Every forward edge of `geo` as an SVG path, in the order the layout engine
 * sorted them -- overlapping strokes stack the way the rendered SVG stacks them
 * only if that order survives into the DOM.
 *
 * Both ends are trimmed by that marker's own half-height rather than by one
 * radius for all three shapes: a triangle is 0.866 of its width, so a fixed trim
 * leaves a gap under one and overshoots into another.
 */
export function bakeEdges(geo, grid) {
  const byId = new Map(geo.nodes.map((n) => [n.id, n]))
  const out = []
  for (const e of geo.edges) {
    if (e.back) continue
    const src = byId.get(e.from)
    const dst = byId.get(e.to)
    if (!src || !dst || !e.points?.length) continue
    const roles = jogRoles(e, src, dst)
    const pts = e.points.map(([row, lane], i) => [
      grid.x(lane),
      grid.y(row) + roles[i] * BAND * grid.gap(row),
    ])
    pts[0] = [pts[0][0], pts[0][1] + src.marker_h / 2]
    const last = pts.length - 1
    pts[last] = [pts[last][0], pts[last][1] - dst.marker_h / 2]
    const { points, arcs } = roundCorners(pts, grid.lanePitch / 2)
    out.push({ from: e.from, to: e.to, hue: e.hue, d: svgPath(points, arcs) })
  }
  return out
}

/**
 * Where the label column starts, once `minLanes` has been honoured.
 *
 * Lanes run right to left, so lane 0 -- where most nodes sit -- is the one next
 * to the labels, and a gutter is widened by pushing every lane right rather than
 * by adding lanes on the far side.
 */
export function gutter(geo, minLanes = 1) {
  const lanes = (geo.lane_x ?? []).length || 1
  const shift = Math.max(0, minLanes - lanes) * geo.lane_pitch
  const labelX = geo.nodes?.[0]?.label_x ?? geo.margin + geo.marker_d
  return { shift, width: labelX + shift }
}
