<script>
  import { api } from '../lib/api.svelte.js'

  // A git-log style rail: one dot per row, a line wherever a row names another
  // as its parent. Its only job is to make lineage visible at a glance -- the
  // rows themselves already carry every field a parent link can be edited
  // through, so a dot never gets a label of its own.
  //
  // Lane placement -- which column a dot sits in, which lane a line travels
  // through to avoid another -- comes from the same engine that lays out the
  // plan DAG (`POST /api/dag/layout`, `MiniGraph.svelte`'s trick exactly),
  // so branching lineage here is arranged by the one rails algorithm the app
  // has rather than a second one invented for a narrower column.
  //
  // What that response is *not* trusted for is `y`: the server's `cx`/`cy`
  // assume a uniform row pitch, and these rows are not uniform -- a value row
  // wraps, an array row grows a count note. `y` is measured off the real DOM
  // by the caller and applied after the fact; only `lane` crosses the wire.
  let { rows = [], height = 0 } = $props()

  const LANE_PITCH = 14
  const DOT_R = 3.5

  const cache = new Map()
  const CACHE_MAX = 64

  const shape = (rs) =>
    JSON.stringify({
      n: rs.map((r) => r.key),
      e: rs.flatMap((r) => r.parents.map((p) => [p, r.key])),
    })

  let lanes = $state(null) // Map<key, {lane}> | null
  let edgeLanes = $state([]) // [{from, to, lane}]
  let failed = $state(false)

  $effect(() => {
    const rs = rows
    if (rs.length === 0) {
      lanes = null
      edgeLanes = []
      return
    }
    const edges = rs.flatMap((r) => r.parents.map((p) => ({ from: p, to: r.key })))
    // nothing to branch: every dot sits in lane 0 and no round trip is worth
    // making for a fresh recipe or a straight, unforked chain of one lane
    if (edges.length === 0) {
      lanes = new Map(rs.map((r) => [r.key, { lane: 0 }]))
      edgeLanes = []
      failed = false
      return
    }
    const key = shape(rs)
    if (cache.has(key)) {
      const cached = cache.get(key)
      lanes = cached.lanes
      edgeLanes = cached.edgeLanes
      failed = false
      return
    }
    let live = true
    ;(async () => {
      try {
        const geo = await api.post('/dag/layout', {
          nodes: rs.map((r) => ({ id: r.key, kind: 'data' })),
          edges,
        })
        const laneMap = new Map(geo.nodes.map((n) => [n.id, { lane: n.lane }]))
        // the response's own edges carry no `lane` -- only nodes do, and an
        // edge's x-position here is always its two endpoints' node lanes
        // (see `pathFor`), never a lane of its own
        const eLanes = geo.edges.map((e) => ({ from: e.from, to: e.to }))
        if (cache.size >= CACHE_MAX) cache.delete(cache.keys().next().value)
        cache.set(key, { lanes: laneMap, edgeLanes: eLanes })
        if (!live) return
        lanes = laneMap
        edgeLanes = eLanes
        failed = false
      } catch {
        // a rail that cannot lay out says nothing rather than something wrong
        if (live) {
          lanes = null
          failed = true
        }
      }
    })()
    return () => {
      live = false
    }
  })

  let yOf = $derived(new Map(rows.map((r) => [r.key, r.y])))
  let maxLane = $derived(
    lanes ? Math.max(0, ...[...lanes.values()].map((l) => l.lane)) : 0,
  )
  let width = $derived((maxLane + 1) * LANE_PITCH + DOT_R * 2)
  const laneX = (lane) => DOT_R + lane * LANE_PITCH

  // a lane change is drawn as a single cubic S-curve rather than the plan
  // DAG's jogged, corner-rounded polyline -- there is no grid to stay
  // axis-aligned on here, only two points and an even number of rows between
  // them, so one smooth curve reads as clearly and costs nothing to compute
  function pathFor(e) {
    const from = lanes?.get(e.from)
    const to = lanes?.get(e.to)
    const y1 = yOf.get(e.from)
    const y2 = yOf.get(e.to)
    if (!from || !to || y1 == null || y2 == null) return null
    const x1 = laneX(from.lane)
    const x2 = laneX(to.lane)
    if (x1 === x2) return `M${x1},${y1} L${x2},${y2}`
    const midY = (y1 + y2) / 2
    return `M${x1},${y1} C${x1},${midY} ${x2},${midY} ${x2},${y2}`
  }
</script>

{#if lanes && !failed}
  <svg class="rail" width={Math.max(width, DOT_R * 2)} {height} aria-hidden="true">
    {#each edgeLanes as e, i (i)}
      {@const d = pathFor(e)}
      {#if d}<path class="line" {d} />{/if}
    {/each}
    {#each rows as r (r.key)}
      {@const lane = lanes.get(r.key)?.lane ?? 0}
      {@const y = yOf.get(r.key)}
      {#if y != null}
        <circle class="dot" cx={laneX(lane)} cy={y} r={DOT_R} />
      {/if}
    {/each}
  </svg>
{/if}

<style>
  .rail {
    flex: 0 0 auto;
    /* height is an explicit attribute, set from the rows column's own
       measured height -- a flex-stretched height on a replaced element like
       an <svg> is inconsistent across engines, and the caller already knows
       this number from measuring the rows it is drawing beside */
    display: block;
    overflow: visible;
  }
  .line {
    fill: none;
    stroke: var(--accent);
    stroke-opacity: 0.7;
    stroke-width: 1.3;
  }
  .dot {
    fill: var(--bg);
    stroke: var(--accent);
    stroke-width: 1.5;
  }
</style>
