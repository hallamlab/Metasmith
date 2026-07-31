<script>
  import { api } from '../lib/api.svelte.js'
  import DagRail from './DagRail.svelte'

  // A git-log style rail beside a recipe's rows: one marker per row, a line
  // wherever a row names another as its parent. Its only job is to make lineage
  // visible at a glance -- the rows themselves already carry every field a
  // parent link can be edited through, so a marker never gets a label of its own.
  //
  // Everything about how it looks is `DagRail`, the one drawing the workflow
  // page makes; everything about where it *sits* is here, because this is the
  // one surface whose rows are not a uniform height. A value row wraps, an array
  // row grows a count note, so `y` is measured off the real DOM by the caller
  // and handed down; only the grid crosses the wire.
  //
  // This used to keep `lane` from that response and nothing else, drawing its
  // own S-curves at its own pitch with lanes running left to right -- which is
  // the mirror of every other drawing on the page, so the trunk that most rows
  // sit in ended up furthest from the rows it names.
  let { rows = [], height = 0, kind = 'data', hovered = null } = $props()

  const cache = new Map()
  const CACHE_MAX = 64

  const shape = (rs, k) =>
    JSON.stringify({
      k,
      n: rs.map((r) => r.key),
      e: rs.flatMap((r) => r.parents.map((p) => [p, r.key])),
    })

  let laid = $state(null)
  let failed = $state(false)

  $effect(() => {
    const rs = rows
    const k = kind
    if (rs.length === 0) {
      laid = null
      failed = false
      return
    }
    const key = shape(rs, k)
    if (cache.has(key)) {
      laid = cache.get(key)
      failed = false
      return
    }
    let live = true
    ;(async () => {
      try {
        // no short circuit for the un-branched case: a lone column of markers
        // still has to sit at the gutter the engine specifies, and guessing it
        // here is exactly the second implementation this component is losing
        const geo = await api.post('/dag/layout', {
          nodes: rs.map((r) => ({ id: r.key, kind: k })),
          edges: rs.flatMap((r) => r.parents.map((p) => ({ from: p, to: r.key }))),
        })
        if (cache.size >= CACHE_MAX) cache.delete(cache.keys().next().value)
        cache.set(key, geo)
        if (!live) return
        laid = geo
        failed = false
      } catch {
        // a rail that cannot lay out says nothing rather than something wrong
        if (live) {
          laid = null
          failed = true
        }
      }
    })()
    return () => {
      live = false
    }
  })

  // the measured y of each laid-out row, by row index -- the one thing the
  // server's placement is deliberately not trusted for
  let rowY = $derived.by(() => {
    if (!laid) return null
    const yOf = new Map(rows.map((r) => [r.key, r.y]))
    const out = []
    for (const n of laid.nodes) {
      const y = yOf.get(n.id)
      if (y != null) out[n.row] = y
    }
    return out.every((v) => v != null) && out.length === laid.nodes.length ? out : null
  })
</script>

{#if laid && rowY && !failed}
  <DagRail
    geo={laid}
    {rowY}
    {height}
    {hovered}
    showLabels={false}
    ground="var(--panel)"
    minLanes={2}
  />
{/if}
