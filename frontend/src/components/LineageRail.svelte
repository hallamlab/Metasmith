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
  // and handed down.
  //
  // Those measurements go *up* with the layout request rather than being
  // applied to the answer. Two things were wrong with applying them: the engine
  // is free to reorder rows, so a rail laid out in one order and drawn in
  // another crosses the markers it was told to avoid; and re-baking the curves
  // against the new positions meant a port of the bake living in this codebase
  // with nothing to hold it in step with the original. The rows go with the
  // question now, and the answer is the drawing.
  let { rows = [], height = 0, kind = 'data', lit = null } = $props()

  const cache = new Map()
  const CACHE_MAX = 64

  // the measured positions are part of the shape: the same rows at different
  // heights are a different drawing. Rounded, so that a sub-pixel reflow -- a
  // font settling, a scrollbar appearing -- is a cache hit rather than a
  // request.
  const shape = (rs, k) =>
    JSON.stringify({
      k,
      n: rs.map((r) => [r.key, Math.round(r.y ?? -1)]),
      e: rs.flatMap((r) => r.parents.map((p) => [p, r.key])),
    })

  let laid = $state(null)
  let failed = $state(false)

  $effect(() => {
    const rs = rows
    const k = kind
    // every row has to have been measured, or the rail would be drawn partly at
    // the engine's nominal pitch and partly where the rows actually are
    if (rs.length === 0 || rs.some((r) => r.y == null)) {
      if (rs.length === 0) laid = null
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
          // the rows are the recipe's, in the recipe's order, at the recipe's
          // heights -- not something for the engine to decide
          order: rs.map((r) => r.key),
          row_y: Object.fromEntries(rs.map((r) => [r.key, r.y])),
          // two lanes' worth of gutter always, so the rail does not slide
          // sideways the first time a row is given a parent
          min_lanes: 2,
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
</script>

{#if laid && !failed}
  <DagRail geo={laid} {height} {lit} showLabels={false} ground="var(--panel)" />
{/if}
