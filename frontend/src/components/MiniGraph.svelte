<script>
  import { untrack } from 'svelte'
  import { api } from '../lib/api.svelte.js'
  import DagRail from './DagRail.svelte'

  // The info panel's frame: a viewport onto the page's one DAG drawing.
  //
  // Placement -- rows, lanes, and the routed corridor -- comes from the server
  // (`POST /api/dag/layout` runs `metasmith.models.dag_layout`), and the drawing
  // itself is `DagRail`, which the plan and the recipe's lineage rails also use.
  // What is left here is the part that is only true of *this* frame: which graph
  // to ask for, the cache that keeps re-derivation off the wire, and pan/zoom.
  //
  // The graph itself is still built in the browser (`lib/graphs.js`): which
  // nodes exist, and which are plumbing, are content rules with a second
  // consumer. Only the geometry is server-side.
  let { graph = null, focus = null, empty = '', onpicktype, onpicktransform } = $props()

  let frame = $state(null)

  // Laying out costs a round trip, and the panel re-derives its graph on every
  // library toggle and every node pick. Two things keep that off the wire: the
  // same graph is only ever laid out once, and the last drawing stays up while
  // a new one is in flight -- otherwise fast clicking flashes an empty panel.
  const cache = new Map()
  const CACHE_MAX = 64

  let laid = $state(null)
  let failed = $state(false)

  const shape = (g) =>
    g?.nodes?.length
      ? JSON.stringify({
          n: g.nodes.map((n) => [n.id, n.kind, n.label]),
          e: (g.edges ?? []).map((e) => [e.from, e.to]),
        })
      : null

  $effect(() => {
    const key = shape(graph)
    if (!key) {
      laid = null
      failed = false
      return
    }
    if (cache.has(key)) {
      laid = cache.get(key)
      failed = false
      return
    }
    let live = true
    ;(async () => {
      try {
        const geo = await api.post('/dag/layout', {
          nodes: graph.nodes.map((n) => ({ id: n.id, kind: n.kind, label: n.label })),
          edges: (graph.edges ?? []).map((e) => ({ from: e.from, to: e.to })),
        })
        if (cache.size >= CACHE_MAX) cache.delete(cache.keys().next().value)
        cache.set(key, geo)
        if (!live) return
        laid = geo
        failed = false
      } catch {
        // the panel is a companion to the page, never the thing you came for:
        // a layout that will not compute says so quietly and leaves the rest
        // of the workflow page working
        if (live) failed = true
      }
    })()
    return () => {
      live = false
    }
  })

  // what the page knows about a node that the geometry does not: its kind, the
  // `per` tag, the library line, and which transform index it stands for
  let extra = $derived(new Map((graph?.nodes ?? []).map((n) => [n.id, n])))
  let nodeMeta = $derived(
    new Map(
      (graph?.nodes ?? []).map((n) => [
        n.id,
        { kind: n.kind, tag: n.tag, sub: n.sub, disabled: n.kind === 'more' },
      ]),
    ),
  )
  let edgeKind = $derived(
    new Map((graph?.edges ?? []).map((e) => [`${e.from} ${e.to}`, e.kind])),
  )
  let hovered = $state(null)

  // -- pan and zoom -------------------------------------------------------
  //
  // Same model `NewWorkflow`'s template preview uses: `scale`/`tx`/`ty` place
  // the drawing's own top-left corner in the frame's own pixels, with
  // `transform-origin: 0 0` so nothing fights the browser's default (centre)
  // origin. A graph too big to read at a glance used to just clip or force a
  // scrollbar; now it is panned and zoomed by hand instead, which is also what
  // lets a graph too *small* to read comfortably be zoomed in on.
  let scale = $state(1)
  let tx = $state(0)
  let ty = $state(0)
  let drag = $state(null)

  const MIN_SCALE = 0.2
  const MAX_SCALE = 8

  // Fit to the frame and centre. Below a floor the text stops being readable,
  // so the fit never shrinks past it -- a graph that does not fit at that size
  // is panned to, not squeezed to fit.
  function fit() {
    if (!frame || !laid?.width) return
    const w = frame.clientWidth
    const h = frame.clientHeight
    scale = Math.min(1, Math.max(0.6, (w - 16) / laid.width))
    tx = (w - laid.width * scale) / 2
    ty = Math.max(8, (h - laid.height * scale) / 2)
  }

  // Only on a *new* drawing -- refitting on every resize would wipe out a pan
  // or a zoom the moment the panel's own grip is dragged.
  $effect(() => {
    if (laid) fit()
  })

  // A graph too wide for the frame used to open with whatever is in focus off
  // the side of it. Panned to the middle of the space there is, rather than
  // scrolled -- scrollIntoView would take the page with it.
  $effect(() => {
    const target = laid?.nodes.find((n) => n.id === focus)
    if (!frame || !target) return
    // read outside tracking -- a wheel-zoom tick sets `scale`, and picking it
    // up as a dependency here would re-fire this effect on every zoom step,
    // stomping the `tx`/`ty` that `onWheel` just computed to keep the point
    // under the cursor fixed
    const s = untrack(() => scale)
    tx = frame.clientWidth / 2 - target.cx * s
    ty = frame.clientHeight / 2 - target.cy * s
  })

  function onWheel(e) {
    if (!laid) return
    e.preventDefault()
    const rect = frame.getBoundingClientRect()
    const cx = e.clientX - rect.left
    const cy = e.clientY - rect.top
    const ix = (cx - tx) / scale
    const iy = (cy - ty) / scale
    const next = Math.min(MAX_SCALE, Math.max(MIN_SCALE, scale * Math.exp(-e.deltaY * 0.0015)))
    tx = cx - ix * next
    ty = cy - iy * next
    scale = next
  }

  function onPointerDown(e) {
    if (e.button !== 0 || !laid) return
    // a node is a real button and wants its own click -- capturing the
    // pointer here would retarget its pointerup onto the frame and swallow it
    if (e.target.closest('button.node')) return
    drag = { x: e.clientX - tx, y: e.clientY - ty, id: e.pointerId }
    frame.setPointerCapture(e.pointerId)
  }

  function onPointerMove(e) {
    if (!drag || drag.id !== e.pointerId) return
    tx = e.clientX - drag.x
    ty = e.clientY - drag.y
  }

  function endDrag(e) {
    if (drag?.id === e.pointerId) drag = null
  }

  function pick(id) {
    const n = extra.get(id)
    if (!n) return
    if (n.kind === 'type') onpicktype?.(n.name)
    else if (n.kind === 'transform') onpicktransform?.(n.index)
  }
</script>

<div
  class="frame"
  class:panning={!!drag}
  role="application"
  aria-label="type graph — scroll to zoom, drag to pan"
  bind:this={frame}
  onwheel={onWheel}
  onpointerdown={onPointerDown}
  onpointermove={onPointerMove}
  onpointerup={endDrag}
  onpointercancel={endDrag}
  ondblclick={fit}
>
  {#if !laid}
    <p class="small muted hint">{failed ? 'could not lay this graph out' : empty}</p>
  {:else}
    <div class="inner" style={`transform: translate(${tx}px, ${ty}px) scale(${scale})`}>
      <DagRail
        geo={laid}
        {focus}
        {hovered}
        meta={nodeMeta}
        edgeMeta={edgeKind}
        onpick={pick}
        onhover={(id) => (hovered = id)}
      />
    </div>
  {/if}
</div>

<style>
  .frame {
    flex: 1;
    min-height: 0;
    position: relative;
    overflow: hidden;
    padding: 2px;
    touch-action: none;
    cursor: grab;
  }
  .frame.panning { cursor: grabbing; }
  .hint { margin: 4px 2px; }
  /* the drawing's own top-left corner, placed in the frame's own pixels --
     see the pan/zoom comment above `scale` in the script */
  .inner { position: absolute; top: 0; left: 0; transform-origin: 0 0; }
</style>
