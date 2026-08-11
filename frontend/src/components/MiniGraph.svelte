<script>
  import { untrack } from 'svelte'
  import { api } from '../lib/api.svelte.js'
  import { around, neighbours } from '../lib/highlight.js'
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
  // library line, and which transform index it stands for
  let extra = $derived(new Map((graph?.nodes ?? []).map((n) => [n.id, n])))
  let nodeMeta = $derived(
    new Map(
      (graph?.nodes ?? []).map((n) => [
        n.id,
        { kind: n.kind, sub: n.sub, disabled: n.kind === 'more' },
      ]),
    ),
  )
  let edgeKind = $derived(
    new Map((graph?.edges ?? []).map((e) => [`${e.from} ${e.to}`, e.kind])),
  )

  // -- what is lit ---------------------------------------------------------
  //
  // The pointer is held here rather than inside the drawing, and what it means
  // is worked out against the graph: a node and the one hop around it, in both
  // directions, because the panel is for reading a type's place in the graph
  // and both halves of that are the answer.
  //
  // `$derived` and not an `$effect`: the pointer comes up from `DagRail` and
  // the marks go back down, and an effect in that loop re-runs on what it just
  // wrote.
  //
  // `focus` is deliberately not part of this. What the panel is showing is said
  // by its heading and pointed at by the pan below; marking it here as well left
  // one node lit for as long as the panel was open, in a frame where nearly
  // everything drawn is that node's own neighbourhood.
  let pointed = $state(null)
  let hlMarks = $derived(around(graph, { pointed, relation: neighbours }))

  // -- pan and zoom -------------------------------------------------------
  //
  // Same model `NewWorkflow`'s template preview uses: `scale`/`tx`/`ty` place
  // the drawing's own top-left corner in the frame's own pixels, with
  // `transform-origin: 0 0` so nothing fights the browser's default (centre)
  // origin. A graph too big to read at a glance used to just clip or force a
  // scrollbar; now it is panned and zoomed by hand instead, which is also what
  // lets a graph too *small* to read comfortably be zoomed in on.
  //
  // Behind a toggle, and off by default. The panel this sits in scrolls as one
  // column, and a frame that swallowed the wheel to zoom would be a hole in the
  // middle of that column you could not scroll past; a frame that panned on
  // drag would also be selecting the text under the pointer the whole way.
  // With it on both are this frame's, and the page's own gestures stop here.
  let live = $state(false)
  let scale = $state(1)
  let tx = $state(0)
  let ty = $state(0)
  let drag = $state(null)

  const MIN_SCALE = 0.2
  const MAX_SCALE = 8

  // Fit to the frame and centre. Below a floor the text stops being readable,
  // so the fit never shrinks past it -- a graph that does not fit at that size
  // is panned to, not squeezed to fit.
  //
  // The fit is computed into a local and assigned once. Reading `scale` back
  // after setting it, which is the obvious way to write the two lines below,
  // put `scale` in the *dependencies* of the effect that calls this -- so
  // every wheel tick re-ran the fit and put the drawing straight back where it
  // was. That is the whole of "zoom does nothing but snap to a position".
  function fit() {
    if (!frame || !laid?.width) return
    const w = frame.clientWidth
    const h = frame.clientHeight
    const s = Math.min(1, Math.max(0.6, (w - 16) / laid.width))
    tx = (w - laid.width * s) / 2
    ty = Math.max(8, (h - laid.height * s) / 2)
    scale = s
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

  // Attached by hand rather than as `onwheel={…}`. Svelte 5 registers a
  // declarative wheel handler as a *passive* listener, so `preventDefault()` in
  // it is a no-op and the browser scrolls anyway -- which is why zoom looked
  // like it did nothing but snap to whatever the focus effect had just centred.
  $effect(() => {
    if (!frame) return
    const el = frame
    el.addEventListener('wheel', onWheel, { passive: false })
    return () => el.removeEventListener('wheel', onWheel)
  })

  function onWheel(e) {
    if (!laid || !live) return
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
    if (e.button !== 0 || !laid || !live) return
    // no exception for a node here: with the gestures on, the rows are not
    // buttons (`interactive={!live}` below), so a drag that begins over one is
    // a drag like any other rather than a click waiting to be swallowed
    //
    // a drag over text is a selection unless something says otherwise, and a
    // pan that highlighted half the panel on the way past is not a pan
    e.preventDefault()
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

  // the mode and nothing else. It used to refit on every change, in both
  // directions, so zooming in to read something and then reaching for the node
  // you zoomed in to click cost you the view you set up to click it from.
  // Double-click, in pan mode, is the way back to the fit.
  function setLive(v) {
    live = v
  }
</script>

<div
  class="frame"
  class:panning={!!drag}
  class:live
  role="application"
  aria-label={live ? 'type graph — scroll to zoom, drag to pan' : 'type graph'}
  bind:this={frame}
  onpointerdown={onPointerDown}
  onpointermove={onPointerMove}
  onpointerup={endDrag}
  onpointercancel={endDrag}
  ondblclick={() => live && fit()}
>
  {#if !laid}
    <p class="small muted hint">{failed ? 'could not lay this graph out' : empty}</p>
  {:else}
    <!-- Two labelled halves, one of them lit: the same shape as the recipe's
         file/value switch, so which mode the frame is in reads at a glance
         rather than from a single button's own changing label.

         `pointerdown` is stopped here, not left to reach `onPointerDown`:
         while live, that handler starts a drag and captures the pointer on
         `frame` itself the moment anything inside it goes down, which steals
         the matching pointerup a click needs -- so the "interact" button
         could not be clicked to turn panning back off. It never got as far
         as the click handler; the drag took the gesture first. -->
    <div
      class="grip"
      role="group"
      aria-label="interact with nodes, or pan and zoom the drawing"
      onpointerdown={(e) => e.stopPropagation()}
    >
      <button
        type="button"
        class:on={!live}
        title="take hold of nodes: click to pick, hover to see connections"
        onclick={() => setLive(false)}
      >interact</button>
      <button
        type="button"
        class:on={live}
        title="pan and zoom this drawing — scroll zooms, drag pans, double-click fits"
        onclick={() => setLive(true)}
      >pan + zoom</button>
    </div>
    <div class="inner" style={`transform: translate(${tx}px, ${ty}px) scale(${scale})`}>
      <DagRail
        geo={laid}
        marks={hlMarks}
        meta={nodeMeta}
        edgeMeta={edgeKind}
        interactive={!live}
        onpick={pick}
        onhover={(id) => (pointed = id)}
      />
    </div>
  {/if}
</div>

<style>
  .frame {
    flex: 1 0 auto;
    /* a fixed height inside a scrolling column: the panel is one scroller now,
       so this frame cannot be "whatever is left" -- and `fit()` reads
       `clientHeight`, so it has to be a number before the first fit */
    height: 340px;
    position: relative;
    overflow: hidden;
    padding: 2px;
    margin: 8px 0;
  }
  /* only while it is holding the gestures: off, the wheel belongs to the panel
     and a drag belongs to the selection */
  .frame.live { touch-action: none; cursor: grab; user-select: none; }
  .frame.live.panning { cursor: grabbing; }
  .grip {
    position: absolute;
    z-index: 5;
    top: 4px;
    right: 4px;
    display: inline-flex;
    border: 1px solid var(--line);
    border-radius: 999px;
    overflow: hidden;
    background: var(--panel);
    opacity: 0.75;
  }
  .grip:hover { opacity: 1; }
  .grip button {
    border: none;
    background: none;
    color: var(--muted);
    padding: 1px 8px;
    font-size: 11px;
  }
  .grip button.on { background: var(--accent); color: var(--panel); }
  .hint { margin: 4px 2px; }
  /* the drawing's own top-left corner, placed in the frame's own pixels --
     see the pan/zoom comment above `scale` in the script */
  .inner { position: absolute; top: 0; left: 0; transform-origin: 0 0; }
</style>
