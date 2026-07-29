<script>
  import { untrack } from 'svelte'
  import { api } from '../lib/api.svelte.js'

  // The same engine that draws the plan, drawn again with clickable parts.
  //
  // Placement -- rows, lanes, and the routed edge paths -- comes from the
  // server: `POST /api/dag/layout` runs `metasmith.models.dag_layout` and hands
  // back pixels. There was a second layout engine in the browser, and two
  // engines meant the plan diagram and this panel disagreed about the shape of
  // the same graph. What stays here is everything the SVG cannot do: a node is
  // an ordinary button, so it gets focus, hover, truncation and the page's own
  // styling, and clicking one moves the panel onto it.
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
  let edgeKind = $derived(
    new Map((graph?.edges ?? []).map((e) => [`${e.from} ${e.to}`, e.kind])),
  )

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

  const kindOf = (id) => extra.get(id)?.kind ?? 'type'

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
      <svg width={laid.width} height={laid.height} aria-hidden="true">
        {#each laid.edges as e, i (i)}
          {@const kind = edgeKind.get(`${e.from} ${e.to}`)}
          <path
            class="edge"
            class:soft={kind === 'satisfies'}
            class:lineage={kind === 'lineage'}
            d={e.d}
          />
        {/each}
      </svg>

      {#each laid.nodes as n (n.id)}
        {@const meta = extra.get(n.id)}
        {@const kind = kindOf(n.id)}
        {@const left = n.cx - n.marker_w / 2}
        <button
          class="node {kind}"
          class:on={n.id === focus}
          style="
            left: {left}px;
            top: {n.cy - laid.row_pitch / 2}px;
            width: {Math.max(0, laid.width - left)}px;
            height: {laid.row_pitch}px;
            --marker-w: {n.marker_w}px;
            --marker-h: {n.marker_h}px;
            --gap: {n.label_x - left - n.marker_w}px;
            --fs: {laid.font_size}px;
          "
          title={n.full}
          disabled={kind === 'more'}
          onclick={() => pick(n.id)}
        >
          <span class="marker"></span>
          <span class="text">
            {#if n.namespace}<span class="ns truncate">{n.namespace}</span>{/if}
            <span class="line">
              <span class="label mono truncate">{n.label}</span>
              {#if meta?.tag}
                <span class="mark" title="one run per group of this input">{meta.tag}</span>
              {/if}
              {#if meta?.sub}<span class="sub small muted truncate">{meta.sub}</span>{/if}
            </span>
          </span>
        </button>
      {/each}
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
  svg { position: absolute; top: 0; left: 0; overflow: visible; }
  .edge {
    fill: none;
    /* the line colour is right for a border and too quiet for a line that has to
       be followed across a graph */
    stroke: var(--muted);
    stroke-opacity: 0.55;
    stroke-width: 1.3;
  }
  /* a type standing in for a more general one: the same connection, made by the
     type system rather than by a name lining up */
  .edge.soft { stroke-dasharray: 3 3; }
  /* one input having to descend from another: not data moving, so it is drawn in
     the accent rather than in the grey every flow edge shares */
  .edge.lineage { stroke: var(--accent); stroke-opacity: 0.7; }

  /* A node row runs from its marker to the right edge of the drawing, so the
     whole line is clickable. It is transparent by default because the rails of
     the lanes to its right pass underneath it. */
  .node {
    position: absolute;
    display: flex;
    align-items: center;
    gap: 0;
    padding: 0;
    text-align: left;
    background: none;
    border: 1px solid transparent;
    border-radius: var(--radius);
    overflow: hidden;
  }
  .marker {
    flex: 0 0 auto;
    width: var(--marker-w);
    height: var(--marker-h);
    background: var(--bg);
    border: 1.5px solid var(--muted);
    border-radius: 2px;
  }
  /* the same three shapes the SVG backend draws: a rounded marker for data, a
     filled one for a step */
  .node.type .marker { border-radius: 50%; border-color: var(--accent); }
  .node.transform .marker { background: var(--muted); border-color: var(--muted); }
  .node.more .marker { border-style: dashed; }
  .text {
    display: flex;
    flex-direction: column;
    justify-content: center;
    min-width: 0;
    margin-left: var(--gap);
    /* the page's line-height is set for prose; at this size it is what pushes
       the second line past the bottom of the row */
    line-height: 1.15;
  }
  .ns { font-size: calc(var(--fs) / 2); color: var(--muted); }
  .line { display: flex; align-items: baseline; gap: 4px; min-width: 0; }
  .label { font-size: var(--fs); }
  .node.type .label { color: var(--accent); }
  .sub { font-size: calc(var(--fs) * 0.72); }
  .node.more { opacity: 0.7; }
  .node.on { border-color: var(--accent); background: var(--panel-2); }
  .node:hover:not(:disabled) { border-color: var(--accent); }
  .mark {
    flex: 0 0 auto;
    font-size: calc(var(--fs) * 0.72);
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--warn);
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 0 4px;
  }
</style>
