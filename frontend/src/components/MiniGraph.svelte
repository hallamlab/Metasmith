<script>
  import { layout } from '../lib/dagLayout.js'

  // Edges are drawn in one SVG layer; nodes are ordinary buttons positioned over
  // it. That is deliberate -- a node here is something you click to move the
  // panel onto it, and a real button gets focus, hover, truncation and the
  // page's own styling for free, where an <svg><text> would get none of it.
  let { graph = null, focus = null, empty = '', onpicktype, onpicktransform } = $props()

  let width = $state(0)
  let frame = $state(null)

  let laid = $derived(graph?.nodes?.length ? layout(graph) : null)

  // Fit to the panel where it can. Below a floor the text stops being readable,
  // so past that it scrolls instead -- clipping a graph is never the answer.
  // The allowance is for the vertical scrollbar: fit to the full width and a
  // graph one row too tall pulls a scrollbar in, which narrows the box, which
  // would change the scale that was just fitted -- so the room is left whether
  // it is needed or not, and the fit is stable either way.
  let scale = $derived.by(() => {
    if (!laid?.width || !width) return 1
    // ...and the floor is where the labels stop being readable: past it a wide
    // graph is panned rather than shrunk into an unreadable diagram
    return Math.min(1, Math.max(0.6, (width - 16) / laid.width))
  })

  // A graph too wide for the panel opens with whatever is in focus off the side
  // of it, which reads as the wrong graph having been drawn. Scrolling the box
  // itself -- never scrollIntoView, which would take the page with it -- puts the
  // thing you clicked in the middle of the space there is.
  $effect(() => {
    const target = laid?.nodes.find((n) => n.id === focus)
    if (!frame || !target) return
    frame.scrollLeft = Math.max(0, target.cx * scale - frame.clientWidth / 2)
    frame.scrollTop = Math.max(0, target.cy * scale - frame.clientHeight / 2)
  })
</script>

<div class="frame" bind:this={frame} bind:clientWidth={width}>
  {#if !laid}
    <p class="small muted hint">{empty}</p>
  {:else}
    <div
      class="canvas"
      style="width: {Math.ceil(laid.width * scale)}px; height: {Math.ceil(laid.height * scale)}px"
    >
      <div
        class="inner"
        style="width: {laid.width}px; height: {laid.height}px; transform: scale({scale})"
      >
        <svg width={laid.width} height={laid.height} aria-hidden="true">
          <defs>
            <marker
              id="msm-arrow"
              viewBox="0 0 8 8"
              refX="7"
              refY="4"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 1 L 7 4 L 0 7 z" fill="var(--muted)" fill-opacity="0.55" />
            </marker>
            <!-- a second head in the accent, because a lineage arrow is a
                 different statement to a data-flow one and sharing a grey head
                 would leave only the line weight to tell them apart -->
            <marker
              id="msm-arrow-lin"
              viewBox="0 0 8 8"
              refX="7"
              refY="4"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M 0 1 L 7 4 L 0 7 z" fill="var(--accent)" fill-opacity="0.75" />
            </marker>
          </defs>
          {#each laid.edges as e, i (i)}
            <path
              class="edge"
              class:back={e.back}
              class:soft={e.kind === 'satisfies'}
              class:lineage={e.kind === 'lineage'}
              d={e.d}
              marker-end={e.kind === 'lineage' ? 'url(#msm-arrow-lin)' : 'url(#msm-arrow)'}
            />
          {/each}
        </svg>

        {#each laid.nodes as n (n.id)}
          <button
            class="node {n.kind}"
            class:on={n.id === focus}
            style="left: {n.x}px; top: {n.y}px; width: {n.w}px; height: {n.h}px"
            title={n.kind === 'type' ? n.name : n.label}
            disabled={n.kind === 'more'}
            onclick={() => {
              if (n.kind === 'type') onpicktype?.(n.name)
              else if (n.kind === 'transform') onpicktransform?.(n.index)
            }}
          >
            <span class="row line">
              <span class="label mono truncate">{n.label}</span>
              {#if n.tag}
                <span class="mark" title="one run per group of this input">{n.tag}</span>
              {/if}
            </span>
            {#if n.sub}<span class="sub small muted truncate">{n.sub}</span>{/if}
          </button>
        {/each}
      </div>
    </div>
  {/if}
</div>

<style>
  .frame {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 2px;
    /* centred while it fits, anchored to the top-left once it does not: `safe`
       is what keeps the start of a big graph reachable instead of centring it
       out through the scroll origin */
    display: grid;
    place-content: safe center;
  }
  .hint { margin: 4px 2px; }
  /* the scaled box takes the space the scaled content occupies, so the scrollbars
     appear for what is actually on screen rather than for the unscaled layout */
  .canvas { position: relative; margin: 0 auto; }
  .inner { position: absolute; top: 0; left: 0; transform-origin: top left; }
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
  .edge.back { stroke-dasharray: 2 4; }
  /* one input having to descend from another: not data moving, so it is drawn in
     the accent rather than in the grey every flow edge shares */
  .edge.lineage { stroke: var(--accent); stroke-opacity: 0.7; }

  .node {
    position: absolute;
    display: flex;
    flex-direction: column;
    justify-content: center;
    gap: 1px;
    padding: 2px 6px;
    text-align: left;
    background: var(--bg);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    overflow: hidden;
  }
  .node .line { gap: 4px; width: 100%; min-width: 0; }
  /* the page's line-height is set for prose; at this size it is what pushes the
     second line of a box past the bottom of it */
  .node .label { flex: 1; min-width: 0; font-size: 11.5px; line-height: 1.3; }
  .node .sub { font-size: 10.5px; line-height: 1.2; width: 100%; }
  .node.type { border-radius: 12px; }
  .node.type .label { color: var(--accent); }
  .node.transform { background: var(--panel-2); }
  .node.more { border-style: dashed; opacity: 0.7; }
  .node.on { border-color: var(--accent); box-shadow: 0 0 0 1px var(--accent); }
  .node:hover:not(:disabled) { border-color: var(--accent); }
  .mark {
    flex: 0 0 auto;
    font-size: 9.5px;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--warn);
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 0 4px;
  }
</style>
