<script>
  import { dagInk } from '../lib/dagink.svelte.js'
  import { ui } from '../lib/state.svelte.js'

  // The one drawing the workflow page makes, in every frame that needs it.
  //
  // There were three: the plan's server-rendered SVG, the info panel's own
  // buttons-over-paths, and the recipe's lineage rails. They shared an engine
  // for placement and nothing else -- different markers, different curves, and
  // in the rails' case lanes running the wrong way -- so the same graph looked
  // like three different products depending on where you met it.
  //
  // Placement arrives already decided, and so do the curves: `geo` is what
  // `POST /api/dag/layout` (or a stored plan) baked, down to each edge's `d`.
  // A frame whose rows are its own -- the recipe's, one per form row -- sends
  // those rows' measured heights with the request rather than re-baking the
  // paths here, which is the second implementation this component used to
  // carry. Ink arrives from the server too (`lib/dagink`). What this adds is
  // the part an SVG file cannot have: a node is a button, so it gets focus,
  // hover and a click that moves the panel.
  //
  // It owns no viewport. No overflow, no transform, no fit -- the three frames
  // want three different things (the plan scrolls with the page, the panel pans
  // and zooms, a rail does neither) and a component that guessed would be wrong
  // for two of them. Its output is a plain block of a known width and height.
  let {
    geo,
    // a rail beside rows that already name themselves draws markers only
    showLabels = true,
    width = null,
    height = null,
    focus = null,
    // what a *pointer somewhere else* is marking: `{nodes, edges}`, both sets,
    // edges keyed `${from} ${to}`. A pair rather than a single id because the
    // thing being pointed at is usually a link -- a parent chip means "this row
    // and that one, joined by this line" and nothing else on the drawing.
    lit = null,
    // per-node extras the geometry knows nothing about: {kind, tag, sub,
    // disabled}. `kind` is a page word ('type', 'transform', 'more') used for
    // colour; the marker shape comes from the geometry's own kind.
    meta = null,
    // per-edge page word, keyed `${from} ${to}`: 'satisfies' | 'lineage'
    edgeMeta = null,
    // what a hollow marker is filled with. The served theme carries the plate's
    // background hex, because a file on disk cannot know what it will sit on;
    // here the frame does know, and hollow only reads hollow where the fill and
    // the ground agree.
    ground = 'var(--bg)',
    onpick = null,
    onhover = null,
  } = $props()

  let ink = $derived(dagInk(ui.theme))

  // where the label column starts, for a frame drawing markers only
  let gutter = $derived(geo.nodes?.[0]?.label_x ?? geo.width)
  let boxW = $derived(width ?? (showLabels ? geo.width : gutter))
  let boxH = $derived(height ?? geo.height)

  const styleOf = (kind) => ink.styles[kind] ?? ink.styles.data

  // where a colour scheme's hue lands: on the fill of a solid marker, on the
  // outline of a hollow one -- `dag_draw.tint`, so a coloured target still
  // reads solid rather than outlined
  function marker(n) {
    const st = styleOf(n.kind)
    const hue = n.hue
    return {
      st,
      fill: st.solid ? (hue ?? st.fill) : ground,
      stroke: hue && !st.solid ? hue : st.stroke,
    }
  }

  // the pointer inside this drawing, which is a different question from `lit`
  let self = $state(null)

  const isOn = (id) => id === focus || id === self || !!lit?.nodes?.has(id)
  const edgeOn = (e) => !!lit?.edges?.has(`${e.from} ${e.to}`)

  function enter(id) {
    self = id
    onhover?.(id)
  }
  function leave(id) {
    if (self !== id) return
    self = null
    onhover?.(null)
  }
</script>

<div class="rail" style="width: {boxW}px; height: {boxH}px;">
  <svg width={boxW} height={boxH} aria-hidden="true">
    <g fill="none" stroke={ink.plate.edge} stroke-linejoin="round" stroke-linecap="round">
      {#each geo.edges as e, i (i)}
        {#if !e.back}
          {@const kind = edgeMeta?.get(`${e.from} ${e.to}`)}
          <path
            class="edge"
            class:soft={kind === 'satisfies'}
            class:lineage={kind === 'lineage'}
            class:lit={edgeOn(e)}
            class:near={!edgeOn(e) && (isOn(e.from) || isOn(e.to))}
            d={e.d}
            stroke={e.hue || null}
          />
        {/if}
      {/each}
    </g>
    {#each geo.nodes as n (n.id)}
      {@const m = marker(n)}
      {@const cx = n.cx}
      {@const cy = n.cy}
      {@const w = n.marker_w}
      {@const h = n.marker_h}
      <g class="mk" class:lit={isOn(n.id)}>
        {#if m.st.shape === 'triangle_down'}
          <polygon
            points="{cx - w / 2},{cy - h / 2} {cx + w / 2},{cy - h / 2} {cx},{cy + h / 2}"
            fill={m.fill}
            stroke={m.stroke}
            stroke-width={m.st.stroke_width}
            stroke-linejoin="round"
          />
        {:else if m.st.shape === 'square'}
          <rect
            x={cx - w / 2}
            y={cy - h / 2}
            width={w}
            height={h}
            rx={m.st.rx}
            fill={m.fill}
            stroke={m.stroke}
            stroke-width={m.st.stroke_width}
          />
        {:else}
          <circle
            {cx}
            {cy}
            r={w / 2}
            fill={m.fill}
            stroke={m.stroke}
            stroke-width={m.st.stroke_width}
          />
        {/if}
      </g>
    {/each}
  </svg>

  {#if showLabels}
    {#each geo.nodes as n (n.id)}
      {@const x = meta?.get(n.id)}
      {@const left = n.cx - n.marker_w / 2}
      <button
        class="node {x?.kind ?? 'data'}"
        class:on={n.id === focus}
        class:lit={isOn(n.id)}
        style="
          left: {left}px;
          top: {n.cy - geo.row_pitch / 2}px;
          width: {Math.max(0, boxW - left)}px;
          height: {geo.row_pitch}px;
          --gap: {n.label_x - left}px;
          --fs: {geo.font_size}px;
          --ink: {styleOf(n.kind).text};
          --dim: {styleOf(n.kind).muted};
        "
        title={n.full}
        disabled={!onpick || !!x?.disabled}
        onclick={() => onpick?.(n.id)}
        onpointerenter={() => enter(n.id)}
        onpointerleave={() => leave(n.id)}
        onfocus={() => enter(n.id)}
        onblur={() => leave(n.id)}
      >
        <span class="text">
          <!-- the half-size line, always stacked above the name: a type's
               prefix, a step's library. It used to be drawn to the right of the
               name here and left out of the plan's SVG entirely, which is three
               answers to one question. -->
          {#if n.namespace}<span class="ns truncate">{n.namespace}</span>{/if}
          <span class="line">
            <span class="label mono truncate">{n.label}</span>
            {#if x?.tag}
              <span class="mark" title="one run per group of this input">{x.tag}</span>
            {/if}
            {#if x?.sub}<span class="sub small muted truncate">{x.sub}</span>{/if}
          </span>
        </span>
      </button>
    {/each}
  {/if}
</div>

<style>
  .rail {
    position: relative;
    flex: 0 0 auto;
  }
  svg {
    position: absolute;
    top: 0;
    left: 0;
    overflow: visible;
  }
  .edge {
    stroke-width: 1.4;
    stroke-opacity: 0.55;
  }
  /* a type standing in for a more general one: the same connection, made by the
     type system rather than by a name lining up */
  .edge.soft { stroke-dasharray: 3 3; }
  /* one input having to descend from another: not data moving, so it is drawn in
     the accent rather than the grey every flow edge shares */
  .edge.lineage { stroke: var(--accent); stroke-opacity: 0.7; }
  /* the link being pointed at, as opposed to merely touching a lit node. It has
     to be findable at a glance in a column of near-identical grey curves, so it
     takes the accent and twice the weight rather than a shade more opacity. */
  .edge.lit {
    stroke: var(--accent);
    stroke-opacity: 1;
    stroke-width: 3;
  }
  .edge.near { stroke-opacity: 0.9; stroke-width: 1.8; }

  /* what a hover marks: the node itself, not a box drawn around its whole row.
     The rectangle this replaces was the one part of the panel that had no
     counterpart in the rendered drawing. */
  .mk { transition: opacity 80ms linear; }
  .mk.lit { filter: drop-shadow(0 0 4px var(--accent)) drop-shadow(0 0 2px var(--accent)); }

  /* A node row runs from its marker to the right edge, so the whole line is
     clickable. Transparent by default because the rails of the lanes to its
     right pass underneath it. */
  .node {
    position: absolute;
    display: flex;
    align-items: center;
    gap: 0;
    padding: 0;
    text-align: left;
    background: none;
    border: 0;
    overflow: hidden;
  }
  .node:disabled { cursor: default; }
  /* the highlight is on the text, not on the button. The button starts at its
     own marker's left edge, so a background there covers the marker it is
     meant to be marking -- and every rail passing under that row with it. */
  .text {
    display: flex;
    flex-direction: column;
    justify-content: center;
    min-width: 0;
    margin-left: var(--gap);
    padding: 0 4px;
    align-self: stretch;
    border-radius: var(--radius);
    /* the page's line-height is set for prose; at this size it is what pushes
       the second line past the bottom of the row */
    line-height: 1.15;
  }
  .ns {
    font-size: calc(var(--fs) / 2);
    color: var(--dim);
  }
  .line { display: flex; align-items: baseline; gap: 4px; min-width: 0; }
  .label { font-size: var(--fs); color: var(--ink); }
  .node.type .label { color: var(--accent); }
  .sub { font-size: calc(var(--fs) * 0.72); }
  .node.more { opacity: 0.7; }
  .node.on .text,
  .node.lit .text,
  .node:hover:not(:disabled) .text {
    background: var(--panel-2);
  }
  .node.on .label { text-decoration: underline; text-underline-offset: 2px; }
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
