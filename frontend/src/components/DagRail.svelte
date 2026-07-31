<script>
  import { bakeEdges, gutter, makeGrid } from '../lib/dagpaths.js'
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
  // Placement arrives already decided (`geo`, from `POST /api/dag/layout` or
  // stored beside a solved plan). Ink arrives from the server too (`lib/dagink`).
  // What this component adds is the part an SVG file cannot have: a node is a
  // button, so it gets focus, hover and a click that moves the panel.
  //
  // It owns no viewport. No overflow, no transform, no fit -- the three frames
  // want three different things (the plan scrolls with the page, the panel pans
  // and zooms, a rail does neither) and a component that guessed would be wrong
  // for two of them. Its output is a plain block of a known width and height.
  let {
    geo,
    // measured y per row index, for a surface whose rows are not a uniform
    // height. Null means the engine's own nominal pitch.
    rowY = null,
    // reserve a wider gutter than the current lane count needs, so that adding
    // or removing the first branch does not shift every row sideways
    minLanes = 1,
    // a rail beside rows that already name themselves draws markers only
    showLabels = true,
    width = null,
    height = null,
    focus = null,
    // hover driven from outside (a recipe row pointed at from its own list);
    // the component also sets its own on the nodes it draws
    hovered = null,
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
  let pad = $derived(gutter(geo, minLanes))
  let grid = $derived(makeGrid(geo, { rowY, xOffset: pad.shift }))
  let edges = $derived(bakeEdges(geo, grid))

  let boxW = $derived(width ?? (showLabels ? geo.width + pad.shift : pad.width))
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

  const at = (n) => (rowY ? (rowY[n.row] ?? n.cy) : n.cy)

  const isOn = (id) => id === focus || id === hovered

  function enter(id) {
    onhover?.(id)
  }
  function leave(id) {
    if (hovered === id) onhover?.(null)
  }
</script>

<div class="rail" bind:this={inner} style="width: {boxW}px; height: {boxH}px;">
  <svg width={boxW} height={boxH} aria-hidden="true">
    <g fill="none" stroke={ink.plate.edge} stroke-linejoin="round" stroke-linecap="round">
      {#each edges as e, i (i)}
        {@const kind = edgeMeta?.get(`${e.from} ${e.to}`)}
        <path
          class="edge"
          class:soft={kind === 'satisfies'}
          class:lineage={kind === 'lineage'}
          class:lit={isOn(e.from) || isOn(e.to)}
          d={e.d}
          stroke={e.hue || null}
        />
      {/each}
    </g>
    {#each geo.nodes as n (n.id)}
      {@const m = marker(n)}
      {@const cx = n.cx + pad.shift}
      {@const cy = at(n)}
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
      {@const left = n.cx + pad.shift - n.marker_w / 2}
      {@const cy = at(n)}
      <button
        class="node {x?.kind ?? 'data'}"
        class:on={n.id === focus}
        class:lit={n.id === hovered}
        style="
          left: {left}px;
          top: {cy - geo.row_pitch / 2}px;
          width: {Math.max(0, boxW - left)}px;
          height: {geo.row_pitch}px;
          --gap: {n.label_x + pad.shift - left}px;
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
  .edge.lit { stroke-opacity: 1; stroke-width: 1.8; }

  /* what a hover marks: the node itself, not a box drawn around its whole row.
     The rectangle this replaces was the one part of the panel that had no
     counterpart in the rendered drawing. */
  .mk { transition: opacity 80ms linear; }
  .mk.lit { filter: drop-shadow(0 0 3px var(--accent)); }

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
    border-radius: var(--radius);
    overflow: hidden;
  }
  .node:disabled { cursor: default; }
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
  .ns {
    font-size: calc(var(--fs) / 2);
    color: var(--dim);
  }
  .line { display: flex; align-items: baseline; gap: 4px; min-width: 0; }
  .label { font-size: var(--fs); color: var(--ink); }
  .node.type .label { color: var(--accent); }
  .sub { font-size: calc(var(--fs) * 0.72); }
  .node.more { opacity: 0.7; }
  .node.on,
  .node.lit,
  .node:hover:not(:disabled) {
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
