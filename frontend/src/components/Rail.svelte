<script>
  import {
    ui,
    setRailWidth,
    clampRail,
    RAIL_DEFAULT,
    RAIL_MIN,
    RAIL_MAX,
  } from '../lib/state.svelte.js'

  // Every rail is a flat list. Relationships -- a workflow's runs, an agent's
  // runs -- live in the main pane, not in a tree here.
  let {
    title,
    items = [],
    selected = null,
    onselect,
    actions,
    row,
    empty = 'nothing here yet',
    showArchivedToggle = false,
    showArchived = false,
    ontoggleArchived,
  } = $props()

  // -- resizing ------------------------------------------------------------
  // Pointer capture on the grip, so the drag survives the cursor outrunning a
  // 7px target -- which it will, every time.

  let el
  let dragging = $state(false)

  function startDrag(e) {
    if (e.button !== 0) return
    dragging = true
    e.currentTarget.setPointerCapture(e.pointerId)
    e.preventDefault()
  }

  function onDrag(e) {
    if (!dragging) return
    setRailWidth(e.clientX - el.getBoundingClientRect().left)
  }

  function endDrag(e) {
    if (!dragging) return
    dragging = false
    e.currentTarget.releasePointerCapture?.(e.pointerId)
  }

  function onGripKey(e) {
    const step = e.shiftKey ? 40 : 10
    if (e.key === 'ArrowLeft') setRailWidth(ui.railWidth - step)
    else if (e.key === 'ArrowRight') setRailWidth(ui.railWidth + step)
    else if (e.key === 'Home') setRailWidth(RAIL_DEFAULT)
    else return
    e.preventDefault()
  }

  // While dragging the pointer is over the main pane as often as not, so the
  // cursor and the no-select have to be global rather than on the grip.
  $effect(() => {
    document.body.classList.toggle('resizing', dragging)
    return () => document.body.classList.remove('resizing')
  })
</script>

<div class="rail" class:dragging bind:this={el} style="--rail-w: {ui.railWidth}px">
  <div class="head">
    <div class="spread">
      <h3>{title}</h3>
      <div class="row">{@render actions?.()}</div>
    </div>
    {#if showArchivedToggle}
      <label class="small muted archived">
        <input
          type="checkbox"
          checked={showArchived}
          onchange={(e) => ontoggleArchived?.(e.currentTarget.checked)}
        />
        show archived
      </label>
    {/if}
  </div>

  <div class="list">
    {#if items.length === 0}
      <p class="small muted pad">{empty}</p>
    {/if}
    {#each items as item (item.id)}
      {#if item.kind === 'heading'}
        <!-- A label over a run of rows. Still one flat list: the rows below it
             are siblings of the rows above, not children of anything. -->
        <div class="heading small muted">{item.label}</div>
      {:else}
        <div
          class="item"
          class:sel={item.id === selected}
          class:dim={item.dim}
          role="button"
          tabindex="0"
          onclick={() => onselect?.(item.id)}
          onkeydown={(e) => (e.key === 'Enter' || e.key === ' ') && onselect?.(item.id)}
        >
          {@render row(item)}
        </div>
      {/if}
    {/each}
  </div>

  <!-- A focusable `separator` carrying a value *is* the ARIA window-splitter
       pattern; the rule below only knows that separators are usually static. -->
  <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
  <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
  <div
    class="grip"
    role="separator"
    aria-orientation="vertical"
    aria-label="resize the list"
    aria-valuenow={ui.railWidth}
    aria-valuemin={RAIL_MIN}
    aria-valuemax={RAIL_MAX}
    tabindex="0"
    title="drag to resize"
    onpointerdown={startDrag}
    onpointermove={onDrag}
    onpointerup={endDrag}
    onpointercancel={endDrag}
    ondblclick={() => setRailWidth(RAIL_DEFAULT)}
    onkeydown={onGripKey}
  ></div>
</div>

<style>
  .rail {
    width: var(--rail-w);
    flex: 0 0 var(--rail-w);
    border-right: 1px solid var(--line);
    background: var(--panel);
    display: flex;
    flex-direction: column;
    min-height: 0;
    position: relative;
  }
  .grip {
    position: absolute;
    top: 0;
    bottom: 0;
    /* straddles the border, so the target is wider than the line it moves */
    right: -3px;
    width: 7px;
    cursor: col-resize;
    z-index: 2;
  }
  .grip::after {
    content: '';
    position: absolute;
    inset: 0 3px;
    background: transparent;
    transition: background 120ms;
  }
  .grip:hover::after,
  .grip:focus-visible::after,
  .rail.dragging .grip::after { background: var(--accent); }
  .grip:focus-visible { outline: none; }
  .head {
    padding: 10px 12px;
    border-bottom: 1px solid var(--line);
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .archived { display: flex; align-items: center; gap: 6px; cursor: pointer; }
  .archived input { width: auto; }
  .list { overflow-y: auto; flex: 1; min-height: 0; }
  .pad { padding: 12px; }
  .heading {
    padding: 10px 12px 4px;
    border-bottom: 1px solid var(--line);
    background: var(--bg);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 11px;
    position: sticky;
    top: 0;
    z-index: 1;
  }
  .item {
    padding: 8px 12px;
    border-bottom: 1px solid var(--line);
    cursor: pointer;
  }
  .item:hover { background: var(--panel-2); }
  .item.sel { background: var(--panel-2); box-shadow: inset 2px 0 0 var(--accent); }
  .item.dim { opacity: 0.5; }
</style>
