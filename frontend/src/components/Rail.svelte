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
      <!-- a chip, not a checkbox: it is a filter on the list below it, the same
           kind of thing as the group headings, and a lone tickbox in a header
           read as a setting rather than as a view -->
      <div class="row">
        <button
          class="chip small"
          class:on={showArchived}
          aria-pressed={showArchived}
          title={showArchived
            ? 'hide the ones that were deleted'
            : 'show the ones that were deleted — deleting archives, so they are still here'}
          onclick={() => ontoggleArchived?.(!showArchived)}
        >archived</button>
      </div>
    {/if}
  </div>

  <div class="list">
    {#if items.length === 0}
      <p class="small muted pad">{empty}</p>
    {/if}
    {#each items as item (item.id)}
      {#if item.kind === 'heading'}
        <!-- A label over a run of rows. The list stays flat: a heading that can
             be shut does it by dropping its rows from `items`, so nothing here
             is nested inside anything else. -->
        {#if item.ontoggle}
          <button class="heading toggle small muted" onclick={() => item.ontoggle()}>
            <span class="caret" class:open={!item.collapsed}>▸</span>
            <span class="truncate grow">{item.label}</span>
            <span class="count">{item.count}</span>
          </button>
        {:else}
          <div class="heading small muted">{item.label}</div>
        {/if}
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
  /* off, it is as quiet as the muted text it sits under; on, it says which
     view you are in -- the same on/off shape as the sample-index star */
  .chip {
    padding: 1px 8px;
    font-size: 11px;
    background: none;
    color: var(--muted);
    border-color: var(--line);
    border-radius: 999px;
  }
  .chip:hover { color: var(--text); }
  .chip.on { color: var(--accent); border-color: var(--accent); }
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
  .heading.toggle {
    display: flex;
    align-items: center;
    gap: 6px;
    width: 100%;
    text-align: left;
    border-radius: 0;
    cursor: pointer;
    /* the same box as the static heading, less the button chrome */
    padding: 8px 12px 6px;
    color: var(--muted);
  }
  .heading.toggle:hover { background: var(--panel-2); }
  .caret { font-size: 9px; transition: transform 0.1s; }
  .caret.open { transform: rotate(90deg); }
  .count {
    font-variant-numeric: tabular-nums;
    text-transform: none;
    letter-spacing: 0;
    opacity: 0.7;
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
