<script>
  import {
    ui,
    setPanelOpen,
    setPanelWidth,
    PANEL_DEFAULT,
    PANEL_MIN,
    PANEL_MAX,
  } from '../lib/state.svelte.js'

  // The right-hand mirror of the rail: same grip, same pointer capture, same
  // remembered width. Collapsed it leaves a strip rather than vanishing, so
  // there is always something to click to get it back.
  let { title, subtitle = null, children } = $props()

  // reassigned when the panel is folded and unfolded, so it has to be reactive
  let el = $state(null)
  let dragging = $state(false)

  function startDrag(e) {
    if (e.button !== 0) return
    dragging = true
    e.currentTarget.setPointerCapture(e.pointerId)
    e.preventDefault()
  }

  // measured from the right edge, since that is the one that stays put
  function onDrag(e) {
    if (!dragging) return
    setPanelWidth(el.getBoundingClientRect().right - e.clientX)
  }

  function endDrag(e) {
    if (!dragging) return
    dragging = false
    e.currentTarget.releasePointerCapture?.(e.pointerId)
  }

  function onGripKey(e) {
    const step = e.shiftKey ? 40 : 10
    if (e.key === 'ArrowLeft') setPanelWidth(ui.panelWidth + step)
    else if (e.key === 'ArrowRight') setPanelWidth(ui.panelWidth - step)
    else if (e.key === 'Home') setPanelWidth(PANEL_DEFAULT)
    else return
    e.preventDefault()
  }

  $effect(() => {
    document.body.classList.toggle('resizing', dragging)
    return () => document.body.classList.remove('resizing')
  })
</script>

{#if ui.panelOpen}
  <aside class="panel" class:dragging bind:this={el} style="--panel-w: {ui.panelWidth}px">
    <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
    <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
    <div
      class="grip"
      role="separator"
      aria-orientation="vertical"
      aria-label="resize the panel"
      aria-valuenow={ui.panelWidth}
      aria-valuemin={PANEL_MIN}
      aria-valuemax={PANEL_MAX}
      tabindex="0"
      title="drag to resize"
      onpointerdown={startDrag}
      onpointermove={onDrag}
      onpointerup={endDrag}
      onpointercancel={endDrag}
      ondblclick={() => setPanelWidth(PANEL_DEFAULT)}
      onkeydown={onGripKey}
    ></div>

    <div class="head">
      <div class="spread">
        <h3 class="truncate">{title}</h3>
        <button class="fold" onclick={() => setPanelOpen(false)} title="collapse">▸</button>
      </div>
      {#if subtitle}<div class="small muted truncate">{subtitle}</div>{/if}
    </div>

    <div class="body">{@render children()}</div>
  </aside>
{:else}
  <button class="strip" onclick={() => setPanelOpen(true)} title="show the panel">
    <span class="vert">◂ {title}</span>
  </button>
{/if}

<style>
  .panel {
    position: sticky;
    top: 0;
    align-self: flex-start;
    flex: 0 0 var(--panel-w);
    width: var(--panel-w);
    max-height: calc(100vh - 36px);
    display: flex;
    flex-direction: column;
    min-height: 0;
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: var(--radius);
  }
  .grip {
    position: absolute;
    top: 0;
    bottom: 0;
    left: -4px;
    width: 8px;
    cursor: col-resize;
    z-index: 2;
  }
  .grip::after {
    content: '';
    position: absolute;
    inset: 0 3px;
    background: transparent;
    transition: background 120ms;
    border-radius: 2px;
  }
  .grip:hover::after,
  .grip:focus-visible::after,
  .panel.dragging .grip::after { background: var(--accent); }
  .grip:focus-visible { outline: none; }

  .head {
    padding: 10px 12px;
    border-bottom: 1px solid var(--line);
    display: flex;
    flex-direction: column;
    gap: 4px;
  }
  .fold {
    background: none;
    border: none;
    color: var(--muted);
    padding: 0 4px;
    line-height: 1;
  }
  .fold:hover { color: var(--text); border: none; }
  .body { overflow-y: auto; padding: 12px; min-height: 0; }

  .strip {
    position: sticky;
    top: 0;
    align-self: flex-start;
    flex: 0 0 30px;
    width: 30px;
    padding: 10px 0;
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    color: var(--muted);
  }
  .strip:hover { color: var(--text); border-color: var(--accent); }
  .vert {
    writing-mode: vertical-rl;
    font-size: 12px;
    white-space: nowrap;
    letter-spacing: 0.04em;
  }
</style>
