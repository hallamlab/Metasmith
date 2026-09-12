<script>
  import {
    panelState,
    setPanelOpen,
    setPanelWidth,
    setPanelTop,
    PANEL_DEFAULT,
    PANEL_MIN,
    PANEL_MAX,
    PANEL_TOP_DEFAULT,
    PANEL_TOP_MIN,
    PANEL_TOP_MAX,
  } from '../lib/state.svelte.js'

  // The right-hand mirror of the rail: same grip, same pointer capture, same
  // remembered width. Collapsed it leaves a strip rather than vanishing, so
  // there is always something to click to get it back.
  //
  // It is a real column, not a block floating inside the page: the pane beside
  // it scrolls on its own, so left to right you get the page, the page's
  // scrollbar, this panel, and this panel's scrollbar. Only the lower section
  // scrolls -- the head and whatever `top` holds stay put while it does.
  //
  // `id` names which panel's remembered geometry this is. There is more than
  // one panel now and they are not interchangeable furniture, so sharing one
  // width and one open state made each page's layout depend on which other
  // page had been visited.
  let {
    id,
    title,
    subtitle = null,
    action = null,
    top = null,
    topDefault = PANEL_TOP_DEFAULT,
    fill = false,
    children,
  } = $props()

  // Read once, at init, rather than through `$derived`: the first read of a
  // panel id is what *hydrates* it from localStorage, and hydrating writes into
  // `ui.panels` -- a state write, which Svelte refuses inside a derived
  // (`state_unsafe_mutation`) and which took the whole view down with it. A
  // panel's id never changes for a mounted panel, and what it returns is a
  // `$state` object, so nothing reactive is lost by asking for it once.
  const st = panelState(id, topDefault)

  // reassigned when the panel is folded and unfolded, so it has to be reactive
  let el = $state(null)
  let dragging = $state(false)
  let draggingTop = $state(false)

  function startDrag(e) {
    if (e.button !== 0) return
    dragging = true
    e.currentTarget.setPointerCapture(e.pointerId)
    e.preventDefault()
  }

  // measured from the right edge, since that is the one that stays put
  function onDrag(e) {
    if (!dragging) return
    setPanelWidth(id, el.getBoundingClientRect().right - e.clientX)
  }

  function endDrag(e) {
    if (!dragging) return
    dragging = false
    e.currentTarget.releasePointerCapture?.(e.pointerId)
  }

  function onGripKey(e) {
    const step = e.shiftKey ? 40 : 10
    if (e.key === 'ArrowLeft') setPanelWidth(id, st.width + step)
    else if (e.key === 'ArrowRight') setPanelWidth(id, st.width - step)
    else if (e.key === 'Home') setPanelWidth(id, PANEL_DEFAULT)
    else return
    e.preventDefault()
  }

  // -- the split between the two sections ------------------------------------

  function startTopDrag(e) {
    if (e.button !== 0) return
    draggingTop = true
    e.currentTarget.setPointerCapture(e.pointerId)
    e.preventDefault()
  }

  // Measured from the top of the upper section, and clamped against the panel
  // as well as against itself: dragging it to the floor would leave the list
  // below with nothing to scroll in, which reads as the panel having broken.
  function onTopDrag(e) {
    if (!draggingTop) return
    const box = el?.getBoundingClientRect()
    if (!box) return
    const head = el.querySelector('.head')?.getBoundingClientRect().height ?? 0
    const room = box.height - head - 120
    setPanelTop(id, Math.min(room, e.clientY - box.top - head))
  }

  function endTopDrag(e) {
    if (!draggingTop) return
    draggingTop = false
    e.currentTarget.releasePointerCapture?.(e.pointerId)
  }

  function onTopGripKey(e) {
    const step = e.shiftKey ? 40 : 10
    if (e.key === 'ArrowUp') setPanelTop(id, st.top - step)
    else if (e.key === 'ArrowDown') setPanelTop(id, st.top + step)
    else if (e.key === 'Home') setPanelTop(id, topDefault)
    else return
    e.preventDefault()
  }

  $effect(() => {
    document.body.classList.toggle('resizing', dragging)
    document.body.classList.toggle('resizing-v', draggingTop)
    return () => {
      document.body.classList.remove('resizing')
      document.body.classList.remove('resizing-v')
    }
  })
</script>

{#if st.open}
  <aside
    class="panel"
    class:dragging
    bind:this={el}
    style="--panel-w: {st.width}px; --top-h: {st.top}px"
  >
    <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
    <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
    <div
      class="grip"
      role="separator"
      aria-orientation="vertical"
      aria-label="resize the panel"
      aria-valuenow={st.width}
      aria-valuemin={PANEL_MIN}
      aria-valuemax={PANEL_MAX}
      tabindex="0"
      title="drag to resize"
      onpointerdown={startDrag}
      onpointermove={onDrag}
      onpointerup={endDrag}
      onpointercancel={endDrag}
      ondblclick={() => setPanelWidth(id, PANEL_DEFAULT)}
      onkeydown={onGripKey}
    ></div>

    <div class="head">
      <div class="spread">
        <h3 class="truncate">{title}</h3>
        <div class="row">
          <!-- whatever the panel's contents can do to the page it is beside;
               it sits by the title because it acts on what the title names -->
          {#if action}{@render action()}{/if}
          <button class="fold" onclick={() => setPanelOpen(id, false)} title="collapse">▸</button>
        </div>
      </div>
      {#if subtitle}<div class="small muted truncate">{subtitle}</div>{/if}
    </div>

    {#if top}
      <div class="top" class:dragging={draggingTop}>{@render top()}</div>
      <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
      <!-- svelte-ignore a11y_no_noninteractive_tabindex -->
      <div
        class="hgrip"
        class:dragging={draggingTop}
        role="separator"
        aria-orientation="horizontal"
        aria-label="resize the upper section"
        aria-valuenow={st.top}
        aria-valuemin={PANEL_TOP_MIN}
        aria-valuemax={PANEL_TOP_MAX}
        tabindex="0"
        title="drag to resize"
        onpointerdown={startTopDrag}
        onpointermove={onTopDrag}
        onpointerup={endTopDrag}
        onpointercancel={endTopDrag}
        ondblclick={() => setPanelTop(id, topDefault)}
        onkeydown={onTopGripKey}
      ></div>
    {/if}

    <div class="body" class:fill>{@render children()}</div>
  </aside>
{:else}
  <button class="strip" onclick={() => setPanelOpen(id, true)} title="show the panel">
    <span class="vert">◂ {title}</span>
  </button>
{/if}

<style>
  .panel {
    position: relative;
    flex: 0 0 var(--panel-w);
    width: var(--panel-w);
    height: 100%;
    display: flex;
    flex-direction: column;
    min-height: 0;
    background: var(--panel);
    /* furniture, like the rail on the other edge: it runs the height of the
       window and is joined to it, rather than floating as a card in the page */
    border-left: 1px solid var(--line);
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
    flex: 0 0 auto;
  }
  .fold {
    background: none;
    border: none;
    color: var(--muted);
    padding: 0 4px;
    line-height: 1;
  }
  .fold:hover { color: var(--text); border: none; }

  /* fixed to the remembered height, and it does not scroll: what lives here is
     laid out to fit the space it is given, not scrolled through */
  .top {
    flex: 0 0 var(--top-h);
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  .hgrip {
    flex: 0 0 7px;
    position: relative;
    cursor: row-resize;
    border-top: 1px solid var(--line);
  }
  .hgrip::after {
    content: '';
    position: absolute;
    inset: 3px 0;
    background: transparent;
    transition: background 120ms;
    border-radius: 2px;
  }
  .hgrip:hover::after,
  .hgrip:focus-visible::after,
  .hgrip.dragging::after { background: var(--accent); }
  .hgrip:focus-visible { outline: none; }

  .body { flex: 1; overflow-y: auto; padding: 12px; min-height: 0; }
  /* for a body whose last child is a frame rather than a list: a column, so a
     `flex: 1` child takes whatever the ones above it left instead of standing
     at a fixed height with the panel empty under it. It still scrolls -- a
     child that will not shrink past its own floor pushes past the bottom. */
  .body.fill { display: flex; flex-direction: column; }

  .strip {
    align-self: stretch;
    flex: 0 0 30px;
    width: 30px;
    padding: 10px 0;
    background: var(--panel);
    border: none;
    border-left: 1px solid var(--line);
    border-radius: 0;
    color: var(--muted);
  }
  .strip:hover { color: var(--text); background: var(--panel-2); }
  .vert {
    writing-mode: vertical-rl;
    font-size: 12px;
    white-space: nowrap;
    letter-spacing: 0.04em;
  }
</style>
