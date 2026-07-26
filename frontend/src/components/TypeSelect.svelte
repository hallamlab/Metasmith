<script>
  import { rank } from '../lib/fuzzy.js'

  // A type picker that looks like a list you chose from, not a hint about what
  // you typed. The native `<input list>` + `<datalist>` this replaces was the
  // right behaviour behind the wrong shape: Chrome renders it as a pale bubble
  // in its own chrome, sized to the browser's taste rather than the field's, and
  // its option labels are shown as a dim aside — so the produce/consume counts
  // that are the whole reason to look at the list read as a tooltip.
  //
  // Also: a datalist is un-styleable and un-scriptable. There is no hook for
  // "row for a type nothing produces", no way to keep it open while the panel
  // beside it updates, and no way to show which row is active. All of that is
  // wanted here, so the list is ours.
  let {
    value = '',
    options = [],
    placeholder = '',
    // (option) => { label?, note?, warn? } for the right-hand side of a row
    describe = null,
    // a row shows its type as a word until it is clicked, and clicking it is
    // what mounts this -- so the click has to land in the field it just opened
    autofocus = false,
    onchange,
    oncommit,
  } = $props()

  let open = $state(false)
  let active = $state(0)
  let query = $state(value)
  let root = $state(null)
  let input = $state(null)
  let listbox = $state(null)

  const ROW_ID = 'msm-type-opt'

  // Typing filters; picking does not. After a pick the field holds a complete
  // type name, and filtering by it would leave a list of one -- so the query is
  // only what was typed, and a pick clears it back to "show everything".
  let typed = $state(false)

  $effect(() => {
    // an outside change (a chip in the panel, a hint) replaces what was typed
    if (value !== query) {
      query = value
      typed = false
    }
  })

  // Matching is fuzzy and ranked: the characters have to appear in order, not as
  // one run, so a half-remembered name still finds its type. Ordering is what
  // makes that usable rather than noisy -- see lib/fuzzy.js.
  let shown = $derived(typed ? rank(options, query) : options)

  function set(next, { close = false } = {}) {
    query = next
    typed = !close
    onchange?.(next)
    if (close) {
      open = false
      oncommit?.(next)
    }
  }

  function show() {
    open = true
    // land the highlight on what is already in the field, so Enter re-picks it
    const at = shown.indexOf(value)
    active = at >= 0 ? at : 0
    scrollActive()
  }

  function scrollActive() {
    // after the DOM has the new highlight, not before
    requestAnimationFrame(() => {
      listbox?.querySelector('.opt.active')?.scrollIntoView({ block: 'nearest' })
    })
  }

  function move(by) {
    if (!open) return show()
    if (!shown.length) return
    active = (active + by + shown.length) % shown.length
    scrollActive()
  }

  function keydown(e) {
    if (e.key === 'ArrowDown') {
      e.preventDefault()
      move(1)
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      move(-1)
    } else if (e.key === 'Enter') {
      if (open && shown[active]) {
        e.preventDefault()
        set(shown[active], { close: true })
      }
    } else if (e.key === 'Escape') {
      if (open) {
        // swallowed only when it had something to close
        e.stopPropagation()
        open = false
      }
    } else if (e.key === 'Tab') {
      open = false
    }
  }

  // Once, on mount. `input.focus()` fires `onfocus`, which opens the list --
  // so a field that opened because a word was clicked is already showing what
  // else that word could have been.
  $effect(() => {
    if (autofocus) input?.focus()
  })

  // A click anywhere else closes it. Pointerdown rather than click so a press
  // that starts outside cannot land on a row that moved out from under it.
  $effect(() => {
    if (!open) return
    const away = (e) => {
      if (!root?.contains(e.target)) open = false
    }
    window.addEventListener('pointerdown', away, true)
    return () => window.removeEventListener('pointerdown', away, true)
  })
</script>

<div class="select" bind:this={root}>
  <div class="control" class:open>
    <input
      bind:this={input}
      class="mono"
      role="combobox"
      aria-expanded={open}
      aria-controls="msm-type-list"
      aria-autocomplete="list"
      aria-activedescendant={open && shown[active] ? `${ROW_ID}-${active}` : undefined}
      value={query}
      {placeholder}
      spellcheck="false"
      autocomplete="off"
      oninput={(e) => {
        set(e.currentTarget.value)
        open = true
        active = 0
      }}
      onfocus={show}
      onkeydown={keydown}
    />
    <button
      class="caret"
      type="button"
      tabindex="-1"
      aria-label={open ? 'close the list' : 'open the list'}
      onclick={() => {
        if (open) {
          open = false
        } else {
          input?.focus()
          show()
        }
      }}
    >
      <!-- drawn rather than typed: `▾` renders as a faint speck at this size in
           the system font, and this is the affordance that says "a list opens" -->
      <svg viewBox="0 0 10 6" width="10" height="6" aria-hidden="true" class:up={open}>
        <path d="M1 1L5 5L9 1" fill="none" stroke="currentColor" stroke-width="1.6"
              stroke-linecap="round" stroke-linejoin="round" />
      </svg>
    </button>
  </div>

  {#if open}
    <div class="list" id="msm-type-list" role="listbox" bind:this={listbox}>
      {#if shown.length === 0}
        <div class="empty small muted">
          no type matches <span class="mono">{query}</span>
        </div>
      {:else}
        {#each shown.slice(0, 400) as opt, i (opt)}
          {@const d = describe?.(opt) ?? {}}
          <button
            type="button"
            class="opt"
            class:active={i === active}
            class:on={opt === value}
            id={`${ROW_ID}-${i}`}
            role="option"
            aria-selected={opt === value}
            onmouseenter={() => (active = i)}
            onclick={() => set(opt, { close: true })}
          >
            <span class="mono name truncate">{opt}</span>
            {#if d.note}
              <span class="small note" class:warn={d.warn}>{d.note}</span>
            {/if}
          </button>
        {/each}
        {#if shown.length > 400}
          <div class="empty small muted">
            {shown.length - 400} more — keep typing to narrow it
          </div>
        {/if}
      {/if}
    </div>
  {/if}
</div>

<style>
  .select { position: relative; }
  /* the input and its caret read as one control, so the border is on the wrapper
     and the input inside it has none of its own */
  .control {
    display: flex;
    align-items: stretch;
    background: var(--bg);
    border: 1px solid var(--line);
    border-radius: var(--radius);
  }
  .control:focus-within { outline: 1px solid var(--accent); }
  .control.open { border-bottom-left-radius: 0; border-bottom-right-radius: 0; }
  .control input {
    border: none;
    background: none;
    border-radius: var(--radius);
    min-width: 0;
  }
  .control input:focus { outline: none; }
  .caret {
    flex: 0 0 auto;
    display: flex;
    align-items: center;
    background: none;
    border: none;
    border-left: 1px solid var(--line);
    border-radius: 0;
    color: var(--muted);
    padding: 0 9px;
  }
  .caret:hover { color: var(--text); border-color: var(--line); }
  .caret svg.up { transform: rotate(180deg); }

  /* Deliberately not a floating bubble: it is the same width as the field and
     joined to it, so it reads as the field opened up. */
  .list {
    position: absolute;
    z-index: 30;
    top: 100%;
    left: 0;
    right: 0;
    max-height: 320px;
    overflow-y: auto;
    background: var(--panel);
    border: 1px solid var(--accent);
    border-top-color: var(--line);
    border-radius: 0 0 var(--radius) var(--radius);
    box-shadow: 0 10px 24px rgba(0, 0, 0, 0.45);
  }
  .opt {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
    width: 100%;
    background: none;
    border: none;
    border-radius: 0;
    border-bottom: 1px solid var(--line);
    padding: 5px 9px;
    text-align: left;
  }
  .opt:last-child { border-bottom: none; }
  .opt.active { background: var(--panel-2); }
  .opt.active:hover { border-color: transparent; }
  .opt.on .name { color: var(--accent); }
  .name { min-width: 0; }
  .note { flex: 0 0 auto; color: var(--muted); }
  .note.warn { color: var(--warn); }
  .empty { padding: 8px 9px; }
</style>
