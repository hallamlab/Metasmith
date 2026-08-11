<script>
  import TypeName from './TypeName.svelte'
  import { rank } from '../lib/fuzzy.js'
  import { splitType, typeName } from '../lib/types.js'

  // A type picker that looks like a list you chose from, not a hint about what
  // you typed. The native `<input list>` + `<datalist>` this replaces was the
  // right behaviour behind the wrong shape: Chrome renders it as a pale bubble
  // in its own chrome, sized to the browser's taste rather than the field's, and
  // its option labels are shown as a dim aside — so anything worth reading in
  // the list read as a tooltip.
  //
  // Also: a datalist is un-styleable and un-scriptable. There is no hook for
  // "row for a type nothing produces", no way to keep it open while the panel
  // beside it updates, and no way to show which row is active. All of that is
  // wanted here, so the list is ours.
  let {
    value = '',
    options = [],
    placeholder = '',
    // a row shows its type as a word until it is clicked, and clicking it is
    // what mounts this -- so the click has to land in the field it just opened
    autofocus = false,
    onchange,
    // (type) -- what was picked, once. Not `onchange`, which fires per
    // keystroke: anything hung off that chases `s`, `se`, `seq`.
    oncommit,
    // (type) -- what the highlight is on, as it is moved by an arrow key or a
    // pointer. Reading around the list is a question about a type too, so the
    // panel beside it follows; nothing is written by it.
    onpreview,
    // closed without picking, from any of the four ways there are to do that --
    // one hook, so whoever answered `onpreview` has one place to undo it
    onclose,
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
  let focused = $state(false)

  // Settled, the field draws the type the way the list drew it: the namespace
  // half size above, the bare name below. Being edited, the box holds the raw
  // string, because that is what is being typed into it.
  //
  // Not `typed` on its own, which is the *filter* flag and is deliberately
  // cleared by a pick so the list shows everything again; and `open` as well as
  // `focused`, so the half-beat of blur while the caret button is pressed does
  // not collapse the box under the cursor.
  let editing = $derived(focused || typed || open)

  // Unconditional, and that is the point: it holds the box's height still
  // between the two states. `LineageBand` measures every row and `LineageRail`
  // posts those heights to be drawn against, so a control that shrinks on focus
  // twitches the rail beside it every time a type is clicked.
  let namespace = $derived(splitType(query).ns)

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

  // Closed without a pick. Whatever was last previewed is not what this field
  // holds, so this is the one signal that says "put back what you were showing".
  function close() {
    if (!open) return
    open = false
    onclose?.()
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

  // A click on a settled field places the caret against the bare name that was
  // showing -- offset 5 in `fasta` -- and the swap to `seqs::fasta` then leaves
  // it at `seqs:|:fasta`. Move it to the end once the swap has landed, on the
  // same beat `scrollActive` waits for and for the same reason.
  //
  // Not `select()` instead: a mouse press overrides a selection and a Tab focus
  // does not, which would be two behaviours for one state.
  function caretToEnd() {
    requestAnimationFrame(() => {
      const at = input?.value.length ?? 0
      input?.setSelectionRange(at, at)
    })
  }

  function move(by) {
    if (!open) return show()
    if (!shown.length) return
    active = (active + by + shown.length) % shown.length
    scrollActive()
    onpreview?.(shown[active])
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
        close()
      }
    } else if (e.key === 'Tab') {
      close()
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
      if (!root?.contains(e.target)) close()
    }
    window.addEventListener('pointerdown', away, true)
    return () => window.removeEventListener('pointerdown', away, true)
  })
</script>

<div class="select" bind:this={root}>
  <div class="control" class:open>
    <!-- a real label rather than a handler forwarding the click, so pressing
         the namespace line lands the caret in the box under it. The caret
         button stays outside it: inside, every press would re-focus the input
         and re-open the list it was pressed to close. -->
    <label class="stack">
      <span class="ns truncate">{namespace || ' '}</span>
      <input
        bind:this={input}
        class="mono"
        role="combobox"
        aria-expanded={open}
        aria-controls="msm-type-list"
        aria-autocomplete="list"
        aria-activedescendant={open && shown[active] ? `${ROW_ID}-${active}` : undefined}
        aria-label="type"
        title={query}
        value={editing ? query : typeName(query)}
        {placeholder}
        spellcheck="false"
        autocomplete="off"
        oninput={(e) => {
          set(e.currentTarget.value)
          open = true
          active = 0
        }}
        onfocus={() => {
          const settled = !editing
          focused = true
          show()
          if (settled) caretToEnd()
        }}
        onblur={() => (focused = false)}
        onkeydown={keydown}
      />
    </label>
    <button
      class="caret"
      type="button"
      tabindex="-1"
      aria-label={open ? 'close the list' : 'open the list'}
      onclick={() => {
        if (open) {
          close()
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
          <button
            type="button"
            class="opt"
            class:active={i === active}
            class:on={opt === value}
            id={`${ROW_ID}-${i}`}
            role="option"
            aria-selected={opt === value}
            onmouseenter={() => {
              active = i
              onpreview?.(opt)
            }}
            onclick={() => set(opt, { close: true })}
          >
            <!-- the same two stacked lines the drawing gives a node, and the
                 same ones the parent menu draws -- one component, so a list and
                 a diagram naming the same type cannot say it two ways. The
                 counts that used to sit out to the right are on the row's own
                 note line under the field. -->
            <TypeName type={opt} />
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
  /* the two lines an option is drawn as, with the lower one editable. The
     padding is the stack's rather than the box's, so the namespace above and
     the name below share one left edge -- and one line-height with the list, so
     the field is the same height as the option it will hold. */
  .stack {
    display: flex;
    flex-direction: column;
    align-items: stretch;
    flex: 1 1 auto;
    min-width: 0;
    padding: 3px 8px;
    line-height: 1.2;
  }
  /* the same rule `TypeName` carries, and it has to be written out again: this
     line's sibling is an `<input>`, not a span, and a component that sometimes
     contains a form control is not one component. Each is the other's copy --
     change one, change both. */
  .ns { font-size: 0.5em; color: var(--muted); }
  .control input {
    border: none;
    background: none;
    border-radius: var(--radius);
    min-width: 0;
    /* off the box and onto `.stack`; doubled, it would undo the height the two
       lines were sized to hold */
    padding: 0;
    line-height: inherit;
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
    box-shadow: 0 10px 24px var(--shadow);
  }
  /* a column, so `scrollIntoView({block:'nearest'})` still steps over whole
     rows: the two lines are one option, not two */
  .opt {
    display: flex;
    flex-direction: column;
    align-items: stretch;
    width: 100%;
    background: none;
    border: none;
    border-radius: 0;
    border-bottom: 1px solid var(--line);
    padding: 4px 9px;
    text-align: left;
    line-height: 1.2;
  }
  .opt:last-child { border-bottom: none; }
  .opt.active { background: var(--panel-2); }
  .opt.active:hover { border-color: transparent; }
  /* the option a row already holds. Styles are scoped per component, so this
     cannot reach the name inside `TypeName` -- it hands the colour across the
     boundary as a custom property instead, which is the one thing that does
     cross one. */
  .opt.on { --typename-ink: var(--accent); }
  .empty { padding: 8px 9px; }
</style>
