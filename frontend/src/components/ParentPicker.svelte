<script>
  // What a row of the recipe descends from: the parents stated as chips at the
  // right of the row's detail line, each removable on its own, with a dropdown
  // beside them that adds one more.
  //
  // It was a menu of checkboxes before -- every row it *could* descend from,
  // ticked or not. That put what a row does descend from behind a click, in a
  // list of what it does not, and the two are not equally interesting: the
  // parents are part of reading the row, and the candidates are only wanted
  // while you are adding one. So the parents are the control and the menu is
  // the add button.
  //
  // One line, never two. This was a column, so a row grew and shrank by a whole
  // line as parents came and went -- and the trigger disappearing when there was
  // nothing left to add did the same again. Everything here is laid out
  // horizontally and the trigger's slot is always occupied, so nothing this
  // control does changes the height of the row it sits on. That matters more
  // than it sounds: the lineage rail beside these rows is drawn at their
  // measured heights, so a row twitching redraws the rail.
  //
  // What descends from this row is not here at all. It is stated on those rows,
  // which is where changing it belongs -- offering it twice gives one link two
  // places to be edited from and no way to tell which one you are looking at.
  let {
    // this row's own key, so a hover can name the *link* rather than one end
    self = null,
    chosen = [], // {key, sub?} -- in the order they were added. No label: which
    // row a chip names is the hover highlight's job, not this text's.
    options = [], // {key, label, sub?} -- legal to add: no self, no cycle, not already here
    disabled = false,
    // shown at the foot of the menu: why the list is what it is
    note = null,
    onadd,
    onremove,
    // ({child, parent}|null) -- the link this line is, so the recipe can mark
    // both of its ends and the one edge between them
    onhover,
  } = $props()

  let open = $state(false)
  let root = $state(null)

  // Pointerdown rather than click, and in the capture phase, so a press that
  // starts outside cannot land on a row that moved out from under it.
  $effect(() => {
    if (!open) return
    const away = (e) => {
      if (!root?.contains(e.target)) open = false
    }
    window.addEventListener('pointerdown', away, true)
    return () => window.removeEventListener('pointerdown', away, true)
  })

  // Per line rather than once on the container: a leave fires on every move
  // between two lines that touch, so the enter on the next one has to be what
  // settles it -- and a handler on the wrapper would be a mouse role on a plain
  // div, which it does not have. A list of parents is a list.
  const mark = (key) => onhover?.({ child: self, parent: key })
  const leave = () => onhover?.(null)

  // A chip names what its parent *is*, and the namespace is shared by every
  // type in a library -- so it is the half of the name that never tells two
  // parents apart. A parent whose type has not been filled in yet says so:
  // an em dash reads as "no parent", which is the opposite of the truth.
  const chip = (sub) => {
    const t = String(sub ?? '').trim()
    if (!t) return '<empty>'
    const cut = t.lastIndexOf('::')
    return cut > 0 ? t.slice(cut + 2) : t
  }
</script>

<div class="parents" bind:this={root}>
  <!-- still a list, so a chip may carry a pointer role -->
  <ul class="stack">
    {#each chosen as p (p.key)}
      <!-- a stated parent, hoverable so the row it names can be found: which
           row it is is what the hover highlight is for, so the chip states
           only its type -->
      <li class="parent" onmouseenter={() => mark(p.key)} onmouseleave={leave}>
        <span class="muted truncate small">{chip(p.sub)}</span>
        {#if !disabled}
          <!-- one click, not two: a lineage link is re-added from the menu
               right below it, so there is nothing here to protect against -->
          <button class="x" title="no longer descends from this" onclick={() => onremove?.(p.key)}
            >×</button>
        {/if}
      </li>
    {/each}
  </ul>

  {#if !disabled}
    <div class="add">
      <!-- always here, even with nothing left to offer. A control that comes
           and goes is a row that changes height, and this one sits beside a
           rail drawn at the row's measured height. Disabled, it is also the
           only place the "why is there nothing to descend from" sentence can
           be said at all. -->
      <button
        class="trigger small"
        aria-expanded={open}
        disabled={options.length === 0}
        title={options.length
          ? 'add something this descends from'
          : (note ?? 'nothing to descend from')}
        onclick={() => (open = !open)}
        onkeydown={(e) => {
          if (e.key === 'Escape' && open) {
            e.stopPropagation()
            open = false
          }
        }}
      >
        + parent
        <svg viewBox="0 0 10 6" width="10" height="6" aria-hidden="true" class:up={open}>
          <path d="M1 1L5 5L9 1" fill="none" stroke="currentColor" stroke-width="1.6"
                stroke-linecap="round" stroke-linejoin="round" />
        </svg>
      </button>

      {#if open}
        <div class="menu">
          {#each options as o (o.key)}
            <button
              class="opt small"
              onmouseenter={() => mark(o.key)}
              onmouseleave={leave}
              onclick={() => {
                open = false
                onhover?.(null)
                onadd?.(o.key)
              }}
            >
              <span class="mono truncate grow">{o.label}</span>
              {#if o.sub}<span class="muted truncate sub">{o.sub}</span>{/if}
            </button>
          {/each}
          {#if note}<p class="foot small muted">{note}</p>{/if}
        </div>
      {/if}
    </div>
  {/if}
</div>

<style>
  /* one line, right-aligned: the chips read as a trailing annotation on the
     row rather than as a second column of their own, and the row's height is
     the same whatever is in here.
     Not `overflow: hidden` here, however much this is the box whose width is
     being defended: the menu below hangs off `.add`, which is a child of this,
     at `top: 100%` -- so a clip here cuts away every pixel of it and pressing
     "+ parent" opens onto nothing. The clip belongs on the chips, which are the
     only thing that can grow; see `.stack`. */
  .parents {
    position: relative;
    display: flex;
    flex-wrap: nowrap;
    justify-content: flex-end;
    align-items: center;
    gap: 4px;
    min-width: 0;
  }
  /* a real box rather than `display: contents`, so the chips have somewhere of
     their own to be clipped without taking the menu with them. It shrinks
     (`flex: 0 1 auto`) while the trigger does not, so a row runs out of width
     by ellipsizing its chips, never by losing the control. */
  .stack {
    display: flex;
    flex-wrap: nowrap;
    justify-content: flex-end;
    align-items: center;
    gap: 4px;
    flex: 0 1 auto;
    min-width: 0;
    overflow: hidden;
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .parent {
    display: flex;
    align-items: center;
    gap: 4px;
    min-width: 0;
    padding: 1px 2px 1px 5px;
    border: 1px solid transparent;
    border-radius: var(--radius);
  }
  .parent:hover { border-color: var(--line); background: var(--panel-2); }
  .sub { flex: 0 1 auto; }
  .x {
    flex: 0 0 auto;
    background: none;
    border: none;
    color: var(--muted);
    padding: 0 3px;
    line-height: 1;
    font-size: 14px;
    border-radius: 3px;
  }
  .x:hover { color: var(--bad); border: none; }

  /* Quiet, not absent. Most rows have no lineage and a control shouting on every
     row of a long list is noise -- but with a fully transparent border on no
     background this read as plain text, and lineage was reported as a feature
     that did not exist. A dashed hairline is the least a thing can say and still
     say "you can press me"; hover firms it into the same box as before. */
  .trigger {
    display: flex;
    align-items: center;
    gap: 5px;
    white-space: nowrap;
    background: none;
    border: 1px dashed var(--line);
    color: var(--muted);
    padding: 1px 5px;
    text-align: left;
  }
  .trigger:hover:not(:disabled) { border-style: solid; background: var(--panel-2); }
  .trigger:disabled { opacity: 0.45; cursor: default; }
  .trigger svg.up { transform: rotate(180deg); }

  .add { position: relative; flex: 0 0 auto; }
  /* hung off the right edge, because that is the edge this control now sits
     against; left-aligned it would run off the side of the card */
  .menu {
    position: absolute;
    z-index: 30;
    top: 100%;
    right: 0;
    min-width: 240px;
    max-width: 380px;
    max-height: 300px;
    overflow-y: auto;
    background: var(--panel);
    border: 1px solid var(--accent);
    border-radius: var(--radius);
    box-shadow: 0 10px 24px var(--shadow);
  }
  .opt {
    display: flex;
    align-items: center;
    gap: 6px;
    width: 100%;
    background: none;
    border: none;
    border-radius: 0;
    border-bottom: 1px solid var(--line);
    padding: 4px 9px;
    text-align: left;
  }
  .opt:hover { background: var(--panel-2); border-color: var(--line); }
  .foot { padding: 6px 9px; margin: 0; line-height: 1.35; }
</style>
