<script>
  // What a row of the recipe descends from: the parents stated one per line,
  // each removable on its own, under a dropdown that adds one more.
  //
  // It was a menu of checkboxes before -- every row it *could* descend from,
  // ticked or not. That put what a row does descend from behind a click, in a
  // list of what it does not, and the two are not equally interesting: the
  // parents are part of reading the row, and the candidates are only wanted
  // while you are adding one. So the parents are the control and the menu is
  // the add button.
  //
  // What descends from this row is not here at all. It is stated on those rows,
  // which is where changing it belongs -- offering it twice gives one link two
  // places to be edited from and no way to tell which one you are looking at.
  let {
    chosen = [], // {key, label, sub?} -- in the order they were added
    options = [], // {key, label, sub?} -- legal to add: no self, no cycle, not already here
    disabled = false,
    // shown at the foot of the menu: why the list is what it is
    note = null,
    onadd,
    onremove,
    // (key|null) -- the row this line refers to, so the recipe can mark it
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
  const leave = () => onhover?.(null)
</script>

<div class="parents" bind:this={root}>
  {#if chosen.length}
    <ul class="stack">
      {#each chosen as p (p.key)}
        <!-- a stated parent, hoverable so the row it names can be found: the
             label is a type name on an output and a path on an input, and
             neither is unique enough on its own to point at one row -->
        <li class="parent" onmouseenter={() => onhover?.(p.key)} onmouseleave={leave}>
          <span class="mono truncate grow">{p.label}</span>
          {#if p.draft}<span class="draft small">draft</span>{/if}
          {#if p.sub}<span class="muted truncate sub small">{p.sub}</span>{/if}
          {#if !disabled}
            <!-- one click, not two: a lineage link is re-added from the menu
                 right below it, so there is nothing here to protect against -->
            <button class="x" title="no longer descends from this" onclick={() => onremove?.(p.key)}
              >×</button>
          {/if}
        </li>
      {/each}
    </ul>
  {/if}

  {#if !disabled && options.length > 0}
    <div class="add">
      <button
        class="trigger small"
        aria-expanded={open}
        title="add something this descends from"
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
              onmouseenter={() => onhover?.(o.key)}
              onmouseleave={leave}
              onclick={() => {
                open = false
                onhover?.(null)
                onadd?.(o.key)
              }}
            >
              <span class="mono truncate grow">{o.label}</span>
              <!-- offerable, but not registered yet: a row that descends from
                   one of these waits for it, and the wait is invisible unless
                   the list says which rows are which -->
              {#if o.draft}<span class="draft small">draft</span>{/if}
              {#if o.sub}<span class="muted truncate sub">{o.sub}</span>{/if}
            </button>
          {/each}
          {#if note}<p class="foot small muted">{note}</p>{/if}
        </div>
      {/if}
    </div>
  {:else if !chosen.length}
    <!-- the column is still the column: an empty one that collapsed would move
         the delete on the row above it.
         This is also the only place the note can be said. It used to be drawn
         only at the foot of the open menu -- which is exactly the thing an empty
         candidate list has no trigger for -- so the sentence explaining why a row
         has nothing to descend from was unreachable in every case it explains,
         and the row said the bare "nothing to descend from" instead. -->
    <span class="none small muted">{note ?? 'nothing to descend from'}</span>
  {/if}
</div>

<style>
  .parents,
  .stack {
    position: relative;
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 2px;
    min-width: 0;
  }
  .stack { list-style: none; margin: 0; padding: 0; width: 100%; }
  .parent {
    display: flex;
    align-items: center;
    gap: 6px;
    max-width: 100%;
    padding: 1px 2px 1px 5px;
    border: 1px solid transparent;
    border-radius: var(--radius);
  }
  .parent:hover { border-color: var(--line); background: var(--panel-2); }
  .sub { flex: 0 1 auto; }
  /* not a warning -- a draft parent is a legitimate thing to name, it just has
     not registered yet */
  .draft {
    flex: 0 0 auto;
    color: var(--warn);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    padding: 0 4px;
    font-size: 10px;
  }
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
    background: none;
    border: 1px dashed var(--line);
    color: var(--muted);
    padding: 1px 5px;
    text-align: left;
  }
  .trigger:hover { border-style: solid; background: var(--panel-2); }
  .trigger svg.up { transform: rotate(180deg); }
  .none { padding: 2px 5px; }

  .add { position: relative; }
  /* left-aligned under its trigger, unlike the type list: this control sits at
     the right of the row already, and hanging the menu off the right edge would
     put it under the panel */
  .menu {
    position: absolute;
    z-index: 30;
    top: 100%;
    left: 0;
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
