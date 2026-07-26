<script>
  // What a row of the recipe descends from, and what descends from it.
  //
  // This is the grey "from /data/a.gbk" line the rows used to carry, made
  // editable. It is a menu anchored to one row rather than a section of the
  // list -- the same shape TypeSelect uses for the type field beside it -- so
  // it opens from the row it belongs to and closes the moment you look away.
  //
  // Only the parents are editable. What descends from this row is stated on
  // *that* row, which is where changing it belongs; listing it as an editable
  // control here as well would give one link two places to be changed from,
  // and no way to tell which one you were looking at.
  let {
    options = [], // {key, label, sub?} -- what this row could descend from
    selected = [], // the keys it does, as sent to the server
    descendants = [], // {label, sub?} -- what descends from it, read only
    disabled = false,
    // shown at the foot of the menu: why the lineage is what it is, or why it
    // cannot be changed
    note = null,
    onchange,
  } = $props()

  let open = $state(false)
  let root = $state(null)

  let byKey = $derived(new Map(options.map((o) => [o.key, o])))
  // a parent whose row has since been removed still has to be shown, or an
  // entry would sit in a lineage nothing on the page admits to
  let picked = $derived(selected.map((k) => byKey.get(k)?.label ?? String(k)))

  let summary = $derived.by(() => {
    const below = descendants.length ? `${descendants.length} below` : null
    if (!picked.length) return below ?? 'lineage'
    return below ? `from ${picked.join(', ')} · ${below}` : `from ${picked.join(', ')}`
  })

  function toggle(key) {
    onchange?.(selected.includes(key) ? selected.filter((k) => k !== key) : [...selected, key])
  }

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
</script>

<div class="lineage" bind:this={root}>
  <button
    class="trigger small truncate"
    class:some={picked.length > 0}
    aria-expanded={open}
    title={disabled ? note : 'what this descends from, and what descends from it'}
    onclick={() => (open = !open)}
    onkeydown={(e) => {
      if (e.key === 'Escape' && open) {
        e.stopPropagation()
        open = false
      }
    }}
  >{summary}</button>

  {#if open}
    <div class="menu">
      <div class="group small muted">descends from</div>
      {#if options.length === 0}
        <p class="empty small muted">Nothing else to descend from yet.</p>
      {:else}
        {#each options as o (o.key)}
          <label class="opt small" class:off={disabled}>
            <input
              type="checkbox"
              {disabled}
              checked={selected.includes(o.key)}
              onchange={() => toggle(o.key)}
            />
            <span class="mono truncate grow">{o.label}</span>
            {#if o.sub}<span class="muted truncate sub">{o.sub}</span>{/if}
          </label>
        {/each}
      {/if}

      <div class="group small muted">descended from by</div>
      {#if descendants.length === 0}
        <p class="empty small muted">Nothing descends from this.</p>
      {:else}
        {#each descendants as c, i (i)}
          <div class="opt small read">
            <span class="mono truncate grow">{c.label}</span>
            {#if c.sub}<span class="muted truncate sub">{c.sub}</span>{/if}
          </div>
        {/each}
        <p class="empty small muted">Change these on their own rows.</p>
      {/if}

      {#if note}<p class="empty small muted">{note}</p>{/if}
    </div>
  {/if}
</div>

<style>
  .lineage { position: relative; flex: 0 1 auto; min-width: 0; }
  /* quiet until it says something: an entry with no lineage is the common case,
     and a control shouting on every row of a long list is noise */
  .trigger {
    max-width: 220px;
    background: none;
    border: 1px solid transparent;
    color: var(--muted);
    padding: 1px 5px;
    text-align: left;
  }
  .trigger.some { color: var(--text); }
  .trigger:hover { border-color: var(--line); background: var(--panel-2); }

  /* right-aligned: the control sits mid-row, and a menu hung off its left edge
     would run off the page on the rows nearest the panel */
  .menu {
    position: absolute;
    z-index: 30;
    top: 100%;
    right: 0;
    min-width: 260px;
    max-width: 380px;
    max-height: 320px;
    overflow-y: auto;
    background: var(--panel);
    border: 1px solid var(--accent);
    border-radius: var(--radius);
    box-shadow: 0 10px 24px rgba(0, 0, 0, 0.45);
  }
  .group {
    padding: 5px 9px;
    background: var(--panel-2);
    border-bottom: 1px solid var(--line);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 10.5px;
  }
  .opt {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 4px 9px;
    border-bottom: 1px solid var(--line);
  }
  .opt:hover:not(.read) { background: var(--panel-2); }
  .opt input { width: auto; margin: 0; flex: 0 0 auto; }
  .opt.off { opacity: 0.6; }
  /* a child is stated, not offered: it lines up with the checkboxes above it
     rather than pretending to be one */
  .opt.read { padding-left: 24px; }
  .sub { flex: 0 1 auto; }
  .empty { padding: 6px 9px; margin: 0; line-height: 1.35; }
</style>
