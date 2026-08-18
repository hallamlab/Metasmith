<script>
  // The libraries, as chips: the eye and the checkbox from the old list
  // collapsed into one gesture each. A chip *is* an enabled library -- clicking
  // its name draws it, its × disables it -- and a library that is off is not
  // drawn as a struck-through row any more, it is just not a chip; it waits in
  // the dropdown instead, which is the only place left to turn one back on.
  //
  // This used to be a fold in the middle of the builder, a long way from the
  // counts and the list it narrows. It lives here now, above both, with a rule
  // under it to keep it from reading as part of the graph below.
  //
  // The tally under the dropdown is what the panel's third section used to
  // hold. It says how much of the library is switched on behind the drawing --
  // which is the one thing the drawing cannot say about itself -- so it belongs
  // beside the switches rather than below the picture.
  let { index = null, libraries = [], enabled = null, viewing = null, ontoggle, onview } = $props()

  // `enabled` is null for "all of them" and a Set otherwise -- stored that way so
  // a library added to the standard library later is picked up rather than
  // silently excluded. Read it the wrong way round and every chip renders off.
  const on = (l) => !enabled || enabled.has(l.path)

  let onLibs = $derived(libraries.filter(on))
  let offLibs = $derived(libraries.filter((l) => !on(l)))

  let transformCount = $derived(
    (index?.transforms ?? []).filter((t) => !enabled || enabled.has(t.library)).length,
  )
  let typeCount = $derived(Object.keys(index?.by_type ?? {}).length)
  let unreadable = $derived((index?.libraries ?? []).filter((l) => l.error))

  function addFromSelect(e) {
    const path = e.currentTarget.value
    e.currentTarget.value = ''
    if (path) ontoggle?.(path)
  }
</script>

<div class="wrap">
  <div class="head small">
    <span class="muted grow">transform libraries</span>
  </div>

  <div class="chips">
    {#each onLibs as l (l.path)}
      <span class="chip" class:viewing={viewing === l.path} class:bad={!!l.error}>
        <button
          class="name truncate"
          title={l.error ?? l.path}
          aria-pressed={viewing === l.path}
          onclick={() => onview?.(l.path)}
        >{l.name}{#if !l.error}<span class="count muted">{l.transform_count}</span>{/if}</button>
        <button
          class="x"
          title="disable this library"
          aria-label="disable {l.name}"
          onclick={() => ontoggle?.(l.path)}
        >×</button>
      </span>
    {/each}
    {#if !libraries.length}
      <p class="small muted empty">no transform libraries found</p>
    {:else if !onLibs.length}
      <p class="small muted empty">nothing enabled — pick one below</p>
    {/if}
  </div>

  {#if offLibs.length}
    <select class="add small" value="" onchange={addFromSelect}>
      <option value="" disabled>+ add a library…</option>
      {#each offLibs as l (l.path)}
        <option value={l.path}>{l.name}</option>
      {/each}
    </select>
  {/if}

  {#if index}
    <table class="tally small">
      <tbody>
        <tr><td class="muted">transforms</td><td>{transformCount}</td></tr>
        <tr><td class="muted">types known</td><td>{typeCount}</td></tr>
        <tr>
          <td class="muted">libraries</td>
          <td title={offLibs.length
            ? 'a generate may only use the enabled ones, and narrowing marks a result stale'
            : 'all of them are offered to the planner'}>{onLibs.length} of {libraries.length}</td>
        </tr>
      </tbody>
    </table>
    {#each unreadable as l}
      <p class="small err"><span class="tag bad">unreadable</span>
        <span class="mono">{l.name}</span> — {l.error}</p>
    {/each}
  {/if}
</div>

<style>
  .wrap {
    flex: 0 0 auto;
    padding-bottom: 8px;
    margin-bottom: 6px;
    /* the line the DAG window sits under -- without it the chips and the
       graph read as one scrolling list rather than two things */
    border-bottom: 1px solid var(--line);
  }
  .head {
    display: flex;
    gap: 8px;
    align-items: baseline;
    padding: 0 0 6px;
  }
  /* no cap and no scroller of its own. It had one while this sat in a fixed
     section of the panel and the graph below it wanted the room; the panel is
     one scrolling column now, so a second scroller inside it is a small window
     you have to find and drive separately to read a list that would otherwise
     just be there. */
  .chips {
    display: flex;
    flex-wrap: wrap;
    gap: 5px;
  }
  .empty { margin: 0; padding: 2px 0; }
  .chip {
    display: flex;
    align-items: center;
    gap: 2px;
    max-width: 100%;
    border: 1px solid var(--line);
    border-radius: 12px;
    background: var(--panel-2);
    padding-left: 2px;
  }
  .chip.viewing { border-color: var(--accent); }
  .chip.bad { border-color: var(--tag-bad-line); }
  .chip .name {
    display: flex;
    align-items: baseline;
    gap: 4px;
    min-width: 0;
    max-width: 200px;
    background: none;
    border: none;
    border-radius: 10px;
    padding: 2px 4px 2px 8px;
    font-size: 12px;
    font-family: var(--mono, monospace);
    color: var(--text);
  }
  .chip.bad .name { color: var(--bad); }
  .chip .name:hover { background: var(--panel); }
  .chip.viewing .name { color: var(--accent); }
  .chip .count { font-size: 10px; }
  .chip .x {
    flex: 0 0 auto;
    background: none;
    border: none;
    border-radius: 50%;
    color: var(--muted);
    line-height: 1;
    padding: 2px 7px 2px 3px;
    font-size: 13px;
  }
  .chip .x:hover { color: var(--bad); }
  .add { margin: 6px 0 0; }
  .tally { margin-top: 8px; }
  .err { margin: 6px 0 0; line-height: 1.3; }
</style>
