<script>
  // The heading *is* the name field. Nothing else on a page needs a name box,
  // and a field that only appears once you go for it keeps the emphasis on the
  // work below rather than on the label at the top. Agents and workflows both
  // do it this way; the difference is only what `oncommit` does with the name.
  //
  // Enter and blur both land on commit, and Enter causes the blur -- closing
  // the field unmounts the input. Without the guard the name is sent twice, and
  // on a workflow the second send races the first.
  let {
    value = '',
    editable = true,
    hint = 'enter to rename',
    title = 'rename this',
    lockedTitle = null,
    oncommit,
  } = $props()

  let editing = $state(false)
  let draft = $state('')

  function start() {
    draft = value
    editing = true
  }

  function commit() {
    if (!editing) return
    editing = false
    const next = draft.trim()
    if (!next || next === value) return
    oncommit?.(next)
  }
</script>

{#if editing}
  <input
    class="rename"
    bind:value={draft}
    autofocus
    spellcheck="false"
    onblur={commit}
    onkeydown={(e) => {
      if (e.key === 'Enter') commit()
      if (e.key === 'Escape') editing = false
    }}
  />
  <span class="small muted">{hint}</span>
{:else if editable}
  <button class="asname" onclick={start} {title}>
    <h1>{value}</h1>
  </button>
{:else}
  <h1 title={lockedTitle}>{value}</h1>
{/if}

<style>
  /* a heading that happens to be clickable, not a button that happens to hold
     one: no chrome until the pointer is on it */
  .asname {
    background: none;
    border: 1px solid transparent;
    padding: 1px 5px;
    margin-left: -5px;
    color: inherit;
    text-align: left;
  }
  .asname:hover { border-color: var(--line); background: var(--panel-2); }
  .rename {
    font: inherit;
    font-size: 18px;
    font-weight: 600;
    width: auto;
    max-width: 420px;
  }
</style>
