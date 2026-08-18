<script>
  // A rail row's name, edited where it sits.
  //
  // Double click, not single: the row around this is what selects the thing,
  // and a name that opened an input on the way past would make the list
  // unusable for the one job it mainly does. `EditableName` is the other half
  // of this pair -- the same gesture on a page heading, where a single click
  // is free because nothing else claims it.
  let { value = '', title = 'double click to rename', oncommit } = $props()

  let editing = $state(false)
  let draft = $state('')

  function start() {
    draft = value
    editing = true
  }

  // Enter and blur both land here, and Enter causes the blur -- closing the
  // field unmounts the input. Without the guard the rename is sent twice.
  function commit() {
    if (!editing) return
    editing = false
    const next = draft.trim()
    if (!next || next === value) return
    oncommit?.(next)
  }
</script>

{#if editing}
  <!-- Every event stops at this input. The row around it is a `role="button"`
       that selects on click and on space, so a name with a space in it would
       otherwise change the selection mid-word. -->
  <input
    class="railname"
    bind:value={draft}
    autofocus
    spellcheck="false"
    onclick={(e) => e.stopPropagation()}
    ondblclick={(e) => e.stopPropagation()}
    onblur={commit}
    onkeydown={(e) => {
      e.stopPropagation()
      if (e.key === 'Enter') commit()
      if (e.key === 'Escape') editing = false
    }}
  />
{:else}
  <!-- svelte-ignore a11y_no_static_element_interactions -->
  <div class="truncate" {title} ondblclick={start}>{value}</div>
{/if}

<style>
  /* sized to the row it replaces, so opening it does not reflow the list */
  .railname {
    font: inherit;
    width: 100%;
    padding: 0;
    background: var(--panel-2);
  }
</style>
