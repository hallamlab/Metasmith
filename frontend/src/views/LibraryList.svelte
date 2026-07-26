<script>
  import Icon from '../components/Icon.svelte'

  // The libraries, stacked, with the two things you do to one kept apart: the
  // eye draws it, the checkbox decides whether the planner may reach for it.
  // They are deliberately independent -- looking at a library you have switched
  // off is a reasonable thing to want, and switching one off should not yank the
  // graph out from under you.
  //
  // This used to be a fold in the middle of the builder, a long way from the
  // counts and the list it narrows. It lives here now, above both.
  let { libraries = [], enabled = null, viewing = null, ontoggle, onview } = $props()

  // `enabled` is null for "all of them" and a Set otherwise -- stored that way so
  // a library added to the standard library later is picked up rather than
  // silently excluded. Read it the wrong way round and every box renders off.
  const on = (l) => !enabled || enabled.has(l.path)

  let count = $derived(libraries.filter(on).length)

  // A checkbox flips itself the moment it is clicked, and the last library may
  // not be switched off -- so on a refusal the box would sit unticked over a
  // library that is still enabled. Nothing else re-renders it, because from the
  // page's point of view nothing changed. Put it back to whatever the state
  // actually says, once that state has had its turn.
  function toggle(l, box) {
    ontoggle?.(l.path)
    queueMicrotask(() => (box.checked = on(l)))
  }
</script>

<div class="head small">
  <span class="muted grow">transform libraries</span>
  <span class="muted" title={count < libraries.length
    ? 'a generate may only use the ticked ones, and narrowing marks a result stale'
    : 'all of them are offered to the planner'}>
    {count} of {libraries.length}
  </span>
</div>

<div class="rows">
  {#each libraries as l (l.path)}
    <div class="row lib" class:off={!on(l)} class:viewing={viewing === l.path}>
      <button
        class="eye"
        class:on={viewing === l.path}
        title="draw this library"
        aria-label="draw {l.name}"
        aria-pressed={viewing === l.path}
        onclick={() => onview?.(l.path)}
      >
        <Icon name="eye" size={13} />
      </button>
      <span class="mono truncate grow" title={l.path}>{l.name}</span>
      {#if l.error}
        <span class="tag bad" title={l.error}>unreadable</span>
      {:else}
        <span class="small muted">{l.transform_count}</span>
      {/if}
      <input
        type="checkbox"
        title="offer this library to the planner"
        aria-label="enable {l.name}"
        checked={on(l)}
        onchange={(e) => toggle(l, e.currentTarget)}
      />
    </div>
  {/each}
  {#if !libraries.length}
    <p class="small muted">no transform libraries found</p>
  {/if}
</div>

<style>
  .head {
    display: flex;
    gap: 8px;
    align-items: baseline;
    padding: 8px 12px 4px;
  }
  .rows {
    /* a fixed ceiling rather than a share of the panel: the graph below is what
       wants the room, and a library list that grows into it as libraries are
       added would take that room without being asked */
    max-height: 118px;
    overflow-y: auto;
    padding: 0 12px 6px;
    flex: 0 0 auto;
  }
  .lib { gap: 6px; padding: 1px 0; }
  .lib.off .mono { color: var(--muted); text-decoration: line-through; }
  .lib input { width: auto; flex: 0 0 auto; }
  .eye {
    flex: 0 0 auto;
    display: flex;
    padding: 3px;
    background: none;
    border-color: transparent;
    color: var(--muted);
  }
  .eye:hover { color: var(--text); background: var(--panel-2); }
  .eye.on { color: var(--accent); border-color: var(--line); background: var(--panel-2); }
</style>
