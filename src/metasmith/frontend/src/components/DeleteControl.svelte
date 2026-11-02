<script>
  // A small cross, double-click to confirm. Deleting is archiving: the first
  // press tombstones and the row is still there under the `archived` chip, so
  // the label says that up front rather than letting the outcome be a surprise
  // in either direction. `archived` flips it to what a second press does --
  // remove the thing for good -- which is the one press that is not undoable.
  let { onconfirm, title = 'delete', archived = false } = $props()
  let armed = $state(false)
  let timer

  function arm(e) {
    e.stopPropagation()
    if (armed) {
      clearTimeout(timer)
      armed = false
      onconfirm()
      return
    }
    armed = true
    timer = setTimeout(() => (armed = false), 2000)
  }
</script>

<button
  class="x"
  class:armed
  class:forgood={archived}
  onclick={arm}
  title={armed
    ? archived
      ? 'click again to remove it for good — this one cannot be undone'
      : 'click again to confirm'
    : archived
      ? `${title} for good`
      : `${title} (archived, so you can get it back)`}
  aria-label={title}
>{armed ? '✓' : '×'}</button>

<style>
  .x {
    background: none;
    border: none;
    color: var(--muted);
    padding: 0 4px;
    line-height: 1;
    font-size: 16px;
    border-radius: 3px;
  }
  .x:hover { color: var(--bad); border: none; }
  .armed { color: var(--bad); font-size: 13px; }
  /* the second delete is the destructive one, and it is the only one that reads
     as such at rest */
  .forgood { color: var(--warn); }
</style>
