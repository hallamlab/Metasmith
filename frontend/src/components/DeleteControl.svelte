<script>
  // A small cross, double-click to confirm. Where an object still has
  // dependents the backend archives instead of deleting, so the label says so
  // up front -- you know the outcome before you commit to it.
  let { onconfirm, title = 'delete', archives = false } = $props()
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
  onclick={arm}
  title={armed ? 'click again to confirm' : archives ? `${title} (archives if in use)` : title}
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
</style>
