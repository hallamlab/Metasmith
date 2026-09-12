<script>
  let { label, hint = null, children } = $props()
</script>

<label class="field">
  <span class="small muted">{label}</span>
  {@render children()}
  {#if hint}<span class="small muted hint">{hint}</span>{/if}
</label>

<style>
  .field { display: flex; flex-direction: column; gap: 3px; }
  .hint { line-height: 1.3; }
</style>
