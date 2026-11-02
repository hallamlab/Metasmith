<script>
  import TypeName from './TypeName.svelte'

  // Every ancestor of the selected result, not just its direct parents: the
  // collected library re-expands the reduced parent graph on load, so this is
  // the whole chain back to the given inputs, nearest first.
  let { node = null, onpick = null } = $props()

  let rows = $derived(node?.parents ?? [])

  function basename(p) {
    const s = String(p ?? '')
    const i = s.lastIndexOf('/')
    return i < 0 ? s : s.slice(i + 1)
  }
</script>

<div class="box">
  {#if !node}
    <p class="small muted pad">Pick a file to see where it came from.</p>
  {:else if !node.is_item}
    <p class="small muted pad">
      This file is not in the result library, so nothing records what produced it.
    </p>
  {:else if !rows.length}
    <p class="small muted pad">
      No parents recorded — this is an input the run was given, not something it
      made.
    </p>
  {:else}
    {#each rows as p (p.path)}
      {#if p.node}
        <button class="row" onclick={() => onpick?.(p.node)} title={p.path}>
          <span class="ty"><TypeName type={p.type_name} /></span>
          <span class="name truncate">{basename(p.path)}</span>
        </button>
      {:else}
        <div class="row still" title={p.path}>
          <span class="ty"><TypeName type={p.type_name} /></span>
          <span class="name truncate">{basename(p.path)}</span>
          <span class="small muted">not collected</span>
        </div>
      {/if}
    {/each}
  {/if}
</div>

<style>
  .box { height: 100%; overflow: auto; }
  .pad { padding: 8px; }
  .row {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    padding: 4px 8px;
    border: none;
    background: none;
    color: inherit;
    text-align: left;
    font: inherit;
  }
  button.row { cursor: pointer; }
  button.row:hover { background: var(--hover, rgba(127, 127, 127, 0.12)); }
  .ty { display: flex; flex-direction: column; min-width: 0; flex: 0 1 45%; }
  .name { min-width: 0; flex: 1 1 auto; color: var(--muted); font-size: 0.85em; }
  .still { opacity: 0.7; }
</style>
