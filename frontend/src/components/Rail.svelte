<script>
  // Every rail is a flat list. Relationships -- a workflow's runs, an agent's
  // runs -- live in the main pane, not in a tree here.
  let {
    title,
    items = [],
    selected = null,
    onselect,
    actions,
    row,
    empty = 'nothing here yet',
    showArchivedToggle = false,
    showArchived = false,
    ontoggleArchived,
  } = $props()
</script>

<div class="rail">
  <div class="head">
    <div class="spread">
      <h3>{title}</h3>
      <div class="row">{@render actions?.()}</div>
    </div>
    {#if showArchivedToggle}
      <label class="small muted archived">
        <input
          type="checkbox"
          checked={showArchived}
          onchange={(e) => ontoggleArchived?.(e.currentTarget.checked)}
        />
        show archived
      </label>
    {/if}
  </div>

  <div class="list">
    {#if items.length === 0}
      <p class="small muted pad">{empty}</p>
    {/if}
    {#each items as item (item.id)}
      <div
        class="item"
        class:sel={item.id === selected}
        class:dim={item.dim}
        role="button"
        tabindex="0"
        onclick={() => onselect?.(item.id)}
        onkeydown={(e) => (e.key === 'Enter' || e.key === ' ') && onselect?.(item.id)}
      >
        {@render row(item)}
      </div>
    {/each}
  </div>
</div>

<style>
  .rail {
    width: 280px;
    flex: 0 0 280px;
    border-right: 1px solid var(--line);
    background: var(--panel);
    display: flex;
    flex-direction: column;
    min-height: 0;
  }
  .head {
    padding: 10px 12px;
    border-bottom: 1px solid var(--line);
    display: flex;
    flex-direction: column;
    gap: 6px;
  }
  .archived { display: flex; align-items: center; gap: 6px; cursor: pointer; }
  .archived input { width: auto; }
  .list { overflow-y: auto; flex: 1; min-height: 0; }
  .pad { padding: 12px; }
  .item {
    padding: 8px 12px;
    border-bottom: 1px solid var(--line);
    cursor: pointer;
  }
  .item:hover { background: var(--panel-2); }
  .item.sel { background: var(--panel-2); box-shadow: inset 2px 0 0 var(--accent); }
  .item.dim { opacity: 0.5; }
</style>
