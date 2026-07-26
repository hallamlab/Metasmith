<script>
  import DeleteControl from '../components/DeleteControl.svelte'

  // Inputs and outputs in one list. They are two headings over one run of rows,
  // the way the ssh rail does managed and native -- the rows stay siblings, and
  // a heading is a label rather than a parent.
  let {
    items = [],
    targets = [],
    sampleType = '',
    focus = null,
    onfocus,
    onsample,
    onremoveInput,
    onremoveTarget,
  } = $props()
</script>

<div class="col" style="gap:10px">
  <div class="spread">
    <h3>recipe</h3>
    <span class="small muted">{items.length} in · {targets.length} out</span>
  </div>

  <div class="rows">
    <div class="heading small muted">inputs</div>
    {#if items.length === 0}
      <p class="small muted pad">
        Nothing registered. Add the files and values you have below — the planner
        works out the steps from their types alone.
      </p>
    {:else}
      {#each items as item (item.path)}
        <div class="row-item" class:sel={focus === item.type_name}>
          <!-- the radio *is* the sample type: it is a property of the inputs you
               can see, not a select somewhere else that has to agree with them -->
          <!-- deliberately not one radio *group*: the mark is on the type, so
               every row sharing it is marked, and a group would let the browser
               enforce exactly one checked row and hide the rest of the branch -->
          <label class="gutter" title="one run per item of this type">
            <input
              type="radio"
              checked={sampleType === item.type_name}
              onclick={() => onsample?.(item.type_name)}
            />
          </label>
          <div class="grow truncate">
            <div class="mono truncate" title={item.path}>{item.path}</div>
            {#if item.parents?.length}
              <div class="small muted truncate">
                from {item.parents.map((p) => p.path).join(', ')}
              </div>
            {/if}
          </div>
          <button class="type mono truncate" onclick={() => onfocus?.(item.type_name)}>
            {item.type_name}
          </button>
          <DeleteControl title="remove from library" onconfirm={() => onremoveInput?.(item)} />
        </div>
      {/each}
    {/if}

    <div class="heading small muted">outputs</div>
    {#if targets.length === 0}
      <p class="small muted pad">Nothing wanted yet. Add at least one to generate.</p>
    {:else}
      {#each targets as target, i (i)}
        <div class="row-item" class:sel={focus === target.type}>
          <span class="gutter small muted">#{i + 1}</span>
          <div class="grow truncate">
            <button class="type mono truncate wide" onclick={() => onfocus?.(target.type)}>
              {target.type}
            </button>
            {#if target.parents?.length}
              <!-- what makes two targets of one type distinct, rather than a
                   duplicate request the builder would have refused -->
              <div class="small muted">
                from {target.parents.map((p) => `#${p + 1}`).join(', ')}
              </div>
            {/if}
          </div>
          <DeleteControl title="stop wanting this" onconfirm={() => onremoveTarget?.(i)} />
        </div>
      {/each}
    {/if}
  </div>

  <p class="small muted">
    The radio marks what each run starts from: every input of that type becomes
    its own branch. Removing an input unregisters it; the file itself is left alone.
  </p>
</div>

<style>
  .rows {
    border: 1px solid var(--line);
    border-radius: var(--radius);
    overflow: hidden;
  }
  .heading {
    padding: 6px 10px;
    background: var(--panel-2);
    border-bottom: 1px solid var(--line);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 11px;
  }
  .row-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
    border-bottom: 1px solid var(--line);
  }
  .row-item:last-child { border-bottom: none; }
  .row-item.sel { background: var(--panel-2); box-shadow: inset 2px 0 0 var(--accent); }
  .pad { padding: 8px 10px; margin: 0; }
  .gutter { flex: 0 0 22px; display: flex; align-items: center; }
  .gutter input { width: auto; margin: 0; }
  .type {
    flex: 0 1 auto;
    max-width: 45%;
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    text-align: left;
  }
  .type.wide { max-width: 100%; }
  .type:hover { text-decoration: underline; border: none; }
</style>
