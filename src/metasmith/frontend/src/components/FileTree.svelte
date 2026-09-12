<script>
  // A collected result library as the folder it is.
  //
  // The tree comes from a real walk of the directory rather than from the
  // library manifest, which is why the run's own logs are in it: they are
  // bookkeeping the manifest does not list, and they are the thing you want
  // when a step went wrong.
  //
  // Recursion is by self-import on a node's children, so a node knows only
  // about itself and the depth it was handed.
  import { bytes } from '../lib/format.js'
  import FileTree from './FileTree.svelte'

  let { node, depth = 0, selected = null, onpick } = $props()

  // Products first, then metadata, then the logs -- and within a level the
  // server already sorted directories before files.
  let kids = $derived(node.children ?? [])

  const key = (n) => n.path

  // Directories start open at the top of the tree and closed further down: a
  // results folder is two levels of products, and a log directory is a hundred
  // files nobody asked to see yet.
  let expanded = $state(new Set())
  let ready = $state(false)
  $effect(() => {
    if (ready) return
    ready = true
    expanded = new Set(
      kids.filter((n) => n.type === 'dir' && n.role === 'output').map(key),
    )
  })

  function toggle(n) {
    const next = new Set(expanded)
    if (next.has(key(n))) next.delete(key(n))
    else next.add(key(n))
    expanded = next
  }
</script>

{#each kids as n (n.path)}
  {#if n.type === 'dir'}
    <button
      class="row dir"
      style="padding-left: {8 + depth * 12}px"
      onclick={() => toggle(n)}
    >
      <span class="caret" class:open={expanded.has(key(n))}>▸</span>
      <span class="name truncate">{n.name}</span>
      {#if n.symlink}<span class="small muted">alias</span>{/if}
    </button>
    {#if expanded.has(key(n))}
      <FileTree node={n} depth={depth + 1} {selected} {onpick} />
    {/if}
  {:else}
    <button
      class="row file"
      class:sel={selected === n.path}
      class:dangling={n.dangling}
      style="padding-left: {8 + depth * 12 + 12}px"
      onclick={() => onpick?.(n)}
      title={n.dangling ? 'the data behind this link is gone on the agent' : n.path}
    >
      <span class="name truncate">{n.name}</span>
      <span class="meta small muted">
        {#if n.type_name}<span class="ty">{n.type_name}</span>{/if}
        {n.dangling ? 'missing' : bytes(n.size)}
      </span>
    </button>
  {/if}
{/each}

{#if depth === 0 && !kids.length}
  <p class="small muted" style="padding:8px">nothing here</p>
{/if}

<style>
  .row {
    display: flex;
    align-items: baseline;
    gap: 6px;
    width: 100%;
    background: none;
    border: none;
    border-radius: 4px;
    padding: 3px 8px;
    text-align: left;
    font-size: 12px;
    color: var(--text);
  }
  .row:hover { background: var(--panel-2); border: none; }
  .row.sel { background: var(--panel-2); box-shadow: inset 2px 0 0 var(--accent); }
  .row.dangling .name { text-decoration: line-through; color: var(--bad); }
  .name { flex: 1; min-width: 0; }
  .meta { flex: 0 0 auto; display: flex; gap: 6px; }
  .ty { font-family: var(--mono, monospace); opacity: 0.75; }
  .caret {
    flex: 0 0 auto;
    font-size: 9px;
    color: var(--muted);
    transition: transform 0.1s;
  }
  .caret.open { transform: rotate(90deg); }
  .dir .name { color: var(--muted); }
</style>
