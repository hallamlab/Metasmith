<script>
  import DeleteControl from './DeleteControl.svelte'
  import { PARAM_NAME, paramProblems, splitPair } from '../lib/params.js'

  // Key/value rows for nextflow params. Two boxes and a cross, the shape the
  // recipe rows already use, because a params file is a list of one thing and
  // a textarea of yaml would be a second syntax to get wrong.
  //
  // Rows rather than an object, so a half-typed key does not collide with
  // another half-typed key and eat it. `rows` is what the parent binds;
  // `toParams` below is how it becomes something to send.
  let {
    rows = $bindable([]),
    // shown greyed in a row whose value is empty, so an inherited default reads
    // as "this is what it will be" rather than as an empty box
    inherited = {},
    keyPlaceholder = 'slurmAccount',
    valuePlaceholder = 'the account to charge',
  } = $props()

  // Read off the rows each time rather than kept in a counter: the parent may
  // replace the whole list (the launch panel re-seeds it when the agent
  // changes), and a stale counter would then hand out an id already in use --
  // which the keyed `each` renders as one row eating another's keystrokes.
  function add() {
    const next = rows.reduce((m, r) => Math.max(m, r.id ?? 0), 0) + 1
    rows = [...rows, { id: next, key: '', value: '' }]
  }

  function drop(id) {
    rows = rows.filter((r) => r.id !== id)
  }

  function settle(id) {
    rows = rows.map((r) => (r.id === id ? splitPair(r) : r))
  }

  let problems = $derived(paramProblems(rows))
</script>

<div class="rows">
  {#each rows as row (row.id)}
    <div class="entry">
      <input
        class="grow mono"
        class:bad={row.key?.trim() && !PARAM_NAME.test(row.key.trim())}
        bind:value={row.key}
        onblur={() => settle(row.id)}
        placeholder={keyPlaceholder}
        spellcheck="false"
        aria-label="param name"
      />
      <input
        class="grow mono"
        bind:value={row.value}
        placeholder={inherited[row.key?.trim()] ?? valuePlaceholder}
        spellcheck="false"
        aria-label="param value"
      />
      <span class="trail">
        <DeleteControl title="remove this param" onconfirm={() => drop(row.id)} />
      </span>
    </div>
  {/each}

  {#each problems as problem}
    <span class="small problem">{problem}</span>
  {/each}

  <div class="entry addrow">
    <button class="small" onclick={add}>+ a param</button>
  </div>
</div>

<style>
  .rows { display: flex; flex-direction: column; gap: 4px; }
  .entry { display: flex; align-items: center; gap: 6px; }
  .entry :global(input) { min-width: 0; }
  .trail { display: flex; align-items: center; }
  .addrow { gap: 10px; }
  .entry :global(input.bad) { border-color: var(--bad); }
  .problem { color: var(--bad); }
</style>
