<script>
  // The sheet a study already has, sitting at the top of the inputs half.
  //
  // What it deliberately does not do is list what it made. Two hundred samples
  // is two hundred rows of nothing to decide -- you declare each *kind* of input
  // once, point it at a column, and the count is the answer. The individual
  // items are in the library and the plan; they are not a thing to read here.
  import DeleteControl from './DeleteControl.svelte'

  let {
    table = null, // {attached, filename, columns, row_count, preview, problems, expansion}
    onattach, // (File|null, text|null) => Promise
    ondetach,
  } = $props()

  let pasting = $state(false)
  let text = $state('')
  let peeking = $state(false)

  let attached = $derived(!!table?.attached)
  let problems = $derived(table?.problems ?? [])
  // What the last solve registered -- solving is what turns an array row into
  // real items now, one per sheet row, so there is nothing to expand or
  // unregister by hand here any more; solving again is also what refreshes
  // this count against however the sheet or the rows have since changed.
  let registered = $derived(table?.expansion?.row_count ?? 0)

  async function paste() {
    if (!text.trim()) return
    await onattach?.(null, text)
    text = ''
    pasting = false
  }

  async function upload(e) {
    const file = e.currentTarget.files?.[0]
    if (!file) return
    await onattach?.(file, null)
    e.currentTarget.value = ''
  }
</script>

<div class="strip" class:on={attached}>
  {#if !attached}
    <div class="row wrap">
      <span class="small muted grow">
        Have a sample sheet? Attach it and every row below picks a column of it
        instead of holding its own value — one run per sheet row. The sheet has
        to hold the finished values; nothing is built out of them.
      </span>
      <label class="filebtn small">
        upload
        <input type="file" accept=".csv,.tsv,.tab,.txt,.xlsx,.xlsm" onchange={upload} />
      </label>
      <button class="small" onclick={() => (pasting = !pasting)}>paste</button>
    </div>
    {#if pasting}
      <textarea
        class="mono"
        rows="4"
        spellcheck="false"
        placeholder={'sample,forward,reverse\nS1,/data/S1_R1.fq.gz,/data/S1_R2.fq.gz'}
        bind:value={text}
      ></textarea>
      <div class="row">
        <button class="small" onclick={paste} disabled={!text.trim()}>use this</button>
        <span class="small muted">csv, tsv, or whatever the delimiter is</span>
      </div>
    {/if}
  {:else}
    <div class="row wrap">
      <!-- an empty `details` -- its content is rendered below, as a sibling,
           not inside it. What is wanted here is the native disclosure toggle
           and nothing else about the element: on the left, ahead of the
           filename it is a peek *of*, the same way the plan's own diagram
           folds behind a `summary` rather than a button. -->
      <details class="peek-toggle" bind:open={peeking}>
        <summary class="small muted">{peeking ? 'hide' : 'peek'}</summary>
      </details>
      <span class="mono truncate grow" title={table.filename}>{table.filename}</span>
      <span class="small muted">{table.row_count} row(s) · {table.columns.length} column(s)</span>
      <DeleteControl title="take the sheet away — what it registered stays" onconfirm={ondetach} />
    </div>

    {#if peeking}
      <div class="peek">
        <table class="small">
          <thead>
            <tr>{#each table.columns as c}<th class="mono">{c}</th>{/each}</tr>
          </thead>
          <tbody>
            {#each table.preview ?? [] as r}
              <tr>{#each table.columns as c}<td class="mono truncate">{r[c]}</td>{/each}</tr>
            {/each}
          </tbody>
        </table>
        {#if table.row_count > (table.preview ?? []).length}
          <span class="small muted">…and {table.row_count - table.preview.length} more</span>
        {/if}
      </div>
    {/if}

    {#if problems.length}
      <ul class="small problems">
        {#each problems as p}<li>{p.message}</li>{/each}
      </ul>
    {:else if registered}
      <span class="small muted">{registered} row(s) registered, as of the last solve</span>
    {:else}
      <span class="small muted">
        Ready — solving registers one item per array row, per sheet row.
      </span>
    {/if}
  {/if}
</div>

<style>
  .strip {
    display: flex;
    flex-direction: column;
    gap: 6px;
    padding: 8px 10px;
    border-bottom: 1px solid var(--line);
    background: var(--panel-2);
  }
  .strip.on { background: none; }
  textarea { width: 100%; resize: vertical; }
  /* same look as the fold around the plan's own diagram (WorkflowView's
     `.dag-details`): no button chrome, just a hand cursor and a quiet
     brightening on hover, so the two read as one convention. */
  .peek-toggle summary {
    cursor: pointer;
    width: fit-content;
    user-select: none;
  }
  .peek-toggle summary:hover { color: var(--text); }
  /* a peek, not the sheet: it scrolls sideways rather than widening the card */
  .peek { overflow-x: auto; }
  .peek th { font-weight: normal; color: var(--muted); text-align: left; padding-right: 10px; }
  .peek td { padding-right: 10px; max-width: 18em; }
  .problems { margin: 0; padding-left: 18px; color: var(--warn); }
  /* a file input is unstyleable, so the label is the button and the input hides
     inside it -- clicking the label is what opens the picker */
  .filebtn {
    display: inline-flex;
    align-items: center;
    padding: 2px 8px;
    border: 1px solid var(--line);
    border-radius: var(--radius);
    cursor: pointer;
  }
  .filebtn:hover { background: var(--panel-2); }
  .filebtn input { display: none; }
</style>
