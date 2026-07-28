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
    busy = false,
    onattach, // (File|null, text|null) => Promise
    ondetach,
    onexpand,
    onclear,
  } = $props()

  let pasting = $state(false)
  let text = $state('')
  let peeking = $state(false)

  let attached = $derived(!!table?.attached)
  let problems = $derived(table?.problems ?? [])
  let expanded = $derived(table?.expansion?.row_count ?? 0)
  let stale = $derived(attached && expanded > 0 && table.expansion.stale)

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
        Have a sample sheet? Attach it and each row below can name a column
        instead of a file — one run per row.
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
      <span class="mono truncate grow" title={table.filename}>{table.filename}</span>
      <span class="small muted">{table.row_count} row(s) · {table.columns.length} column(s)</span>
      <button class="small" onclick={() => (peeking = !peeking)}>
        {peeking ? 'hide' : 'peek'}
      </button>
      <DeleteControl title="take the sheet away — what it registered stays" onconfirm={ondetach} />
    </div>

    <div class="row wrap cols">
      {#each table.columns as c}<span class="tag mono">{c}</span>{/each}
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

    <div class="row wrap">
      <button class="small" onclick={onexpand} disabled={busy || problems.length > 0}>
        {expanded ? 'expand again' : 'expand'}
      </button>
      {#if expanded}
        <button class="small" onclick={onclear} disabled={busy}>unregister</button>
        <span class="small muted">{expanded} row(s) registered</span>
      {/if}
      {#if stale}
        <span class="tag warn">the sheet changed since this was expanded</span>
      {/if}
    </div>

    {#if problems.length}
      <ul class="small problems">
        {#each problems as p}<li>{p.message}</li>{/each}
      </ul>
    {:else if !expanded}
      <span class="small muted">
        Ready — expanding registers one item per templated row, per sheet row.
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
  .cols { gap: 4px; }
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
