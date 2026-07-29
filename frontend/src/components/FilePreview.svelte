<script>
  // One file, previewed without ever reading it whole.
  //
  // The rule underneath every branch below is the same: a preview is a 256 KB
  // byte *window*, and the server never holds more than one. Bioinformatic
  // outputs are routinely tens of gigabytes, so "just show the file" is not a
  // policy that survives contact with the data this page exists to look at.
  //
  // What varies is only how a window is drawn: an image comes off the streaming
  // route whole (they are small, and half a PNG is not a picture), a table is
  // parsed out of the window's first rows, and anything that decodes as text is
  // shown as text with head/tail and paging. Everything else says what it is
  // and offers the file.
  import { api } from '../lib/api.svelte.js'
  import { bytes } from '../lib/format.js'

  let { workflow, run, node } = $props()

  const IMAGE = /\.(png|jpe?g|gif|webp|svg)$/i
  const TABLE = /\.(csv|tsv)$/i
  const MARKUP = /\.(html?|xml)$/i
  // an image that is click-to-load rather than shown: a heatmap is never this
  // big, and something that is has a reason to be
  const INLINE_MAX = 8 * 1024 * 1024
  const TABLE_ROWS = 500

  let win = $state(null)      // the current window
  let text = $state('')       // everything accumulated by paging
  let mode = $state('head')
  let busy = $state(false)
  let error = $state(null)
  let showBig = $state(false)

  let path = $derived(node?.path ?? null)
  let kind = $derived.by(() => {
    if (!node) return null
    if (IMAGE.test(node.name)) return 'image'
    if (MARKUP.test(node.name)) return 'markup'
    if (win?.encoding === 'binary') return 'binary'
    if (TABLE.test(node.name)) return 'table'
    return 'text'
  })

  const src = (p) =>
    `/api/runs/${workflow}/${run}/download?path=${encodeURIComponent(p)}`

  async function fetchWindow(opts = {}) {
    busy = true
    error = null
    try {
      const q = new URLSearchParams({ path, mode: opts.mode ?? mode })
      if (opts.offset != null) q.set('offset', String(opts.offset))
      const r = await api.get(`/runs/${workflow}/${run}/file?${q}`)
      win = r
      text = opts.append ? text + (r.text ?? '') : (r.text ?? '')
    } catch (e) {
      error = e.message
      win = null
      text = ''
    }
    busy = false
  }

  $effect(() => {
    const p = path
    win = null
    text = ''
    mode = 'head'
    error = null
    showBig = false
    // An image or a markup file is never windowed: one is fetched by the
    // browser and the other is not previewed at all.
    if (p && !IMAGE.test(node.name) && !MARKUP.test(node.name)) fetchWindow({ mode: 'head' })
  })

  // Paging is by byte offset, never by counting the lines already shown: the
  // window's ends are trimmed at line boundaries for looks, so the count of
  // lines and the count of bytes are deliberately not the same number.
  let more = $derived(
    win && !win.eof && mode === 'head' && text.length < (win.max_total ?? Infinity),
  )

  async function loadMore() {
    await fetchWindow({ mode: 'head', offset: win.offset + win.length, append: true })
  }

  async function toEnd() {
    mode = 'tail'
    await fetchWindow({ mode: 'tail' })
  }

  async function toStart() {
    mode = 'head'
    await fetchWindow({ mode: 'head', offset: 0 })
  }

  let rows = $derived.by(() => {
    if (kind !== 'table' || !text) return null
    const sep = node.name.toLowerCase().endsWith('.tsv') ? '\t' : ','
    const lines = text.split('\n').filter((l) => l.length)
    return {
      head: lines[0]?.split(sep) ?? [],
      body: lines.slice(1, TABLE_ROWS + 1).map((l) => l.split(sep)),
      shown: Math.min(lines.length - 1, TABLE_ROWS),
    }
  })
</script>

{#if !node}
  <p class="small muted">Pick a file to look inside it.</p>
{:else}
  <div class="col" style="gap:10px">
    <div class="col" style="gap:2px">
      <div class="mono small truncate" title={node.path}>{node.path}</div>
      <div class="small muted">
        {bytes(node.size)}
        {#if node.type_name}· <span class="mono">{node.type_name}</span>{/if}
      </div>
    </div>

    {#if node.dangling}
      <p class="small bad">
        This is a link whose data is gone on the agent. It came across as a
        pointer to nothing, so there is nothing to show.
      </p>
    {:else if error}
      <p class="small bad">{error}</p>
    {:else if kind === 'image'}
      {#if node.size > INLINE_MAX && !showBig}
        <button onclick={() => (showBig = true)}>show image ({bytes(node.size)})</button>
      {:else}
        <!-- through `<img>`, never inlined markup: an SVG a pipeline wrote is
             not markup this page should execute -->
        <img class="preview" src={src(node.path)} alt={node.name} />
      {/if}
    {:else if kind === 'markup'}
      <p class="small muted">
        This is a document with its own scripts and styles — nextflow's report is
        one — so it is not rendered inside the page. Open it as a file instead.
      </p>
    {:else if kind === 'binary'}
      <p class="small muted">
        Not text. Nothing is guessed about it beyond that; the file itself is
        below.
      </p>
    {:else if busy && !text}
      <p class="small muted">reading…</p>
    {:else if kind === 'table' && rows}
      <div class="tablebox">
        <table class="small">
          <thead><tr>{#each rows.head as h}<th>{h}</th>{/each}</tr></thead>
          <tbody>
            {#each rows.body as r}
              <tr>{#each r as c}<td class="mono">{c}</td>{/each}</tr>
            {/each}
          </tbody>
        </table>
      </div>
      <p class="small muted">
        {rows.shown} row{rows.shown === 1 ? '' : 's'} from the first
        {bytes(win?.length)} of {bytes(node.size)}.
      </p>
    {:else}
      <pre class="log">{text || 'empty'}</pre>
    {/if}

    {#if kind === 'text' || kind === 'table'}
      <div class="row" style="gap:6px; flex-wrap:wrap">
        {#if mode === 'tail'}
          <button class="small" onclick={toStart} disabled={busy}>back to start</button>
        {:else if more}
          <button class="small" onclick={loadMore} disabled={busy}>load more</button>
        {/if}
        {#if win && !win.eof}
          <button class="small" onclick={toEnd} disabled={busy || mode === 'tail'}>
            jump to end
          </button>
        {/if}
      </div>
      {#if win}
        <p class="small muted">
          {#if win.eof && win.offset === 0}
            the whole file
          {:else if mode === 'tail'}
            the last {bytes(win.length)} of {bytes(win.size)}
          {:else}
            {bytes(Math.min(win.offset + win.length, win.size))} of {bytes(win.size)}
            {#if !more && !win.eof}· that is as far as the page will hold; jump to
              the end to see the rest{/if}
          {/if}
        </p>
      {/if}
    {/if}

    <div><a class="dl" href={src(node.path)} download>download</a></div>
  </div>
{/if}

<style>
  .preview {
    max-width: 100%;
    height: auto;
    border-radius: 6px;
    background: var(--panel-2);
  }
  .tablebox { overflow: auto; max-height: 50vh; }
  .tablebox td { white-space: nowrap; }
  .bad { color: var(--bad); }
  .dl { font-size: 12px; color: var(--accent); }
</style>
