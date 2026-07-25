<script>
  // A textarea with transparent text sitting exactly on top of a highlighted
  // copy of the same content. The textarea keeps every native behaviour --
  // caret, selection, undo, IME, spellcheck off -- and the layer underneath only
  // paints. The two stay aligned because they share font, padding and wrapping,
  // and the underlay is scrolled to match on every scroll event.
  //
  // ssh_config's grammar is small enough that this needs no parser: a line is a
  // comment, or a keyword and a value, and `Host`/`Match` open a block.

  let {
    value = $bindable(''),
    rows = 14,
    resizable = true,
    spellcheck = false,
    readonly = false,
    label = null,
  } = $props()

  let ta
  let underlay

  const MARKER = /^#\s*(>>>|<<<)\s*metasmith managed hosts/
  const BLOCK_KEYWORDS = new Set(['host', 'match'])

  function escapeHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  }

  function highlightLine(line) {
    if (!line.trim()) return ''
    if (MARKER.test(line.trim())) return `<span class="t-marker">${escapeHtml(line)}</span>`
    if (line.trimStart().startsWith('#')) return `<span class="t-comment">${escapeHtml(line)}</span>`

    // indent, keyword, separator (whitespace and/or '='), value
    const m = line.match(/^(\s*)([A-Za-z][A-Za-z0-9_-]*)([\s=]+)?(.*)$/)
    if (!m) return escapeHtml(line)
    const [, indent, keyword, sep = '', rest] = m
    const cls = BLOCK_KEYWORDS.has(keyword.toLowerCase()) ? 't-block' : 't-key'
    let out = `${indent}<span class="${cls}">${escapeHtml(keyword)}</span>${escapeHtml(sep)}`
    if (rest) out += `<span class="t-value">${escapeHtml(rest)}</span>`
    return out
  }

  // The trailing newline matters: without it the underlay is one line shorter
  // than the textarea and the last line drifts as you scroll to the bottom.
  let html = $derived(value.split('\n').map(highlightLine).join('\n') + '\n')

  function syncScroll() {
    if (!underlay || !ta) return
    underlay.scrollTop = ta.scrollTop
    underlay.scrollLeft = ta.scrollLeft
  }

  // Tab belongs to the document here, not to the focus ring: this is an editor,
  // and ssh_config is conventionally indented.
  function onKeydown(e) {
    if (e.key !== 'Tab' || e.shiftKey || readonly) return
    e.preventDefault()
    const { selectionStart: a, selectionEnd: b } = ta
    value = value.slice(0, a) + '    ' + value.slice(b)
    requestAnimationFrame(() => ta.setSelectionRange(a + 4, a + 4))
  }
</script>

<div class="editor" class:fixed={!resizable} style="--rows: {rows}">
  <pre class="underlay" bind:this={underlay} aria-hidden="true">{@html html}</pre>
  <textarea
    bind:this={ta}
    bind:value
    {rows}
    {spellcheck}
    {readonly}
    aria-label={label}
    autocapitalize="off"
    autocomplete="off"
    autocorrect="off"
    onscroll={syncScroll}
    onkeydown={onKeydown}
  ></textarea>
</div>

<style>
  /* every one of these is shared by both layers; changing one alone
     de-registers the caret from the text under it */
  .editor {
    --pad: 10px;
    --lh: 1.5;
    position: relative;
    font-family: var(--mono);
    font-size: 12.5px;
    line-height: var(--lh);
    border: 1px solid var(--line);
    border-radius: var(--radius);
    background: #0e1013;
    overflow: hidden;
  }
  .editor:focus-within { outline: 1px solid var(--accent); }

  .underlay,
  .editor textarea {
    margin: 0;
    padding: var(--pad);
    border: 0;
    font: inherit;
    line-height: inherit;
    white-space: pre;
    overflow: auto;
    tab-size: 4;
  }

  .underlay {
    position: absolute;
    inset: 0;
    pointer-events: none;
    color: var(--text);
    overflow: hidden;
  }

  .editor textarea {
    position: relative;
    display: block;
    width: 100%;
    height: calc(var(--rows) * var(--lh) * 1em + 2 * var(--pad));
    background: transparent;
    /* the glyphs are painted by the underlay; only the caret and the selection
       come from the textarea itself */
    color: transparent;
    caret-color: var(--text);
    resize: vertical;
    border-radius: 0;
  }
  .editor textarea:focus { outline: none; }
  .editor textarea::selection { background: #2b4a7d; color: transparent; }
  .editor.fixed textarea { resize: none; }

  .underlay :global(.t-comment) { color: var(--muted); font-style: italic; }
  .underlay :global(.t-marker) { color: var(--ok); }
  .underlay :global(.t-block) { color: var(--accent); font-weight: 600; }
  .underlay :global(.t-key) { color: #c39be0; }
  .underlay :global(.t-value) { color: var(--text); }
</style>
