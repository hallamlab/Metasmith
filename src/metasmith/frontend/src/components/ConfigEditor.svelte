<script>
  // A textarea with transparent text sitting exactly on top of a highlighted
  // copy of the same content. The textarea keeps every native behaviour --
  // caret, selection, undo, IME, spellcheck off -- and the layer underneath only
  // paints. The two stay aligned because they share font, padding and wrapping,
  // and the underlay is scrolled to match on every scroll event.
  //
  // Two grammars, both line-at-a-time and neither a parser. ssh_config's is
  // small enough not to need one: a line is a comment, or a keyword and a
  // value, and `Host`/`Match` open a block. Bash's is not, but the box it is
  // used in holds setup commands -- `module load`, `export`, the occasional
  // `if` -- so a scanner that knows comments, strings, variables, the control
  // words and what sits in command position covers what is actually typed
  // there. A construct that spans lines (a heredoc, a quote left open) is
  // painted per line and will look wrong; that is the price of no parser, and
  // it is paid in colour, never in what is saved.

  let {
    value = $bindable(''),
    rows = 14,
    resizable = true,
    spellcheck = false,
    readonly = false,
    label = null,
    language = 'ssh',
  } = $props()

  let ta
  let underlay

  const MARKER = /^#\s*(>>>|<<<)\s*metasmith managed hosts/
  const BLOCK_KEYWORDS = new Set(['host', 'match'])

  function escapeHtml(s) {
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  }

  function tag(cls, text) {
    return `<span class="${cls}">${escapeHtml(text)}</span>`
  }

  // -- bash ------------------------------------------------------------------

  // the words that structure a script, as opposed to the ones that run something
  const BASH_KEYWORDS = new Set([
    'if', 'then', 'elif', 'else', 'fi', 'for', 'in', 'do', 'done', 'while',
    'until', 'case', 'esac', 'function', 'select', 'return', 'break', 'continue',
    'local', 'export', 'declare', 'readonly', 'source', 'eval', 'exec', 'set',
    'unset', 'shift', 'trap', 'alias', 'time',
  ])

  // after one of these the next word starts something again -- either a command
  // (`then ls`) or a name being assigned (`export FOO=bar`) -- rather than
  // being an argument to the keyword itself
  const OPENS_COMMAND = new Set([
    'then', 'else', 'do', 'in', 'function', 'time', 'export', 'local',
    'declare', 'readonly', 'unset', 'eval', 'exec', 'source',
  ])

  const BASH_TOKEN = new RegExp(
    [
      "(?<comment>#.*$)",
      // an unterminated quote is still a string to the end of the line: it is
      // what the character means, and refusing to colour it would flag a
      // heredoc body as ordinary code
      "(?<string>'[^']*'?|\"(?:\\\\.|[^\"\\\\])*\"?)",
      "(?<variable>\\$\\{[^}]*\\}?|\\$[A-Za-z_]\\w*|\\$[-@*#?$!0-9])",
      "(?<word>[A-Za-z_][\\w./+-]*)",
      "(?<op>\\|\\||&&|[|&;()<>]+)",
    ].join('|'),
    'g',
  )

  function highlightBash(line) {
    if (!line.trim()) return ''
    // the shebang is not a comment in the way the rest are: it says what the
    // box is, and it is the one line that is there before anything is typed
    if (line.startsWith('#!')) return tag('t-marker', line)

    let out = ''
    let last = 0
    // a word here runs something; a word after it is an argument to it
    let atCommand = true
    for (const m of line.matchAll(BASH_TOKEN)) {
      const g = m.groups
      out += escapeHtml(line.slice(last, m.index))
      last = m.index + m[0].length
      if (g.comment !== undefined) {
        out += tag('t-comment', g.comment)
      } else if (g.string !== undefined) {
        out += tag('t-value', g.string)
        atCommand = false
      } else if (g.variable !== undefined) {
        out += tag('t-var', g.variable)
        atCommand = false
      } else if (g.op !== undefined) {
        out += tag('t-op', g.op)
        atCommand = true
      } else {
        const word = g.word
        if (BASH_KEYWORDS.has(word)) {
          out += tag('t-block', word)
          atCommand = OPENS_COMMAND.has(word)
        } else if (line[last] === '=' && atCommand) {
          // FOO=bar in command position is an assignment, not a call
          out += tag('t-key', word)
        } else {
          out += tag(atCommand ? 't-cmd' : 't-value', word)
          atCommand = false
        }
      }
    }
    return out + escapeHtml(line.slice(last))
  }

  // -- ssh_config ------------------------------------------------------------

  function highlightSsh(line) {
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

  // -- plain -------------------------------------------------------------
  // No grammar at all -- used for content this box has no vocabulary for
  // (a Nextflow config's Groovy), where colouring wrong would mislead more
  // than colouring nothing.
  function highlightPlain(line) {
    return escapeHtml(line)
  }

  // The trailing newline matters: without it the underlay is one line shorter
  // than the textarea and the last line drifts as you scroll to the bottom.
  let highlight = $derived(
    language === 'bash' ? highlightBash : language === 'plain' ? highlightPlain : highlightSsh,
  )
  let html = $derived(value.split('\n').map(highlight).join('\n') + '\n')

  function syncScroll() {
    if (!underlay || !ta) return
    underlay.scrollTop = ta.scrollTop
    underlay.scrollLeft = ta.scrollLeft
  }

  // Scroll chaining to a page that scrolls, not just to the nearest box: the
  // browser hands a wheel gesture to whichever scrollable box is exhausted
  // first, which on a trackpad's momentum tail can mean several frames spent
  // stuck against this box's own limit before the page picks the rest up --
  // read as the two fighting each other. Forwarding the remainder by hand,
  // frame by frame, the moment this box has nowhere left to go removes the gap.
  function findScrollParent(el) {
    let node = el?.parentElement
    while (node) {
      const style = getComputedStyle(node)
      if (/(auto|scroll)/.test(style.overflowY) && node.scrollHeight > node.clientHeight) return node
      node = node.parentElement
    }
    return null
  }

  // The ancestor chain is stable for as long as this box stays mounted, so the
  // walk above only needs to happen once -- redoing it (with a getComputedStyle
  // per ancestor) on every wheel tick is what made the forwarded scroll choppy:
  // short content pins the textarea at both boundaries at once, so *every*
  // event on it took the expensive path.
  let scrollParent

  function onWheel(e) {
    if (!ta) return
    const atTop = ta.scrollTop <= 0
    const atBottom = ta.scrollTop + ta.clientHeight >= ta.scrollHeight - 1
    if ((e.deltaY < 0 && !atTop) || (e.deltaY > 0 && !atBottom)) return
    if (scrollParent === undefined) scrollParent = findScrollParent(ta)
    if (!scrollParent) return
    e.preventDefault()
    scrollParent.scrollTop += e.deltaY
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
    onwheel={onWheel}
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
    background: var(--sunken);
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
  .editor textarea::selection { background: var(--select); color: transparent; }
  .editor.fixed textarea { resize: none; }

  .underlay :global(.t-comment) { color: var(--muted); font-style: italic; }
  .underlay :global(.t-marker) { color: var(--ok); }
  .underlay :global(.t-block) { color: var(--accent); font-weight: 600; }
  .underlay :global(.t-key) { color: var(--code-key); }
  .underlay :global(.t-value) { color: var(--text); }
  /* bash only: what is being run, a variable, and the plumbing between them */
  .underlay :global(.t-cmd) { color: var(--code-cmd); }
  .underlay :global(.t-var) { color: var(--code-var); }
  .underlay :global(.t-op) { color: var(--muted); }
</style>
