<script>
  import DeleteControl from '../components/DeleteControl.svelte'
  import LineageBand from '../components/LineageBand.svelte'
  import ParentPicker from '../components/ParentPicker.svelte'
  import TypeSelect from '../components/TypeSelect.svelte'
  import { POINTED, link, marks } from '../lib/highlight.js'
  import { ancestorsOf, byKey, candidates, refKey, topoOrder } from '../lib/lineage.js'
  import { entries as rowEntries, isBound } from '../lib/rows.js'

  // Inputs and outputs in one list, and the list *is* the form. They are two
  // headings over one run of rows, the way the ssh rail does managed and native
  // -- the rows stay siblings, and a heading is a label rather than a parent.
  //
  // There used to be a builder card below this one: a form you filled in, then
  // clicked add, then looked at the row that appeared somewhere else. Now the
  // add button makes an empty row here and you fill that in.
  //
  // There is one kind of input row and it never becomes another. A row lives in
  // the request beside the outputs, which have always been request-only; the
  // input library is built from the rows when the workflow is solved. There is
  // therefore nothing to commit, nothing that changes shape under the cursor,
  // and every field on every row is editable for as long as the row exists.
  //
  // An output row is an input row with its path line taken off. That is not a
  // coincidence to be re-derived in two places: both render the same snippet,
  // so the type field and the parents cannot drift apart by a column.
  let {
    rows = [],
    // what the last solve made of them -- a readout, not something this card
    // edits. All it is wanted for is the count an array row stands for.
    items = [],
    targets = [],
    typeOptions = [],
    // the attached sheet: its column names and what the last expansion
    // registered per row
    columns = [],
    rowCount = 0,
    rowUniques = {},
    expansion = null,
    sharedPaths = [],
    // the sheet's own strip, rendered under the inputs band by the view above --
    // it belongs inside this box but it is not this card's business
    tableStrip = null,
    onshared,
    // (type) => {known, produced, consumed, producedVia, consumedVia} -- what
    // the index says about a type, for the line under a row being edited
    counts = null,
    onfocus,
    // reading around a type list, and stopping: what the panel should show
    // while a list is open, and the signal that it is no longer what was asked
    onpreview,
    onpreviewend,
    onremoveRow,
    onremoveTarget,
    onrow,
    ontarget,
    onparents,
    oncommit,
    onadd,
  } = $props()

  // `entries` and `isBound` are imported (see `lib/rows.js`) rather than
  // written here: the workflow view asks the same questions, and a row that
  // reads one way in one place and another in the other is a disagreement
  // nothing on the page can show.
  //
  // A sheet attached is the only switch. With one, every field on every row is
  // a strict choice from that sheet's columns and there is no way to type into
  // it; with none, every field is free text. Each keeps its own answer, so the
  // two states are two fields on the row rather than one field being rewritten
  // -- which is the whole of the switching behaviour, with nothing to save on a
  // transition. A value entry's *key* is the exception and stays a text box in
  // both states: it names the field in the object the row writes.
  let hasTable = $derived(columns.length > 0)

  // What the sheet registered is not rows of this recipe. Those items are in the
  // library and in the plan, and two hundred of them here would be two hundred
  // rows with nothing on them to decide -- the array row carries the count.
  let expanded = $derived(items.filter((it) => it.array_id).length)

  // The entries of a value row, and the one place they are written back. A row
  // holds a list of them, so every edit to one is an edit to the whole list --
  // which is what `apply` below hands `sampleField`, in place of the single
  // field name it used to close over.
  const patchEntry = (row, i, patch) => {
    const next = rowEntries(row.row).map((e, k) => (k === i ? { ...e, ...patch } : e))
    onrow?.(row.id, { values: next })
  }
  const addEntry = (row) => {
    onrow?.(row.id, { values: [...rowEntries(row.row), { key: '', value: '', column: '' }] })
    oncommit?.()
  }
  const dropEntry = (row, i) => {
    onrow?.(row.id, { values: rowEntries(row.row).filter((_e, k) => k !== i) })
    oncommit?.()
  }

  // Both halves are the same shape -- `{key, parents: [key]}` over a minted id --
  // so `lib/lineage` answers both from one implementation. An output used to be
  // addressed by its *position*, which is also the thing that changes when the
  // list is re-sorted; it carries an id of its own now, and the positions are
  // put back on the way to disk.
  let inputRows = $derived(
    rows.map((d) => ({
      kind: 'input',
      key: refKey(d.id),
      id: d.id,
      type: d.dtype,
      parents: d.parents ?? [],
      row: d,
    })),
  )

  // An output is named by its *type* and by nothing else. Two outputs of one
  // type used to carry a `#N` here counting the order they are drawn in; what
  // tells them apart now is the highlight that lights the row you are pointing
  // at, which is the one thing an ordinal never did -- you still had to count
  // rows to use it.
  let targetRows = $derived.by(() =>
    topoOrder(
      targets.map((t) => ({ key: refKey(t.id), parents: (t.parents ?? []).map(refKey), t })),
    ).map((r) => ({
      kind: 'target',
      key: r.key,
      id: r.t.id,
      type: r.t.type,
      parents: r.parents,
    })),
  )

  let orderedInputRows = $derived(topoOrder(inputRows))

  let inputByKey = $derived(byKey(inputRows))
  let targetByKey = $derived(byKey(targetRows))

  // computed once per half rather than once per open menu: `candidates` would
  // otherwise rebuild the fixpoint for every row on every render
  let inputAncestors = $derived(ancestorsOf(inputRows))
  let targetAncestors = $derived(ancestorsOf(targetRows))

  // What a row may be given as a parent. The rule itself is shared
  // (`lib/lineage`), because "can this be a parent" has one answer and the two
  // halves having had two is how the outputs came to refuse links that were
  // perfectly legal -- and now the *wording* is shared too, since there is
  // nothing here to word: an option is a row's key and its type, whichever half
  // is asking.
  const optionsFor = (rows, row, anc) =>
    candidates(rows, row.key, anc).map((r) => ({ key: r.key, type: r.type }))

  // Why the menu is empty, when it is. Two honest cases and they are not the
  // same: there is nothing else in this half yet, or everything else in it
  // already descends from this row.
  const parentNote = (rows, options) =>
    options.length
      ? null
      : rows.length < 2
        ? 'nothing else here to descend from yet'
        : 'everything else here already descends from this'

  // A parent whose row has since gone still has to be shown, or an entry would
  // sit in a lineage nothing on the page admits to. The path a parent chip used
  // to lead with is not unique enough to tell rows apart either -- the hover
  // highlight on the row itself already does that -- so the chip states only
  // what the parent *is*, under the same field name an option carries.
  function chosenFor(row, index) {
    return row.parents.map((k) => ({ key: k, type: index.get(k)?.type ?? null }))
  }

  // Which *link* a parent line is. A type name is what a chip and a menu entry
  // both state, and it is shared by every row of that type -- so finding the
  // row by eye is the highlight's job: hovering the line marks the two rows it
  // joins.
  //
  // The link and not just the parent: marking the parent alone left the reader
  // to remember which row they were pointing from, and marking everything
  // downstream of the parent (which is what a node-keyed highlight does to a
  // rail) answered a question nobody asked.
  let hover = $state(null) // {child, parent} | null

  // ... as the roles `DagRail` paints. `link` and nothing around it: this is the
  // one frame with no `related` tier at all, because a parent's *other* children
  // are not what the chip is pointing at. Both bands are handed the one map, and
  // each paints its rail and its rows' backgrounds off it, so the two cannot
  // disagree about what is marked -- which is also why the halves' two id spaces
  // have to stay disjoint.
  let hlMarks = $derived(
    hover ? marks({ role: POINTED, ...link(hover.parent, hover.child) }) : null,
  )

  const setType = (row, v) =>
    row.kind === 'target' ? ontarget?.(row.id, { type: v }) : onrow?.(row.id, { dtype: v })

</script>

<!-- The type is always the field, never a word standing in for it -- an input
     and an output name a type through the same combobox the same way. Focusing
     it is what moves the panel onto that type, in place of the click that used
     to open it.

     The focus-out is what saves it. A combobox is a box plus a caret button, so
     "left the field" is focus leaving the whole control rather than the input
     inside it -- tabbing from the box to the caret is not leaving. Without this
     a type typed and tabbed away from persisted nothing until some other field
     happened to blur.

     Focusing the row and picking a type are two moves and both aim the panel:
     the first says "this row", and no second `focusin` fires after a pick, so
     without the second the panel would still be showing whatever the row held
     before. A field blurred mid-type without a pick deliberately moves nothing
     -- the persist below is all that happens -- so the panel never chases half
     a name. -->
{#snippet typeCell(row)}
  <div
    class="typefield"
    onfocusin={() => onfocus?.(row.type)}
    onfocusout={(e) => {
      if (!e.currentTarget.contains(e.relatedTarget)) oncommit?.()
    }}
  >
    <TypeSelect
      value={row.type ?? ''}
      options={typeOptions}
      placeholder="namespace::type"
      onchange={(v) => setType(row, v)}
      oncommit={(picked) => {
        // `onfocus` aims the panel, `oncommit` writes the recipe to disk --
        // two props one line apart, and only the name of the parameter says
        // which value belongs to which
        onfocus?.(picked)
        oncommit?.()
      }}
      onpreview={(t) => onpreview?.(t)}
      onclose={() => onpreviewend?.()}
    />
  </div>
{/snippet}

<!-- What a row *is* and what it came from. This is the whole of an output row
     and the second line of an input one -- one snippet, so the two cannot drift
     apart by a column. The trailing cell is fixed width whether or not it holds
     anything, which is what keeps the delete on an input's first line over the
     delete on an output. -->
{#snippet detail(row)}
  {@const isTarget = row.kind === 'target'}
  {@const peers = isTarget ? targetRows : inputRows}
  {@const options = optionsFor(peers, row, isTarget ? targetAncestors : inputAncestors)}
  <div class="row-item detail">
    <div class="typecell">{@render typeCell(row)}</div>
    <div class="parentcell">
      <ParentPicker
        self={row.key}
        chosen={chosenFor(row, isTarget ? targetByKey : inputByKey)}
        {options}
        note={parentNote(peers, options)}
        onadd={(k) => onparents?.(row, [...row.parents, k])}
        onremove={(k) => onparents?.(row, row.parents.filter((x) => x !== k))}
        onhover={(link) => (hover = link)}
      />
    </div>
    <span class="trail">
      {#if isTarget}
        <DeleteControl title="stop wanting this" onconfirm={() => onremoveTarget?.(row.id)} />
      {/if}
    </span>
  </div>
{/snippet}

<!-- One field, two states, and the sheet decides which. With columns it is a
     strict choice from them -- there is nothing to type, because the sheet
     carries the finished value and metasmith never builds a string out of one.
     Without, it is the free text it always was.

     The two are two separate stores on the row, so neither write touches the
     other: switching a sheet on and off moves between the last answer given to
     each. A binding naming a column *this* sheet lacks draws blank and is left
     alone, so re-attaching the sheet it came from restores it.

     `field` is only a name, for the label an assistive reader gets. What a
     write means is the caller's business: a path sets one key on the row, a
     value entry rewrites the whole list it is a member of. -->
{#snippet sampleField(row, field, value, column, placeholder, mono, apply, bind)}
  {#if hasTable}
    <select
      class="grow{mono ? ' mono' : ''}"
      class:unset={!column}
      aria-label={`${field}, a column of the attached sheet`}
      value={columns.includes(column) ? column : ''}
      onchange={(e) => {
        bind(e.currentTarget.value)
        // a select has no blur-after-typing to save on, so the change *is* the
        // commit -- without this every choice would sit unpersisted until some
        // other field happened to blur
        oncommit?.()
      }}
    >
      <option value="">choose a column…</option>
      {#each columns as c}<option value={c}>{c}</option>{/each}
    </select>
  {:else}
    <input
      class="grow{mono ? ' mono' : ''}"
      {value}
      {placeholder}
      spellcheck="false"
      oninput={(e) => apply(e.currentTarget.value)}
      onblur={() => oncommit?.()}
    />
  {/if}
{/snippet}

<!-- One field of a value row: its key, and what it holds. The key is literal in
     both states and never a column -- so the grouping key, what fans out and
     the validation messages all read one set of fields, and a key that happens
     to match a column name means nothing. With one entry the key is optional
     and says so; with two or more it is what the field is called in the object
     the row writes, and the server refuses a launch off a recipe where one is
     blank. -->
{#snippet valueEntry(row, ents, i)}
  {@const e = ents[i]}
  {@const only = ents.length === 1}
  <input
    class="keybox"
    value={e.key}
    placeholder={only ? 'key (optional)' : 'key'}
    aria-label="the key this field is written under"
    spellcheck="false"
    oninput={(ev) => patchEntry(row, i, { key: ev.currentTarget.value })}
    onblur={() => oncommit?.()}
  />
  {@render sampleField(
    row,
    `values.${i}`,
    e.value,
    e.column,
    'GCF_000005845.2',
    false,
    (text) => patchEntry(row, i, { value: text }),
    (col) => patchEntry(row, i, { column: col }),
  )}
{/snippet}

<!-- What an input row holds -- a path or a literal value -- is a switch on the
     row, not a choice made once when it was added. Two labelled halves, one of
     them lit: a slider reads as one thing to flip, and a flip is exactly what
     changing which fields the row shows underneath it is. -->
{#snippet modeSwitch(row)}
  <div class="modeswitch" role="group" aria-label="a path or a value">
    <button
      type="button"
      class:on={row.row.mode !== 'value'}
      title="a path on disk"
      onclick={() => onrow?.(row.id, { mode: 'file' })}
    >file</button>
    <button
      type="button"
      class:on={row.row.mode === 'value'}
      title="a literal value"
      onclick={() => onrow?.(row.id, { mode: 'value' })}
    >value</button>
  </div>
{/snippet}

<div class="col" style="gap:10px">
  <h3>recipe</h3>

  <div class="rows">
    {#if tableStrip}
      <div class="heading samples small muted spread">
        <span>samples</span>
        <span class="count">
          {columns.length ? `${rowCount} row(s) · ${columns.length} column(s)` : 'none attached'}
        </span>
      </div>
      {@render tableStrip()}
    {/if}

    <div class="heading in small muted spread">
      <span>inputs</span>
      <span class="count">
        {rows.length} row(s){expanded ? ` · ${expanded} from the sheet` : ''}
      </span>
    </div>
    {#if inputRows.length === 0}
      <p class="small muted pad">
        Nothing here yet. Add the files and values you have — the planner works
        out the steps from their types alone.
      </p>
    {:else}
      <LineageBand rows={orderedInputRows} marks={hlMarks}>
        {#snippet body(row)}
          {@const info = row.type && counts ? counts(row.type) : null}
          {@const bound = isBound(row.row)}
          <!-- Two lines, not one: the path is the longest thing on an input row and
               was being squeezed into a sliver beside a combobox and a menu. What
               the row points at goes on the first line; what it *is* and what it
               came from go on the second -- and that second line is the whole of an
               output row. -->
          {#if row.row.mode === 'value'}
            {@const ents = rowEntries(row.row)}
            <!-- A value row holds a list, one line per entry. One entry is
                 what a value row has always been -- a box, with an optional
                 key beside it -- and it stays on the switch's own line so
                 the common row does not grow. Give it a key, or a second
                 entry, and the row writes the JSON object those pairs
                 describe instead of the text: which is the point, since
                 hand-typed JSON in that box has braces in it and braces are
                 what make a row a sample array.

                 The add sits hard left, against the switch, on the switch's
                 own line: a second entry pushes the fields down under it
                 rather than moving the button that made them, and it is
                 nowhere near the deletes on the right. -->
            <div class="row-item">
              {@render modeSwitch(row)}
              <button class="star" title="another field, under its own key" onclick={() => addEntry(row)}>+</button>
              {#if ents.length === 1}
                {@render valueEntry(row, ents, 0)}
              {:else}
                <span class="grow"></span>
              {/if}
              <span class="trail">
                <DeleteControl title="discard this row" onconfirm={() => onremoveRow?.(row.id)} />
              </span>
            </div>
            {#if ents.length > 1}
              {#each ents as _e, i}
                <div class="row-item entryline">
                  {@render valueEntry(row, ents, i)}
                  <span class="trail fieldtrail">
                    <DeleteControl title="discard this field" onconfirm={() => dropEntry(row, i)} />
                  </span>
                </div>
              {/each}
            {/if}
          {:else}
            <div class="row-item">
              {@render modeSwitch(row)}
              {@render sampleField(
                row,
                'path',
                row.row.path,
                row.row.column,
                '/data/sample_01.fastq.gz',
                true,
                (text) => onrow?.(row.id, { path: text }),
                (col) => onrow?.(row.id, { column: col }),
              )}
              <span class="trail">
                <DeleteControl title="discard this row" onconfirm={() => onremoveRow?.(row.id)} />
              </span>
            </div>
          {/if}

          {@render detail(row)}

          {#if hasTable}
            <!-- With a sheet attached this row is one declaration, not N rows:
                 what it says about itself is a count of how many items it
                 stands for. A field bound to nothing makes that count zero, and
                 saying so here is what keeps the row from going quiet -- it
                 registers nothing until it is bound. -->
            <div class="notes row wrap small">
              {#if !bound}
                <span class="tag warn">pick a column for every field — this registers nothing</span>
              {:else if expansion?.counts?.[row.id]}
                <span class="tag">× {expansion.counts[row.id]} registered</span>
              {:else}
                <span class="tag">× {rowUniques[row.id] ?? rowCount} once expanded</span>
              {/if}
              <!-- A sample's mask is one index item's lineage, so a reference
                   sitting beside the per-sample files is in no sample at all.
                   Marking it shared is the third way in, and it applies to any
                   row under a sheet: the usual shape is a column repeating one
                   path down every row, which groups onto the single instance
                   this then shares.

                   Keyed by the row, not by a path: a row may not have one yet,
                   and the generate turns the reference into every path that row
                   registered, between the sync that made them and the solve
                   that reads them. -->
              <button
                class="star"
                class:on={sharedPaths.includes(row.key)}
                aria-pressed={sharedPaths.includes(row.key)}
                title={sharedPaths.includes(row.key)
                  ? 'every sample sees this'
                  : 'let every sample see this — a reference beside the per-sample files is otherwise in no sample at all'}
                onclick={() => onshared?.(row.key, !sharedPaths.includes(row.key))}
              >shared by every sample</button>
            </div>
          {/if}

          {#if row.type && !info?.known}
            <div class="notes row wrap small">
              <span class="tag warn">not a type in this library</span>
            </div>
          {/if}
        {/snippet}
      </LineageBand>
    {/if}

    <div class="addrow">
      <!-- One button, not a choice up front: a path and a value are the same
           kind of thing to add, a row, and what it holds is a switch on the row
           itself rather than a different action to take here. -->
      <button class="small" onclick={() => onadd?.('input')}>+ an input</button>
    </div>

    <div class="heading out small muted spread">
      <span>outputs</span>
      <span class="count">{targets.length} wanted</span>
    </div>
    {#if targetRows.length === 0}
      <p class="small muted pad">Nothing wanted yet. Add at least one to solve.</p>
    {:else}
      <!-- a wanted output is a requested one: the engine's solid marker, the
           same one the plan's diagram draws it with -->
      <LineageBand rows={targetRows} kind="target" marks={hlMarks}>
        {#snippet body(row)}
          {@const info = row.type && counts ? counts(row.type) : null}
          {@const dup = targetRows.some(
            (o) =>
              o.id !== row.id &&
              o.type === row.type &&
              row.type &&
              JSON.stringify([...o.parents].sort()) === JSON.stringify([...row.parents].sort()),
          )}
          {@render detail(row)}

          {#if row.type || dup}
            <div class="notes row wrap small">
              {#if row.type && !info?.known}
                <span class="tag warn">not a type in this library</span>
              {:else if info?.known && info.produced === 0}
                <span class="muted">nothing can make this — the plan will not solve</span>
              {/if}
              {#if dup}
                <span class="tag warn">already wanted, with the same lineage</span>
              {/if}
            </div>
          {/if}
        {/snippet}
      </LineageBand>
    {/if}

    <div class="addrow">
      <button class="small" onclick={() => onadd?.('output')}>+ an output</button>
    </div>
  </div>
</div>

<style>
  .rows {
    border: 1px solid var(--line);
    border-radius: var(--radius);
    /* not hidden: a parent menu and a type list both hang out of their row */
    overflow: visible;
  }
  /* The two halves stay siblings in the one box, so the band is the only thing
     telling them apart -- it has to be loud enough to read as a division rather
     than as another row. An accent edge and the half's own count do that; a
     wrapper element around each half would do it too, and would also change
     what `:last-child` and `:first-child` mean two rules down. */
  .heading {
    padding: 6px 10px;
    background: var(--panel-2);
    border-bottom: 1px solid var(--line);
    box-shadow: inset 3px 0 0 var(--accent);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 11px;
  }
  /* a band midway down the box is a seam, not a label at its top -- which one
     that is shifts now that samples can lead, so it is structural rather than
     pinned to `.out` */
  .heading:not(:first-child) { border-top: 3px solid var(--line); }
  .heading .count { text-transform: none; letter-spacing: 0; }
  /* A row and everything about how it sits beside a rail is `LineageBand`'s,
     including the border under it -- Svelte scopes styles per component, so a
     rule here could not reach those rows anyway. What is left in this box is
     the two add-rows, which are this card's own elements and carry the same
     border so the run of rows reads as one list. The last of them closes the
     box, so it drops it. */
  .addrow { border-bottom: 1px solid var(--line); }
  .rows > .addrow:last-child { border-bottom: none; }
  .row-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
  }
  /* file and value are the same row seen two ways, so the row may not change
     height between them. A path is drawn in the mono face at 12.5px and a value
     in the proportional one at the page size, and a field left to size itself
     off its own font is a pixel or two taller in one mode than the other --
     which twitches the row, which redraws the lineage rail measured against it.
     Pinned here rather than globally: nothing else on the page swaps a field's
     face under the cursor. */
  .row-item input,
  .row-item select {
    height: 28px;
  }
  /* an input's own text sits inside a line box and wants the nudge; a select's
     does not -- its value is centered by the UA's own layout, and giving it
     the same line-height only pushes that centering past the fixed height
     above, clipping the bottom of the text. */
  .row-item input { line-height: 1.35; }
  /* the second line of an input row, and the whole of an output row: same
     columns, no gap above it, so an input's two lines read as one row */
  .row-item.detail { align-items: flex-start; padding-top: 0; }
  /* ...except on an output, where it is the first line rather than the second */
  .row-item.detail:first-child { padding-top: 6px; }
  .notes {
    gap: 6px;
    align-items: baseline;
    padding: 0 10px 6px 10px;
  }
  .notes:empty { display: none; }
  .addrow {
    display: flex;
    align-items: center;
    gap: 8px;
    flex-wrap: wrap;
    padding: 6px 10px;
  }
  .pad { padding: 8px 10px; margin: 0; }
  /* the type field is a combobox, not a word. It used to fight the path for the
     width of one line; now it owns the detail line's first column instead. */
  .typecell { flex: 1 1 240px; min-width: 140px; max-width: 360px; }
  .typefield { min-width: 0; }
  .parentcell { flex: 1 1 auto; min-width: 0; padding-top: 1px; }
  /* 20px whether or not it holds a delete: it is what puts an output's × over
     the × on an input's first line -- so nothing else may live in it. A value
     row's add button sits on the left, against the mode switch. */
  .trail {
    flex: 0 0 auto;
    min-width: 20px;
    display: flex;
    gap: 4px;
    justify-content: flex-end;
    align-items: center;
  }
  /* one field of a multi-field value row: indented under the line carrying the
     mode switch, so the block reads as belonging to the row rather than as
     three rows that happen to be adjacent */
  .row-item.entryline { padding-top: 0; padding-left: 28px; }
  /* a field's × is inset from the row's ×, which stays hard right: two deletes
     in one column would read as the same delete, and one of them discards the
     whole row */
  .trail.fieldtrail { padding-right: 16px; }
  /* narrow, like `.cols`: a key is one word and the value beside it is what
     wants the width */
  .keybox { flex: 0 1 8em; min-width: 4em; }
  /* a field under a sheet that has chosen no column yet. It is a blank in the
     recipe, not a constant, and the row's own note says so -- this is only what
     makes the blank findable in a long list. */
  .unset { border-color: var(--warn, var(--line)); }

  /* two labelled halves, one lit -- a slider read as one control to flip
     rather than two buttons doing different things */
  .modeswitch {
    flex: 0 0 auto;
    display: inline-flex;
    border: 1px solid var(--line);
    border-radius: 999px;
    overflow: hidden;
  }
  .modeswitch button {
    border: none;
    background: none;
    color: var(--muted);
    padding: 2px 9px;
    font-size: 11px;
  }
  .modeswitch button.on { background: var(--accent); color: var(--panel); }
</style>
