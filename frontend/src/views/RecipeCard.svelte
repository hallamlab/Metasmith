<script>
  import { flip } from 'svelte/animate'
  import DeleteControl from '../components/DeleteControl.svelte'
  import LineageRail from '../components/LineageRail.svelte'
  import ParentPicker from '../components/ParentPicker.svelte'
  import TypeSelect from '../components/TypeSelect.svelte'
  import { entries as rowEntries, isArrayRow, rowLabel, wholeToken } from '../lib/rows.js'

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
    onremoveRow,
    onremoveTarget,
    onrow,
    ontarget,
    onparents,
    oncommit,
    onadd,
  } = $props()

  // An input row is addressed by its own id, an output by its position -- so
  // lineage needs one key space per half. `#` is safe as the marker: a library
  // path never starts with one, which is what let the two live together while
  // rows and registered items were different things.
  const rowKey = (d) => `#${d.id}`

  // `isArrayRow`, `rowLabel`, `entries` and `wholeToken` are imported (see
  // `lib/rows.js`) rather than written here: the workflow view asks the same
  // questions, and a row that reads as an array in one place and not the other
  // is a disagreement nothing on the page can show. `wholeToken` is the
  // whole-string form of the same `{col}` syntax `columnPicker` splices into
  // the middle of one; the two agree because `ops.samples` on the server reads
  // both the same way, as a token to substitute.

  // A field a dropdown would otherwise own, held open as free text -- typing a
  // pattern like `/data/{sample}_R1.fq.gz` around a token needs the field
  // back. Keyed per row and field so switching one back does not touch
  // another drawn from the same set of columns.
  let freeform = $state(new Set())
  const fieldKey = (row, field) => `${row.key}::${field}`
  const isFreeform = (row, field) => freeform.has(fieldKey(row, field))
  const setFreeform = (row, field, on) => {
    const k = fieldKey(row, field)
    const next = new Set(freeform)
    if (on) next.add(k)
    else next.delete(k)
    freeform = next
  }

  // What the sheet registered is not rows of this recipe. Those items are in the
  // library and in the plan, and two hundred of them here would be two hundred
  // rows with nothing on them to decide -- the array row carries the count.
  let expanded = $derived(items.filter((it) => it.array_id).length)

  // a field's element, so the column picker can insert at the caret rather than
  // at the end -- the usual gesture is `/data/` then a column then `_R1.fq.gz`
  const fieldId = (row, field) => `msm-f-${row.key}-${field}`

  // The entries of a value row, and the one place they are written back. A row
  // holds a list of them, so every edit to one is an edit to the whole list --
  // which is what `apply` below hands `sampleField`, in place of the single
  // field name it used to close over.
  const patchEntry = (row, i, patch) => {
    const next = rowEntries(row.row).map((e, k) => (k === i ? { ...e, ...patch } : e))
    onrow?.(row.id, { values: next })
  }
  const addEntry = (row) => {
    onrow?.(row.id, { values: [...rowEntries(row.row), { key: '', value: '' }] })
    oncommit?.()
  }
  const dropEntry = (row, i) => {
    onrow?.(row.id, { values: rowEntries(row.row).filter((_e, k) => k !== i) })
    oncommit?.()
  }

  let inputRows = $derived(
    rows.map((d) => ({
      kind: 'input',
      key: rowKey(d),
      id: d.id,
      type: d.dtype,
      label: rowLabel(d),
      parents: d.parents ?? [],
      row: d,
    })),
  )

  // An output is named by its *type*, not by its position. The position is what
  // the request stores and is still the key -- but "#2" is a fact about the file
  // on disk, and a person reading a lineage wants to know what the thing is. The
  // number comes back only when two outputs share a type and the name alone
  // would point at either.
  let targetRows = $derived(
    targets.map((t, i) => {
      const name = t.type || '(no type yet)'
      const shared = targets.filter((o) => (o.type || '') === (t.type || '')).length > 1
      return {
        kind: 'target',
        key: `#${i}`,
        id: i,
        type: t.type,
        label: shared ? `${name} #${i + 1}` : name,
        // Stored as numbers, keyed as strings, everywhere else on this page.
        // Left unconverted the two never met: a tick never showed, and the
        // summary fell through to printing the raw 0-based position.
        parents: (t.parents ?? []).map((p) => `#${p}`),
      }
    }),
  )

  let inputByKey = $derived(new Map(inputRows.map((r) => [r.key, r])))
  let targetByKey = $derived(new Map(targetRows.map((r) => [r.key, r])))

  // The lineage rail's Y positions, read off the real rows rather than a
  // fixed pitch -- a value row wraps, an array row grows a count note, so
  // nothing here is uniform the way a plan DAG's steps are. Each rows column
  // is `position: relative`, so a child's own `offsetTop` is already relative
  // to it; observing the *column* rather than each row catches a single row
  // growing too, since that always changes the column's own height.
  let inputBox = $state(null)
  let outputBox = $state(null)
  let inputY = $state(new Map())
  let outputY = $state(new Map())
  let inputBandHeight = $state(0)
  let outputBandHeight = $state(0)

  function measureBand(box) {
    if (!box) return { y: new Map(), height: 0 }
    const y = new Map()
    for (const el of box.children) {
      const key = el.dataset.rowKey
      if (key) y.set(key, el.offsetTop + el.offsetHeight / 2)
    }
    return { y, height: box.offsetHeight }
  }

  $effect(() => {
    if (!inputBox) return
    const remeasure = () => ({ y: inputY, height: inputBandHeight } = measureBand(inputBox))
    const ro = new ResizeObserver(remeasure)
    ro.observe(inputBox)
    remeasure()
    return () => ro.disconnect()
  })

  $effect(() => {
    if (!outputBox) return
    const remeasure = () => ({ y: outputY, height: outputBandHeight } = measureBand(outputBox))
    const ro = new ResizeObserver(remeasure)
    ro.observe(outputBox)
    remeasure()
    return () => ro.disconnect()
  })

  let railInputRows = $derived(
    orderedInputRows.map((r) => ({ key: r.key, parents: r.parents, y: inputY.get(r.key) })),
  )
  let railOutputRows = $derived(
    targetRows.map((r) => ({ key: r.key, parents: r.parents, y: outputY.get(r.key) })),
  )

  // Every key each row descends from, however far up. A row states one level,
  // so this is the fixpoint over them -- and it is what keeps a cycle out of
  // the menu below.
  let ancestors = $derived.by(() => {
    const out = new Map(inputRows.map((r) => [r.key, new Set(r.parents)]))
    // a set only ever gains members and there are finitely many, so a loop
    // already on disk cannot spin this
    for (;;) {
      let grew = false
      for (const set of out.values()) {
        for (const p of [...set]) {
          for (const up of out.get(p) ?? []) {
            if (set.has(up)) continue
            set.add(up)
            grew = true
          }
        }
      }
      if (!grew) break
    }
    return out
  })

  // Listed in the order the data descends: parents above the things made from
  // them, so a pangenome leads the assemblies it was built from rather than
  // turning up wherever its path happened to sort. Sorting by raw ancestor
  // *count* used to stand in for this, but it isn't actually a topological
  // order on arrival: a fresh row with no parents yet has a count of zero, so
  // it would sort ahead of any older row that already has lineage, landing
  // wherever the depths happened to fall rather than at the bottom it was
  // added to.
  //
  // Kahn's algorithm, seeded by insertion order instead, is the honest
  // version: a row is eligible the moment every one of its parents has
  // already been placed, and among eligible rows the one that arrived first
  // goes next. A row with no parents is eligible immediately and -- having
  // arrived after everything already eligible -- keeps the position it was
  // added to. A row only moves when it is actually given a parent that sits
  // later in the list, which is the real reorder `animate:flip` below is for.
  let orderedInputRows = $derived.by(() => {
    const remaining = new Map(inputRows.map((r) => [r.key, new Set(r.parents)]))
    const out = []
    while (remaining.size) {
      const next = inputRows.find((r) => remaining.has(r.key) && remaining.get(r.key).size === 0)
      // a cycle (only ever possible in data loaded off disk -- `inputOptions`
      // refuses to offer one interactively) leaves nothing eligible; rather
      // than drop rows or loop forever, flush what's left in arrival order
      if (!next) {
        for (const r of inputRows) if (remaining.has(r.key)) out.push(r)
        break
      }
      out.push(next)
      remaining.delete(next.key)
      for (const parents of remaining.values()) parents.delete(next.key)
    }
    return out
  })

  // What a row may be given as a parent. Three things are excluded, and the
  // third is the one worth saying out loud: a row cannot descend from something
  // that descends from *it*. Nothing downstream defines a cycle -- `AsSamples`
  // walks up and then back down, so a loop makes every branch the whole library.
  function inputOptions(row) {
    const have = new Set(row.parents)
    return inputRows
      .filter((r) => r.key !== row.key && !have.has(r.key) && !ancestors.get(r.key)?.has(row.key))
      .map((r) => ({ key: r.key, label: r.label, sub: r.type }))
  }

  // Outputs are positions, and a target may only name one declared *before* it
  // -- `ops.workflow._add_targets` refuses a forward reference, so offering one
  // here would be offering a link the generate then throws out. Ordering does
  // the cycle check for free.
  function targetOptions(row) {
    const have = new Set(row.parents)
    return targetRows
      .filter((r) => r.id < row.id && !have.has(r.key))
      .map((r) => ({ key: r.key, label: r.label }))
  }

  // A parent whose row has since gone still has to be shown, or an entry would
  // sit in a lineage nothing on the page admits to. The path a parent chip used
  // to lead with is not unique enough to tell rows apart either -- the hover
  // highlight on the row itself already does that -- so the chip states only
  // what the parent *is*.
  function chosenFor(row, byKey) {
    return row.parents.map((k) => ({ key: k, sub: byKey.get(k)?.type ?? null }))
  }

  // Which row a parent line is pointing at. The label is a path on an input and
  // a type name on an output, and neither is unique enough to find the row by
  // eye in a long list -- so hovering the line marks the row itself.
  let hover = $state(null)

  const setType = (row, v) =>
    row.kind === 'target' ? ontarget?.(row.id, { type: v }) : onrow?.(row.id, { dtype: v })

  // An output nothing makes will not solve.
  const describeType = (row) =>
    counts
      ? (t) => {
          const c = counts(t)
          return {
            note: `${c.produced} produce · ${c.consumed} consume`,
            warn: row.kind === 'target' ? c.produced === 0 : c.produced === 0 && c.consumed === 0,
          }
        }
      : null

</script>

<!-- The type is always the field, never a word standing in for it -- an input
     and an output name a type through the same combobox the same way. Focusing
     it is what moves the panel onto that type, in place of the click that used
     to open it.

     The focus-out is what saves it. A combobox is a box plus a caret button, so
     "left the field" is focus leaving the whole control rather than the input
     inside it -- tabbing from the box to the caret is not leaving. Without this
     a type typed and tabbed away from persisted nothing until some other field
     happened to blur. -->
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
      describe={describeType(row)}
      onchange={(v) => setType(row, v)}
      oncommit={() => oncommit?.()}
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
  <div class="row-item detail">
    <div class="typecell">{@render typeCell(row)}</div>
    <div class="parentcell">
      <ParentPicker
        chosen={chosenFor(row, isTarget ? targetByKey : inputByKey)}
        options={isTarget ? targetOptions(row) : inputOptions(row)}
        note={isTarget
          ? row.id === 0
            ? null
            : 'an output can only come off one declared before it'
          : null}
        onadd={(k) => onparents?.(row, [...row.parents, k])}
        onremove={(k) => onparents?.(row, row.parents.filter((x) => x !== k))}
        onhover={(k) => (hover = k)}
      />
    </div>
    <span class="trail">
      {#if isTarget}
        <DeleteControl title="stop wanting this" onconfirm={() => onremoveTarget?.(row.id)} />
      {/if}
    </span>
  </div>
{/snippet}

<!-- The columns of the attached sheet, as something to put in a field rather
     than something to type from memory. It inserts at the caret and hands focus
     back, because the usual gesture is `/data/` then a column then `_R1.fq.gz`. -->
{#snippet columnPicker(row, field, apply)}
  {#if columns.length}
    <select
      class="cols small"
      aria-label="insert a column"
      value=""
      onchange={(e) => {
        const col = e.currentTarget.value
        e.currentTarget.value = ''
        if (!col) return
        const box = document.getElementById(fieldId(row, field))
        if (!box) return
        const at = box.selectionStart ?? box.value.length
        const next = `${box.value.slice(0, at)}{${col}}${box.value.slice(box.selectionEnd ?? at)}`
        apply(next)
        box.focus()
        const caret = at + col.length + 2
        requestAnimationFrame(() => box.setSelectionRange(caret, caret))
      }}
    >
      <option value="">{'{ }'}</option>
      {#each columns as c}<option value={c}>{c}</option>{/each}
    </select>
  {/if}
{/snippet}

<!-- A field that names one column and nothing else: a choice from the sheet's
     own columns, not a string to type -- which is what a sample table attached
     is *for*. A field around a token in a longer pattern (a path with a column
     in the middle of it) is still free text with `columnPicker` to insert into,
     since a dropdown cannot represent that shape at all.

     `field` is only a name -- for the element id and the freeform set, so two
     fields of one row are told apart. What a write *means* is `apply`'s
     business: a path sets one key on the row, a value entry rewrites the whole
     list it is a member of. -->
{#snippet sampleField(row, field, value, placeholder, mono, apply)}
  {@const col = wholeToken(value)}
  {#if columns.length && col !== null && !isFreeform(row, field)}
    <select
      class="grow{mono ? ' mono' : ''}"
      aria-label={`${field}, a column of the attached sheet`}
      value={col}
      onchange={(e) => apply(`{${e.currentTarget.value}}`)}
    >
      {#each columns as c}<option value={c}>{c}</option>{/each}
    </select>
    <button
      class="star"
      title="type a pattern around a column instead of naming one plainly"
      onclick={() => setFreeform(row, field, true)}
    >pattern</button>
  {:else}
    <input
      class="grow{mono ? ' mono' : ''}"
      id={fieldId(row, field)}
      {value}
      {placeholder}
      spellcheck="false"
      oninput={(e) => apply(e.currentTarget.value)}
      onblur={() => {
        oncommit?.()
        setFreeform(row, field, false)
      }}
    />
    {@render columnPicker(row, field, apply)}
  {/if}
{/snippet}

<!-- One field of a value row: its key, its value, and the sheet's columns for
     the value. The key is literal -- only values take `{column}` tokens, so the
     grouping key, the array test and the validation messages all read one set
     of fields. With one entry the key is optional and says so; with two or more
     it is what the field is called in the object the row writes, and the server
     refuses a launch off a recipe where one is blank. -->
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
    columns.length ? '{sample}' : 'GCF_000005845.2',
    false,
    (text) => patchEntry(row, i, { value: text }),
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
      <div class="band">
        <LineageRail rows={railInputRows} height={inputBandHeight} />
        <div class="rowsCol" bind:this={inputBox}>
          {#each orderedInputRows as row (row.key)}
            {@const info = row.type && counts ? counts(row.type) : null}
            {@const array = isArrayRow(row.row)}
            <div class="entry" data-row-key={row.key} class:hl={hover === row.key} animate:flip={{ duration: 150 }}>
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
                     what make a row a sample array. -->
                <div class="row-item">
                  {@render modeSwitch(row)}
                  {#if ents.length === 1}
                    {@render valueEntry(row, ents, 0)}
                  {:else}
                    <span class="grow small muted">
                      {ents.length} fields — written as one object
                    </span>
                  {/if}
                  <span class="trail">
                    {#if ents.length === 1}
                      <button class="star" title="another field, under its own key" onclick={() => addEntry(row)}>+</button>
                    {/if}
                    <DeleteControl title="discard this row" onconfirm={() => onremoveRow?.(row.id)} />
                  </span>
                </div>
                {#if ents.length > 1}
                  {#each ents as _e, i}
                    <div class="row-item entryline">
                      {@render valueEntry(row, ents, i)}
                      <span class="trail">
                        {#if i === ents.length - 1}
                          <button class="star" title="another field, under its own key" onclick={() => addEntry(row)}>+</button>
                        {/if}
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
                    columns.length ? '/data/{sample}_R1.fastq.gz' : '/data/sample_01.fastq.gz',
                    true,
                    (text) => onrow?.(row.id, { path: text }),
                  )}
                  <span class="trail">
                    <DeleteControl title="discard this row" onconfirm={() => onremoveRow?.(row.id)} />
                  </span>
                </div>
              {/if}

              {@render detail(row)}

              {#if array}
                <!-- An array row is one declaration, not N rows: what it says about
                     itself is a count of how many items it stands for. -->
                <div class="notes row wrap small">
                  {#if expansion?.counts?.[row.id]}
                    <span class="tag">× {expansion.counts[row.id]} registered</span>
                  {:else}
                    <span class="tag">× {rowCount} once expanded</span>
                  {/if}
                </div>
              {/if}

              <!-- Keyed by the row, not by a path: a row may not have one yet, which
                   is the normal state of a fresh recipe, and it is the row that is
                   durable in any case. The generate turns it into a path between the
                   sync that made it and the solve that reads it. -->
              {#if columns.length && !array}
                <div class="notes row wrap small">
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
            </div>
          {/each}
        </div>
      </div>
    {/if}

    <div class="entry addrow">
      <!-- One button, not a choice up front: a path and a value are the same
           kind of thing to add, a row, and what it holds is a switch on the row
           itself rather than a different action to take here. -->
      <button class="small" onclick={() => onadd?.('input')}>+ an input</button>
      <span class="small muted">
        a file you have or a value you type; nothing is copied
      </span>
    </div>

    <div class="heading out small muted spread">
      <span>outputs</span>
      <span class="count">{targets.length} wanted</span>
    </div>
    {#if targetRows.length === 0}
      <p class="small muted pad">Nothing wanted yet. Add at least one to solve.</p>
    {:else}
      <div class="band">
        <LineageRail rows={railOutputRows} height={outputBandHeight} />
        <div class="rowsCol" bind:this={outputBox}>
          {#each targetRows as row (row.key)}
            {@const info = row.type && counts ? counts(row.type) : null}
            {@const dup = targetRows.some(
              (o) =>
                o.id !== row.id &&
                o.type === row.type &&
                row.type &&
                JSON.stringify([...o.parents].sort()) === JSON.stringify([...row.parents].sort()),
            )}
            <div class="entry" data-row-key={row.key} class:hl={hover === row.key}>
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
            </div>
          {/each}
        </div>
      </div>
    {/if}

    <div class="entry addrow">
      <button class="small" onclick={() => onadd?.('output')}>+ an output</button>
      <span class="small muted">a type you want out of this — the planner finds the way to it</span>
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
  /* the rail and its rows are true flex siblings sharing one coordinate
     space -- nothing (padding, a border) may sit between them, or the rail's
     measured `y` values drift from where the rows actually land. `.rowsCol`
     is `position: relative` so a row's own `offsetTop` is already relative
     to it, with nothing to subtract. */
  .band { display: flex; align-items: flex-start; }
  .rowsCol { flex: 1 1 auto; min-width: 0; position: relative; }
  /* the border is on the entry rather than the row, so a row and the line of
     tags under it read as one thing rather than two */
  .entry { border-bottom: 1px solid var(--line); }
  /* a direct-child selector on purpose: an `.entry` nested in `.rowsCol` is
     never the last thing in `.rows` any more, and losing its border just
     because it is last inside its own rail's column would be a visual change
     nothing about this feature asked for */
  .rows > .entry:last-child { border-bottom: none; }
  /* the one mark left on a row, and it comes from a pointer sitting on a parent
     line somewhere else: "that link means *this* row". Clicking a type moves the
     panel and marks nothing -- it used to mark every row of that type, in both
     halves, so touching an input lit up an output that shared its name. */
  .entry.hl { background: var(--panel-2); box-shadow: inset 2px 0 0 var(--accent); }
  .row-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
  }
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
     the × on an input's first line. It grows only for a value row's add button,
     which has nothing above or below it to line up with. */
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
  /* narrow, like `.cols`: a key is one word and the value beside it is what
     wants the width */
  .keybox { flex: 0 1 8em; min-width: 4em; }
  /* narrow on purpose: it sits beside a field that wants the width, and what it
     holds is one short word at a time */
  .cols { flex: 0 0 auto; width: 4.5em; padding: 2px 2px; }

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
