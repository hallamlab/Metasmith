<script>
  import DeleteControl from '../components/DeleteControl.svelte'
  import ParentPicker from '../components/ParentPicker.svelte'
  import TypeSelect from '../components/TypeSelect.svelte'

  // Inputs and outputs in one list, and the list *is* the form. They are two
  // headings over one run of rows, the way the ssh rail does managed and native
  // -- the rows stay siblings, and a heading is a label rather than a parent.
  //
  // There used to be a builder card below this one: a form you filled in, then
  // clicked add, then looked at the row that appeared somewhere else. Now the
  // add button makes an empty row here and you fill that in. An input row only
  // becomes a library item once it is complete, so a half-filled one is a
  // *draft* -- carried in the request beside the outputs, which have always
  // been request-only. Everything else about a draft row is a registered row.
  //
  // An output row is an input row with its path line taken off. That is not a
  // coincidence to be re-derived in two places: both render the same snippet,
  // so the type field and the parents cannot drift apart by a column.
  let {
    items = [],
    drafts = [],
    targets = [],
    typeOptions = [],
    // the attached sheet: its column names, what the last expansion registered
    // per row, and which row is the sample index
    columns = [],
    rowCount = 0,
    expansion = null,
    sharedPaths = [],
    // the sheet's own strip, rendered under the inputs band by the view above --
    // it belongs inside this box but it is not this card's business
    tableStrip = null,
    onindex,
    onshared,
    // (type) => {known, produced, consumed, producedVia, consumedVia} -- what
    // the index says about a type, for the line under a row being edited
    counts = null,
    onfocus,
    onremoveInput,
    onremoveDraft,
    onremoveTarget,
    ondraft,
    ontarget,
    onparents,
    onretype,
    onrepoint,
    oncommit,
    onadd,
  } = $props()

  // A draft is addressed by its own id, a registered item by its path, an
  // output by its position -- so lineage needs one key space per half. `#` is
  // safe as the draft marker: a library path never starts with one.
  const draftKey = (d) => `#${d.id}`

  // What a template's fields may hold, and how to tell one from a plain row.
  // A template is not a fourth kind of thing: it is an ordinary row whose path
  // (or a value row's name and value) names a column, so nothing has to be kept
  // in step with anything.
  // not a global regex: `test` on one carries `lastIndex` between calls, so the
  // same row would answer differently depending on what was asked before it
  const TOKEN = /\{[^{}]*\}/
  const hasToken = (s) => TOKEN.test(String(s ?? ''))
  const isTemplate = (d) =>
    d.mode === 'value' ? hasToken(d.name) || hasToken(d.value) : hasToken(d.path)

  // The items a sheet registered are not rows of this recipe. They are in the
  // library and in the plan, and two hundred of them here would be two hundred
  // rows with nothing on them to decide -- the template row carries the count.
  let ownItems = $derived(items.filter((it) => !it.template_id))
  let expanded = $derived(items.length - ownItems.length)

  // a field's element, so the column picker can insert at the caret rather than
  // at the end -- the usual gesture is `/data/` then a column then `_R1.fq.gz`
  const fieldId = (row, field) => `msm-f-${row.key}-${field}`

  // A draft has nothing to be called until it is filled in, and an empty string
  // in another row's lineage reads as a bug. Its type is the next best name.
  const draftLabel = (d) =>
    (d.mode === 'value' ? d.name : d.path) || (d.dtype ? `a new ${d.dtype}` : 'a new row')

  // The library reports an item's *ancestors*, not its parents: it expands the
  // chain when it loads and collapses it again when it saves. Left as they
  // arrive, a grandparent would be offered here with a tick beside it, and
  // taking that tick off would come back on the next reload -- so the chain is
  // collapsed the same way it is on the way to disk. One level is enough,
  // because what arrives is already the full closure.
  let immediate = $derived.by(() => {
    const of = new Map(ownItems.map((it) => [it.path, (it.parents ?? []).map((p) => p.path)]))
    const out = new Map()
    for (const [path, direct] of of) {
      const inherited = new Set(direct.flatMap((p) => of.get(p) ?? []))
      out.set(path, direct.filter((p) => !inherited.has(p)))
    }
    return out
  })

  let inputRows = $derived([
    ...ownItems.map((it) => ({
      kind: 'item',
      key: it.path,
      id: it.path,
      type: it.type_name,
      label: it.path,
      parents: immediate.get(it.path) ?? [],
      item: it,
    })),
    ...drafts.map((d) => ({
      kind: 'draft',
      key: draftKey(d),
      id: d.id,
      type: d.dtype,
      label: draftLabel(d),
      parents: d.parents ?? [],
      draft: d,
    })),
  ])

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

  // Every key each row descends from, however far up. The direct links are one
  // level (items arrive as a closure and are collapsed above; drafts only ever
  // state one level), so this is the fixpoint over them -- and it is what keeps
  // a cycle out of the menu below.
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
  // turning up wherever its path happened to sort.
  //
  // The ancestor *count* is enough to order this. A row's ancestors always
  // strictly contain each of its parents' ancestors plus that parent, so the
  // count rises along every edge and sorting by it is a topological order --
  // no traversal, and no answer at all to give for a cycle. The sort is stable,
  // so rows at the same depth keep the position they arrived in and nothing
  // reshuffles under a draft being filled in.
  let orderedInputRows = $derived(
    [...inputRows].sort((a, b) => (ancestors.get(a.key)?.size ?? 0) - (ancestors.get(b.key)?.size ?? 0)),
  )

  // What a row may be given as a parent. Three things are excluded, and the
  // third is the one worth saying out loud: a row cannot descend from something
  // that descends from *it*. Nothing downstream defines a cycle -- `AsSamples`
  // walks up and then back down, so a loop makes every branch the whole library.
  function inputOptions(row) {
    const pool = row.kind === 'item' ? inputRows.filter((r) => r.kind === 'item') : inputRows
    const have = new Set(row.parents)
    return pool
      .filter((r) => r.key !== row.key && !have.has(r.key) && !ancestors.get(r.key)?.has(row.key))
      // A draft is offerable on purpose -- that is what lets a chain of rows
      // commit in cascade from the top down, and what `apply` stamps out. But
      // it is not *registered*, and a row that descends from one waits for it:
      // pointing at a row you can already see, and then finding nothing
      // happened, is the same list failing to say which kind of thing it holds.
      .map((r) => ({ key: r.key, label: r.label, sub: r.type, draft: r.kind === 'draft' }))
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
  // sit in a lineage nothing on the page admits to.
  function chosenFor(row, byKey) {
    return row.parents.map((k) => {
      const r = byKey.get(k)
      if (!r) return { key: k, label: String(k) }
      return {
        key: k,
        label: r.label,
        sub: r.kind === 'target' ? null : r.type,
        draft: r.kind === 'draft',
      }
    })
  }

  // Which row a parent line is pointing at. The label is a path on an input and
  // a type name on an output, and neither is unique enough to find the row by
  // eye in a long list -- so hovering the line marks the row itself.
  let hover = $state(null)

  // A registered row is corrected in place rather than removed and added again.
  // Neither half of it touches the user's file: a path here is a *pointer*, so
  // re-pointing it moves nothing, and a type is a label on a manifest entry.
  // What it does cost is identity -- both are derived from path and type -- so
  // anything downstream of the row loses its cache reuse. Only the library's own
  // files (a value's) are ever moved, and those are the library's to move.
  const EDITABLE = 'click to change it — your file is not moved, but the row is re-registered and anything downstream loses cache reuse'

  // One row is edited at a time: which row, and which half of it.
  let edit = $state(null) // {key, field: 'path'|'type'}
  let draft = $state('')

  const editingRow = (row, field) => edit?.key === row.key && edit.field === field
  const wasValue = (row, field) => (field === 'path' ? row.label : row.type)

  function startEdit(row, field) {
    draft = wasValue(row, field)
    edit = { key: row.key, field }
  }

  // Enter closes the field, and closing it unmounts the input, which fires blur
  // -- so both land here and without the guard every edit is sent twice, the
  // second one racing the first. Same trap as renaming the workflow itself.
  function commitEdit(row) {
    if (edit?.key !== row.key) return
    const field = edit.field
    edit = null
    const next = draft.trim()
    if (!next || next === wasValue(row, field)) return
    if (field === 'path') onrepoint?.(row.item, next)
    else onretype?.(row.item, next)
  }

  // A request-held row writes through as it is picked, so leaving the field is
  // only a save and a close.
  function closeRow(row) {
    if (edit?.key === row.key) edit = null
    oncommit?.()
  }

  const setType = (row, v) =>
    row.kind === 'draft' ? ondraft?.(row.id, { dtype: v }) : ontarget?.(row.id, { type: v })

  // An input that nothing takes would sit unused; an output nothing makes will
  // not solve. Same list, different thing to warn about.
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

  // Escape abandons a type edit. TypeSelect swallows the first press while its
  // list is open, so this only fires once there is nothing left to close. On the
  // window rather than on the field's wrapper: that wrapper is a plain div around
  // a combobox, and a keydown handler on one is a role it does not have.
  $effect(() => {
    if (edit?.field !== 'type') return
    const key = (e) => {
      if (e.key === 'Escape') edit = null
    }
    window.addEventListener('keydown', key)
    return () => window.removeEventListener('keydown', key)
  })

</script>

<!-- The type, as a word until it is reached for. Every row does it the same way:
     a registered input, a draft and an output all name a type, and one of them
     rendering a permanently-open combobox while the others read as a word made
     the list look like three kinds of thing. Clicking it moves the panel onto
     that type as well as opening the field -- which is the whole reason the type
     is the thing you click.

     What decides which of the two is drawn is `edit`, and only `edit`. It used
     to be "has a type and is not being edited", which works for a registered
     row -- that one edits a local copy -- and fails for the two that write
     through as you type: an empty row opened its field, the first character
     landed in `row.type`, and the row promptly redrew itself as a word with the
     field gone. One keystroke per attempt. -->
{#snippet typeCell(row)}
  {#if row.type && !editingRow(row, 'type')}
    <button
      class="asfield type mono truncate"
      title={`${row.type} — ${row.kind === 'item' ? EDITABLE : 'click to change it'}`}
      onclick={() => {
        onfocus?.(row.type)
        startEdit(row, 'type')
      }}
    >{row.type}</button>
  {:else if row.kind === 'item'}
    <div
      class="typefield"
      onfocusin={() => ontypefocus?.(row)}
      onfocusout={(e) => {
        // the caret button is part of this control, so moving onto it is not
        // leaving the field
        if (!e.currentTarget.contains(e.relatedTarget)) commitEdit(row)
      }}
    >
      <TypeSelect
        value={draft}
        options={typeOptions}
        placeholder="namespace::type"
        autofocus
        describe={describeType(row)}
        onchange={(v) => (draft = v)}
        oncommit={() => commitEdit(row)}
      />
    </div>
  {:else}
    <!-- A draft's and an output's type is written through as it is typed, so
         the row is only *held open* by `edit` -- taking focus is what puts it
         there, and the field would otherwise collapse back into a word on the
         first character. Leaving the control is what takes it out again. -->
    <div
      class="typefield"
      onfocusin={() => {
        if (!editingRow(row, 'type')) edit = { key: row.key, field: 'type' }
      }}
      onfocusout={(e) => {
        if (!e.currentTarget.contains(e.relatedTarget)) closeRow(row)
      }}
    >
      <TypeSelect
        value={row.type}
        options={typeOptions}
        placeholder="namespace::type"
        autofocus={editingRow(row, 'type')}
        describe={describeType(row)}
        onchange={(v) => setType(row, v)}
        oncommit={() => closeRow(row)}
      />
    </div>
  {/if}
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
            ? 'the first output has nothing declared before it to come off'
            : 'an output can only come off one declared before it'
          : row.kind === 'item'
            ? 'a registered row can only descend from another registered one'
            : 'this row registers itself once it has a type, an identity, and every parent it names is itself registered'}
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
{#snippet columnPicker(row, field)}
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
        ondraft?.(row.id, { [field]: next })
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

<div class="col" style="gap:10px">
  <div class="spread">
    <h3>recipe</h3>
    <span class="small muted">{ownItems.length} in · {targets.length} out</span>
  </div>

  <div class="rows">
    <div class="heading in small muted spread">
      <span>inputs</span>
      <span class="count">
        {ownItems.length} registered{drafts.length ? ` · ${drafts.length} draft` : ''}{expanded
          ? ` · ${expanded} from the sheet`
          : ''}
      </span>
    </div>
    {#if tableStrip}{@render tableStrip()}{/if}
    {#if inputRows.length === 0}
      <p class="small muted pad">
        Nothing registered. Add the files and values you have — the planner works
        out the steps from their types alone.
      </p>
    {/if}
    {#each orderedInputRows as row (row.key)}
      {@const info = row.type && counts ? counts(row.type) : null}
      {@const waiting =
        row.kind === 'draft' && row.parents.some((p) => !items.some((it) => it.path === p))}
      {@const template = row.kind === 'draft' && isTemplate(row.draft)}
      <div class="entry" class:hl={hover === row.key}>
        <!-- Two lines, not one: the path is the longest thing on an input row and
             was being squeezed into a sliver beside a combobox and a menu. What
             the row points at goes on the first line; what it *is* and what it
             came from go on the second -- and that second line is the whole of an
             output row. -->
        <div class="row-item">
          {#if row.kind === 'item'}
            {#if editingRow(row, 'path')}
              <input
                class="grow mono"
                bind:value={draft}
                autofocus
                spellcheck="false"
                onblur={() => commitEdit(row)}
                onkeydown={(e) => {
                  if (e.key === 'Enter') commitEdit(row)
                  if (e.key === 'Escape') edit = null
                }}
              />
              <span class="small muted">enter to re-point</span>
            {:else}
              <button
                class="asfield grow truncate mono"
                title={`${row.label} — ${EDITABLE}`}
                onclick={() => startEdit(row, 'path')}
              >{row.label}</button>
            {/if}
          {:else if row.draft.mode === 'value'}
            <input
              class="grow"
              id={fieldId(row, 'name')}
              value={row.draft.name}
              placeholder={columns.length ? '{sample}' : 'K12'}
              spellcheck="false"
              oninput={(e) => ondraft?.(row.id, { name: e.currentTarget.value })}
              onblur={() => oncommit?.()}
            />
            {@render columnPicker(row, 'name')}
            <input
              class="grow"
              id={fieldId(row, 'value')}
              value={row.draft.value}
              placeholder="GCF_000005845.2"
              spellcheck="false"
              oninput={(e) => ondraft?.(row.id, { value: e.currentTarget.value })}
              onblur={() => oncommit?.()}
            />
            {@render columnPicker(row, 'value')}
          {:else}
            <input
              class="grow mono"
              id={fieldId(row, 'path')}
              value={row.draft.path}
              placeholder={columns.length
                ? '/data/{sample}_R1.fastq.gz'
                : '/data/sample_01.fastq.gz'}
              spellcheck="false"
              oninput={(e) => ondraft?.(row.id, { path: e.currentTarget.value })}
              onblur={() => oncommit?.()}
            />
            {@render columnPicker(row, 'path')}
          {/if}

          <span class="trail">
            <DeleteControl
              title={row.kind === 'item' ? 'remove from library' : 'discard this row'}
              onconfirm={() =>
                row.kind === 'item' ? onremoveInput?.(row.item) : onremoveDraft?.(row.id)}
            />
          </span>
        </div>

        {@render detail(row)}

        {#if row.kind === 'draft' && template}
          <!-- A templated row is one declaration, not N rows. What it says about
               itself is therefore a count and a role: how many items it stands
               for, and whether it is the one that says what a *sample* is. -->
          <div class="notes row wrap small">
            {#if expansion?.counts?.[row.id]}
              <span class="tag">× {expansion.counts[row.id]} registered</span>
            {:else}
              <span class="tag">× {rowCount} once expanded</span>
            {/if}
            <button
              class="star"
              class:on={row.draft.index}
              aria-pressed={!!row.draft.index}
              title={row.draft.index
                ? 'this row says what a sample is; its type is what the plan splits on'
                : 'make this the sample index — one run per sheet row, and every other templated row hangs off it'}
              onclick={() => onindex?.(row.id)}
            >★ sample index</button>
            {#if row.draft.index && row.parents.length}
              <span class="tag warn">
                the index cannot descend from anything, or every sample sees every other
              </span>
            {/if}
          </div>
        {/if}

        {#if row.kind === 'item' && columns.length}
          <div class="notes row wrap small">
            <button
              class="star"
              class:on={sharedPaths.includes(row.id)}
              aria-pressed={sharedPaths.includes(row.id)}
              title={sharedPaths.includes(row.id)
                ? 'every sample sees this'
                : 'let every sample see this — a reference beside the per-sample files is otherwise in no sample at all'}
              onclick={() => onshared?.(row.item, !sharedPaths.includes(row.id))}
            >shared by every sample</button>
          </div>
        {/if}

        {#if row.kind === 'draft' && (row.type || waiting)}
          <div class="notes row wrap small">
            {#if row.type && !info?.known}
              <span class="tag warn">not a type in this library</span>
            {:else if info?.known}
              <span
                class="tag"
                title={info.consumedVia
                  ? `${info.consumedVia} of them ask for a more general type, which this one satisfies`
                  : null}
              >{info.consumed} consume it{info.consumedVia ? ` (${info.consumedVia} indirectly)` : ''}</span>
              {#if info.consumed === 0}
                <span class="muted">nothing takes this — it would sit unused</span>
              {/if}
            {/if}
            {#if waiting}
              <span class="muted">waiting on a parent that is not registered yet</span>
            {/if}
          </div>
        {/if}
      </div>
    {/each}

    <div class="entry addrow">
      <button class="small" onclick={() => onadd?.('file')}>+ a file</button>
      <button class="small" onclick={() => onadd?.('value')}>+ a value</button>
      <span class="small muted">
        a row registers itself once it is complete; nothing is copied
      </span>
    </div>

    <div class="heading out small muted spread">
      <span>outputs</span>
      <span class="count">{targets.length} wanted</span>
    </div>
    {#if targetRows.length === 0}
      <p class="small muted pad">Nothing wanted yet. Add at least one to solve.</p>
    {/if}
    {#each targetRows as row (row.key)}
      {@const info = row.type && counts ? counts(row.type) : null}
      {@const dup = targetRows.some(
        (o) =>
          o.id !== row.id &&
          o.type === row.type &&
          row.type &&
          JSON.stringify([...o.parents].sort()) === JSON.stringify([...row.parents].sort()),
      )}
      <div class="entry" class:hl={hover === row.key}>
        {@render detail(row)}

        {#if row.type || dup}
          <div class="notes row wrap small">
            {#if row.type && !info?.known}
              <span class="tag warn">not a type in this library</span>
            {:else if info?.known}
              <span
                class="tag"
                class:bad={info.produced === 0}
                title={info.producedVia
                  ? `${info.producedVia} of them by a more specific type, which satisfies this one`
                  : null}
              >{info.produced} produce it{info.producedVia ? ` (${info.producedVia} indirectly)` : ''}</span>
              {#if info.produced === 0}
                <span class="muted">nothing can make this — the plan will not solve</span>
              {/if}
            {/if}
            {#if dup}
              <span class="tag warn">already wanted, with the same lineage</span>
            {/if}
          </div>
        {/if}
      </div>
    {/each}

    <div class="entry addrow">
      <button class="small" onclick={() => onadd?.('output')}>+ an output</button>
      <span class="small muted">a type you want out of this — the planner finds the way to it</span>
    </div>
  </div>

  <p class="small muted">
    Everything registered here is one run's worth of input. Removing a row
    unregisters it; the file itself is left alone.
  </p>
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
  /* the outputs band is a seam mid-box, not a label at the top of one */
  .heading.out { border-top: 3px solid var(--line); }
  .heading .count { text-transform: none; letter-spacing: 0; }
  /* the border is on the entry rather than the row, so a row and the line of
     tags under it read as one thing rather than two */
  .entry { border-bottom: 1px solid var(--line); }
  .entry:last-child { border-bottom: none; }
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
  /* fixed whether or not it holds a delete: it is what puts an output's × over
     the × on an input's first line */
  .trail { flex: 0 0 20px; display: flex; justify-content: flex-end; align-items: center; }
  /* A value that opens as a field when it is reached for: a row still reads as a
     row rather than as a form. But with no chrome at all it read as *print* --
     the path and the type were reported as uneditable -- so one underline stays
     at rest, and the box arrives on hover as it did. The transparent sides are
     what keep the resting and hovered states the same height. */
  .asfield {
    background: none;
    border: 1px solid transparent;
    border-bottom-color: var(--line);
    color: inherit;
    padding: 1px 5px;
    margin-left: -5px;
    text-align: left;
  }
  .asfield:hover { border-color: var(--line); background: var(--panel-2); }
  .type {
    display: block;
    max-width: 100%;
    color: var(--accent);
  }
  /* narrow on purpose: it sits beside a field that wants the width, and what it
     holds is one short word at a time */
  .cols { flex: 0 0 auto; width: 4.5em; padding: 2px 2px; }
  /* a role, toggled -- not a delete and not a link. On, it reads as the accent
     it marks the row with; off, it is as quiet as the tags beside it. */
  .star {
    padding: 1px 6px;
    font-size: 11px;
    background: none;
    color: var(--muted);
    border-color: var(--line);
  }
  .star.on { color: var(--accent); border-color: var(--accent); }
</style>
