<script>
  import DeleteControl from '../components/DeleteControl.svelte'
  import LineageMenu from '../components/LineageMenu.svelte'
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
  let {
    items = [],
    drafts = [],
    targets = [],
    sampleType = '',
    focus = null,
    typeOptions = [],
    // (type) => {known, produced, consumed, producedVia, consumedVia} -- what
    // the index says about a type, for the line under a row being edited
    counts = null,
    onfocus,
    onsample,
    onremoveInput,
    onremoveDraft,
    onremoveTarget,
    ondraft,
    ontarget,
    onparents,
    oncommit,
    ontypefocus,
    onadd,
  } = $props()

  // A draft is addressed by its own id, a registered item by its path, an
  // output by its position -- so lineage needs one key space per half. `#` is
  // safe as the draft marker: a library path never starts with one.
  const draftKey = (d) => `#${d.id}`

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
    const of = new Map(items.map((it) => [it.path, (it.parents ?? []).map((p) => p.path)]))
    const out = new Map()
    for (const [path, direct] of of) {
      const inherited = new Set(direct.flatMap((p) => of.get(p) ?? []))
      out.set(path, direct.filter((p) => !inherited.has(p)))
    }
    return out
  })

  let inputRows = $derived([
    ...items.map((it) => ({
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

  let targetRows = $derived(
    targets.map((t, i) => ({
      kind: 'target',
      key: `#${i}`,
      id: i,
      type: t.type,
      label: `#${i + 1} ${t.type || '(no type yet)'}`,
      parents: t.parents ?? [],
    })),
  )

  // Children are the parent map read the other way. The server only ever
  // states parents -- there is one edge, and inverting it here keeps it that
  // way rather than storing the same link twice.
  function invert(rows) {
    const out = new Map()
    for (const row of rows) {
      for (const p of row.parents) {
        if (!out.has(p)) out.set(p, [])
        out.get(p).push({ label: row.label, sub: row.type })
      }
    }
    return out
  }

  let inputChildren = $derived(invert(inputRows))
  let targetChildren = $derived(invert(targetRows))

  const optionsFor = (rows, self) =>
    rows.filter((r) => r.key !== self).map((r) => ({ key: r.key, label: r.label, sub: r.type }))

  // A registered row's type and path are fixed: renaming an item moves the
  // user's own file, and nothing retypes one in place. Saying so is better than
  // a field that silently does nothing -- deleting and adding again is the way.
  const FIXED = 'registered — remove and add it again to change its path or type'

  let unknownSample = $derived(
    !!sampleType && !inputRows.some((r) => r.type === sampleType),
  )
</script>

<div class="col" style="gap:10px">
  <div class="spread">
    <h3>recipe</h3>
    <span class="small muted">{items.length} in · {targets.length} out</span>
  </div>

  <div class="rows">
    <div class="heading small muted">inputs</div>
    {#if inputRows.length === 0}
      <p class="small muted pad">
        Nothing registered. Add the files and values you have — the planner works
        out the steps from their types alone.
      </p>
    {/if}
    {#each inputRows as row (row.key)}
      {@const info = row.type && counts ? counts(row.type) : null}
      {@const waiting =
        row.kind === 'draft' && row.parents.some((p) => !items.some((it) => it.path === p))}
      <div class="entry" class:sel={focus === row.type}>
        <div class="row-item">
          <!-- the radio *is* the sample type: it is a property of the inputs you
               can see, not a select somewhere else that has to agree with them -->
          <!-- deliberately not one radio *group*: the mark is on the type, so
               every row sharing it is marked, and a group would let the browser
               enforce exactly one checked row and hide the rest of the branch -->
          <label class="gutter" title="one run per item of this type">
            <input
              type="radio"
              disabled={!row.type}
              checked={!!row.type && sampleType === row.type}
              onclick={() => row.type && onsample?.(row.type)}
            />
          </label>

          {#if row.kind === 'item'}
            <div class="grow truncate mono" title={row.label}>{row.label}</div>
          {:else if row.draft.mode === 'value'}
            <input
              class="grow"
              value={row.draft.name}
              placeholder="K12"
              spellcheck="false"
              oninput={(e) => ondraft?.(row.id, { name: e.currentTarget.value })}
              onblur={() => oncommit?.()}
            />
            <input
              class="grow"
              value={row.draft.value}
              placeholder="GCF_000005845.2"
              spellcheck="false"
              oninput={(e) => ondraft?.(row.id, { value: e.currentTarget.value })}
              onblur={() => oncommit?.()}
            />
          {:else}
            <input
              class="grow mono"
              value={row.draft.path}
              placeholder="/data/sample_01.fastq.gz"
              spellcheck="false"
              oninput={(e) => ondraft?.(row.id, { path: e.currentTarget.value })}
              onblur={() => oncommit?.()}
            />
          {/if}

          <!-- a registered item can only descend from another registered one:
               a draft has no path yet, so there is nothing for the library to
               point at. Drafts may descend from either. -->
          <LineageMenu
            options={optionsFor(
              row.kind === 'item' ? inputRows.filter((r) => r.kind === 'item') : inputRows,
              row.key,
            )}
            selected={row.parents}
            descendants={inputChildren.get(row.key) ?? []}
            note={row.kind === 'item'
              ? 'the path and the type are fixed; the lineage is not'
              : 'this row is registered once it has a type, an identity, and every parent it names is itself registered'}
            onchange={(keys) => onparents?.(row, keys)}
          />

          {#if row.kind === 'item'}
            <button
              class="type mono truncate"
              title={FIXED}
              onclick={() => onfocus?.(row.type)}
            >{row.type}</button>
          {:else}
            <div
              class="typecell"
              onfocusin={() => ontypefocus?.(row)}
              onfocusout={(e) => {
                // the caret button is part of this control, so moving onto it
                // is not leaving the field
                if (!e.currentTarget.contains(e.relatedTarget)) oncommit?.()
              }}
            >
              <TypeSelect
                value={row.draft.dtype}
                options={typeOptions}
                placeholder="namespace::type"
                describe={counts
                  ? (t) => {
                      const c = counts(t)
                      return {
                        note: `${c.produced} produce · ${c.consumed} consume`,
                        warn: c.produced === 0 && c.consumed === 0,
                      }
                    }
                  : null}
                onchange={(v) => {
                  ontypefocus?.(row)
                  ondraft?.(row.id, { dtype: v })
                }}
                oncommit={() => oncommit?.()}
              />
            </div>
          {/if}

          <DeleteControl
            title={row.kind === 'item' ? 'remove from library' : 'discard this row'}
            onconfirm={() =>
              row.kind === 'item' ? onremoveInput?.(row.item) : onremoveDraft?.(row.id)}
          />
        </div>

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

    <div class="heading small muted">outputs</div>
    {#if targetRows.length === 0}
      <p class="small muted pad">Nothing wanted yet. Add at least one to generate.</p>
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
      <div class="entry" class:sel={focus === row.type}>
        <div class="row-item">
          <span class="gutter small muted">#{row.id + 1}</span>
          <div
            class="grow typecell"
            onfocusin={() => ontypefocus?.(row)}
            onfocusout={(e) => {
              if (!e.currentTarget.contains(e.relatedTarget)) oncommit?.()
            }}
          >
            <TypeSelect
              value={row.type}
              options={typeOptions}
              placeholder="namespace::type"
              describe={counts
                ? (t) => {
                    const c = counts(t)
                    return {
                      note: `${c.produced} produce · ${c.consumed} consume`,
                      warn: c.produced === 0,
                    }
                  }
                : null}
              onchange={(v) => {
                ontypefocus?.(row)
                ontarget?.(row.id, { type: v })
              }}
              oncommit={() => oncommit?.()}
            />
          </div>

          <LineageMenu
            options={optionsFor(targetRows, row.key)}
            selected={row.parents}
            descendants={targetChildren.get(row.key) ?? []}
            note="which earlier output this one comes off — how two outputs of the same type stay distinct"
            onchange={(keys) => onparents?.(row, keys)}
          />

          <DeleteControl title="stop wanting this" onconfirm={() => onremoveTarget?.(row.id)} />
        </div>

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

  {#if unknownSample}
    <p class="small muted">
      Each run starts from <span class="mono">{sampleType}</span>, which nothing
      registered has any more — mark a row below, or the plan has nothing to branch on.
    </p>
  {:else}
    <p class="small muted">
      The radio marks what each run starts from: every input of that type becomes
      its own branch. Removing an input unregisters it; the file itself is left alone.
    </p>
  {/if}
</div>

<style>
  .rows {
    border: 1px solid var(--line);
    border-radius: var(--radius);
    /* not hidden: a lineage menu and a type list both hang out of their row */
    overflow: visible;
  }
  .heading {
    padding: 6px 10px;
    background: var(--panel-2);
    border-bottom: 1px solid var(--line);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    font-size: 11px;
  }
  /* the border is on the entry rather than the row, so a row and the line of
     tags under it read as one thing rather than two */
  .entry { border-bottom: 1px solid var(--line); }
  .entry:last-child { border-bottom: none; }
  .entry.sel { background: var(--panel-2); box-shadow: inset 2px 0 0 var(--accent); }
  .row-item {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 6px 10px;
  }
  .notes {
    gap: 6px;
    align-items: baseline;
    padding: 0 10px 6px 40px;
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
  .gutter { flex: 0 0 22px; display: flex; align-items: center; }
  .gutter input { width: auto; margin: 0; }
  /* the type field is a combobox, not a word: it needs a column of its own,
     and one that does not swallow the path beside it */
  .typecell { flex: 0 1 240px; min-width: 140px; }
  .grow.typecell { flex: 1 1 240px; }
  .type {
    flex: 0 1 auto;
    max-width: 45%;
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    text-align: left;
  }
  .type:hover { text-decoration: underline; border: none; }
</style>
