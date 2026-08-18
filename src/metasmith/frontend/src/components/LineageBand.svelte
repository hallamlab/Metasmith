<script>
  import { flip } from 'svelte/animate'
  import LineageRail from './LineageRail.svelte'

  // One half of the recipe: a lineage rail down the left, the rows themselves
  // down the right, and the measuring that keeps the two in the same coordinate
  // space.
  //
  // This exists because the inputs and the outputs were two copies of it, and
  // the copies had drifted -- one animated its reorder and the other did not
  // reorder at all. What differs between the halves is what a row *says*, which
  // arrives as a snippet; everything about how a row sits beside a rail is the
  // same question twice and is answered here.
  //
  // The rows arrive already in the order they are to be drawn. That order is
  // the recipe's, not this component's -- and it has to be the same array the
  // rail is told about, because `LineageRail` sends the order up with its
  // layout request and a rail laid out in one order against a DOM laid out in
  // another crosses the markers it was told to avoid.
  let {
    rows = [],
    // 'data' | 'target' -- which marker the rail draws
    kind = 'data',
    // `{nodes, edges}` from `lib/highlight`: the rail paints it, and a row's own
    // background reads the same map, so the two cannot disagree about which
    // rows are marked
    marks = null,
    // (row) => the row's contents
    body,
  } = $props()

  // The rail's Y positions, read off the real rows rather than a fixed pitch --
  // a value row wraps, an array row grows a count note, so nothing here is
  // uniform the way a plan DAG's steps are. The column is `position: relative`,
  // so a child's own `offsetTop` is already relative to it; observing the
  // *column* rather than each row catches a single row growing too, since that
  // always changes the column's own height.
  let box = $state(null)
  let y = $state(new Map())
  let bandHeight = $state(0)

  $effect(() => {
    if (!box) return
    const remeasure = () => {
      const next = new Map()
      for (const el of box.children) {
        const key = el.dataset.rowKey
        if (key) next.set(key, el.offsetTop + el.offsetHeight / 2)
      }
      y = next
      bandHeight = box.offsetHeight
    }
    const ro = new ResizeObserver(remeasure)
    ro.observe(box)
    return () => ro.disconnect()
  })

  let railRows = $derived(
    rows.map((r) => ({ key: r.key, parents: r.parents, y: y.get(r.key) })),
  )

  const isHl = (key) => !!marks?.nodes?.has(key)
</script>

<div class="band">
  <LineageRail rows={railRows} height={bandHeight} {kind} {marks} />
  <div class="rowsCol" bind:this={box}>
    {#each rows as row (row.key)}
      <!-- keyed on the row's own id, never its position: `animate:flip` only
           animates an element that survives the move, and on positional keys
           Svelte would hold the elements still and swap what they say instead --
           which reads as several rows all changing at once rather than as one
           row moving to where it belongs. -->
      <div
        class="entry"
        data-row-key={row.key}
        class:hl={isHl(row.key)}
        animate:flip={{ duration: 150 }}
      >
        {@render body(row)}
      </div>
    {/each}
  </div>
</div>

<style>
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
  /* the one mark left on a row, and it comes from a pointer sitting on a parent
     line somewhere else: "that link means *this* row". Clicking a type moves the
     panel and marks nothing -- it used to mark every row of that type, in both
     halves, so touching an input lit up an output that shared its name.
     No left bar: it was a second rail drawn beside the first, saying the same
     thing one column over. The marker in the rail is what lights up now. */
  .entry.hl { background: var(--panel-2); }
</style>
