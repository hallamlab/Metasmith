<script>
  import { splitType } from '../lib/types.js'

  // The two stacked lines a type name is drawn as: the namespace at half size
  // above, the bare name below -- the same shape the plan's diagram gives a
  // node, so a list and a drawing naming one type say it the same way.
  //
  // A component rather than a helper because what is shared here is CSS, not
  // markup. Svelte scopes styles per component, so `.ns`'s half size and the
  // two truncations would otherwise have to be written out again in the type
  // list and again in the parent menu -- which is exactly how those two came to
  // disagree in the first place.
  //
  // No wrapper element. The two spans are laid out by whoever renders them, so
  // an option row stays one flex column and `scrollIntoView({block:'nearest'})`
  // still steps over whole options rather than over half of one.
  let {
    type = '',
    // what a blank type reads as, the caller's word rather than this file's --
    // the same argument `typeName` takes, so a menu entry and the chip it
    // becomes cannot spell an empty row two ways
    empty = '',
  } = $props()

  let parts = $derived(splitType(type))
</script>

<!-- never conditional: a type with no namespace would otherwise draw one line
     in a list of two-line ones, and a menu whose rows change height is the same
     defect as a row that twitches, one column over -->
<span class="ns truncate">{parts.ns || ' '}</span>
<span class="mono name truncate">{parts.name || empty}</span>

<style>
  /* `em` against the inherited body size, so this component must set no
     font-size of its own for it to mean what it means everywhere else */
  .ns { font-size: 0.5em; color: var(--muted); }
  /* The caller says what colour the name is -- a picked option is the accent --
     and, styles being scoped per component, has no way to reach in here and do
     it directly. So it comes across the boundary as a custom property, which is
     the one thing that does cross. */
  .name { min-width: 0; color: var(--typename-ink, inherit); }
</style>
