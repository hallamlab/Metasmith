<script>
  // What the page has done with what you typed, in one word beside the name.
  // Two states, and the point of it is that the *quiet* one is still drawn:
  // a chip that only appears when something is unsaved cannot be trusted for
  // the opposite claim, because "nothing there" is also what a bug looks like.
  let { dirty = false, title = null } = $props()
</script>

<span
  class="tag"
  class:bad={dirty}
  title={title ?? (dirty ? 'not written to disk yet' : 'everything here is on disk')}
>{dirty ? 'unsaved changes' : 'all changes saved'}</span>
