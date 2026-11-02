// A run is called `<workflow>-<nonce>` -- `blazing-ape-0XwE9` -- because in the
// Runs rail, which spans every workflow, the workflow half is the only thing
// telling two runs apart. Everywhere the workflow is already on the screen it is
// the same word twice, and the nonce is the whole of what the row says.
//
// The prefix is stripped rather than the last segment taken, so a run whose
// workflow was named with dashes does not lose half of it, and a name that does
// not start with its workflow (an older record, a hand-made directory) is
// returned whole rather than silently cut.
export function runSuffix(name, workflow) {
  if (!name) return ''
  const prefix = `${workflow ?? ''}-`
  return workflow && name.startsWith(prefix) ? name.slice(prefix.length) : name
}
