// One way to print a type name.
//
// A type is `namespace::name`, and the page was printing it in three places --
// the type list, the parent menu, and the chips beside it -- each with its own
// copy of the split, which is why the two halves of the recipe card could read
// differently at all. The drawing engine prints it in a fourth
// (`models/dag_draw.py:default_label`); what has to agree between that and this
// is where the cut falls, so this cuts where it does: at the *first* `::`. A
// namespace is a source file's stem and holds no `::` today, so the two can
// only disagree over a name neither can be given -- but one of them has to be
// the rule, and the engine is the one that also has to draw it.

export function splitType(type) {
  const t = String(type ?? '').trim()
  const cut = t.indexOf('::')
  return cut > 0 ? { ns: t.slice(0, cut), name: t.slice(cut + 2) } : { ns: '', name: t }
}

/**
 * The half of a type name that tells two of them apart.
 *
 * The namespace is shared by every type in a library, so it is the half that
 * never does -- which is why a parent chip states only this, and leaves saying
 * *which row* to the hover highlight.
 *
 * `empty` is the caller's rather than baked in, because the two callers want
 * opposite things from a blank: a chip says `<empty>`, since an em dash there
 * reads as "no parent", which is the opposite of the truth; a field says
 * nothing at all, so its own placeholder shows through.
 */
export const typeName = (type, empty = '') => splitType(type).name || empty
