// Ranking type names against what has been typed.
//
// A substring filter makes you spell the name the way its author did: nothing
// offers `sequences::flye_assembly` until you have typed a run of it exactly, so
// the namespace half you do not remember is in the way of the half you do. This
// matches the characters in order instead, and then *ranks*, because subsequence
// matching on its own is far too generous -- `gbk` is a subsequence of half the
// library, and the one name that actually spells it has to come first.
//
// Below all of that is one looser tier for a name the characters do not match
// in order at all -- a typo, which is the case in-order matching is blindest to.
//
// Ranking is a display concern only. Whether a type exists is still an exact
// membership test everywhere it matters (`known` in the builder); a name that
// merely ranked well is not a name you can register.

import { typeName } from './types.js'

const WORD = /[a-z0-9]/i

// Where a word starts: the first character, anything after a separator (`::`,
// `_`, `-`, `.`), and a capital following a lower-case letter, so camelCase
// splits the way a reader does it.
function boundaries(name) {
  const out = new Array(name.length)
  for (let i = 0; i < name.length; i++) {
    const c = name[i]
    const prev = i > 0 ? name[i - 1] : ''
    out[i] =
      i === 0 ||
      !WORD.test(prev) ||
      (c >= 'A' && c <= 'Z' && prev >= 'a' && prev <= 'z')
  }
  return out
}

// how a name matched, best first
const EXACT = 0
const PREFIX = 1
const BARE_NAME = 2 // the type's own name, with only the namespace left off
const WHOLE_WORD = 3 // a literal run that is exactly one word of the name
const WORD_START = 4 // a literal run, beginning at the start of a word
const SUBSTRING = 5 // a literal run, beginning mid-word
const SCATTERED = 6 // the characters in order, with other characters between
const APPROX = 7 // not even in order: a window of the name it is a mistyping of

// How far a query may be off and still be offered. Deliberately zero below four
// characters: at one error a three-character window matches most of the
// library, and the tiers above already answer a short query well.
const budget = (q) => Math.floor(q.length / 4)

// Whitespace is dropped rather than matched: "flye asm" is someone typing two
// halves of a name they half remember, not a name with a space in it.
export function normalize(query) {
  return (query ?? '').replace(/\s+/g, '').toLowerCase()
}

/**
 * How well `name` answers `query`, or null when it does not at all.
 *
 * The result is a sort key, not a number: tier first, then how tightly the
 * matched characters sit together (word starts earn their keep here), then how
 * early the match begins, then the shorter name. Every term is deterministic,
 * so equal candidates keep their order between keystrokes instead of shuffling.
 */
export function score(name, query) {
  const q = normalize(query)
  if (!q) return { tier: EXACT, spread: 0, first: 0, length: name.length }
  const lower = name.toLowerCase()
  if (lower === q) return { tier: EXACT, spread: 0, first: 0, length: name.length }

  const bounds = boundaries(name)
  const at = lower.indexOf(q)
  if (at >= 0) {
    // `assembly` should reach `sequences::assembly` first: there the query *is*
    // the type's name and only the namespace was left off. `assembly_accession`
    // merely contains it as a word, which in turn beats a run starting mid-word.
    const end = at + q.length
    const bare = end === name.length && typeName(lower) === q
    const whole = bounds[at] && (end === name.length || bounds[end] || !WORD.test(name[end]))
    const tier = bare
      ? BARE_NAME
      : at === 0
        ? PREFIX
        : whole
          ? WHOLE_WORD
          : bounds[at]
            ? WORD_START
            : SUBSTRING
    return { tier, spread: 0, first: at, length: name.length }
  }

  const hits = subsequence(lower, q)
  if (!hits) {
    // Nothing in order, which is exactly what a transposed character does:
    // `paried` has no `i` after its `r`, so every `paired…` name fails outright
    // rather than merely ranking badly. One looser question before it is
    // dropped -- and only here, since a name that matched in order has already
    // been answered better than this could.
    const max = budget(q)
    const off = max ? nearestWindow(lower, q, max) : null
    return off === null ? null : { tier: APPROX, spread: off, first: 0, length: name.length }
  }
  // characters that had to be skipped over inside the match, less a discount for
  // every one that landed on the start of a word -- `sfa` picking out
  // s(equences)::f(lye)_a(ssembly) is a better answer than three letters that
  // happen to fall in that order in the middle of one word
  const gaps = hits[hits.length - 1] - hits[0] + 1 - q.length
  const onWord = hits.reduce((n, i) => n + (bounds[i] ? 1 : 0), 0)
  return { tier: SCATTERED, spread: gaps - 3 * onWord, first: hits[0], length: name.length }
}

// Every character of `q` in order, packed as tightly as possible: matched left
// to right to find out whether it fits at all, then re-matched right to left
// from where that ended, which pulls the run in off the loose early hits.
function subsequence(lower, q) {
  let at = 0
  for (const c of q) {
    at = lower.indexOf(c, at)
    if (at < 0) return null
    at += 1
  }
  const hits = new Array(q.length)
  let end = at - 1
  for (let k = q.length - 1; k >= 0; k--) {
    end = lower.lastIndexOf(q[k], end)
    hits[k] = end
    end -= 1
  }
  return hits
}

/**
 * How far `q` is from the closest window of `name` -- or null past `max`.
 *
 * Approximate *substring*, not edit distance against the whole name: a query is
 * a fragment, so `paried` against `std::paired_reads_forward` is six characters
 * against twenty and any whole-string distance is about fourteen however good
 * the match is. Zeroing the first row of the matrix is what lets the pattern
 * begin anywhere in the name and so measures the window instead.
 *
 * With a transposition rule, because two adjacent characters swapped is one
 * slip of the fingers and costs two under plain edit distance -- and that is
 * the whole of the `paried` case.
 *
 * The name is the full `namespace::type`, so a window may straddle the `::`
 * rather than the separator being what pushes it over budget.
 */
function nearestWindow(lower, q, max) {
  const n = lower.length
  let prev2 = null
  let prev = new Array(n + 1).fill(0) // the pattern may begin anywhere
  for (let i = 1; i <= q.length; i++) {
    const row = new Array(n + 1)
    row[0] = i
    let best = i
    for (let j = 1; j <= n; j++) {
      const cost = q[i - 1] === lower[j - 1] ? 0 : 1
      let v = Math.min(prev[j] + 1, row[j - 1] + 1, prev[j - 1] + cost)
      if (i > 1 && j > 1 && q[i - 1] === lower[j - 2] && q[i - 2] === lower[j - 1]) {
        v = Math.min(v, prev2[j - 2] + 1)
      }
      row[j] = v
      if (v < best) best = v
    }
    // A row's best never beats the one above it, so a row already over budget
    // settles the name. This is the whole of the cost control: the fallback runs
    // only where the subsequence match failed, which on a garbage query is every
    // name in the library.
    if (best > max) return null
    prev2 = prev
    prev = row
  }
  return Math.min(...prev)
}

function compare(a, b) {
  return (
    a.score.tier - b.score.tier ||
    a.score.spread - b.score.spread ||
    a.score.first - b.score.first ||
    a.score.length - b.score.length ||
    (a.name < b.name ? -1 : a.name > b.name ? 1 : 0)
  )
}

/** The names that match, best first. An empty query keeps the list as it came. */
export function rank(names, query) {
  if (!normalize(query)) return names
  const hits = []
  for (const name of names) {
    const s = score(name, query)
    if (s) hits.push({ name, score: s })
  }
  hits.sort(compare)
  return hits.map((h) => h.name)
}
