// Two readings of one instant. The server stores an ISO string in UTC -- which
// is the right thing to store and the wrong thing to print -- so nothing on the
// page shows it raw: the delta is what you read, and the stamp is what you check.
//
// No library. `Intl.RelativeTimeFormat` is in every browser and does the whole
// hard half -- the wording, the pluralisation, the locale -- leaving the unit
// ladder below, which is ten lines. The app has no runtime dependencies at all,
// and date-fns would be the first, for something the platform already ships.

const rtf = new Intl.RelativeTimeFormat(undefined, { numeric: 'auto' })

// Largest unit whose delta is at least one, floored. Ordered coarse to fine so
// the first match wins; a week is deliberately absent, because "3 weeks ago"
// and "21 days ago" are the same fact and days are the one people count.
const LADDER = [
  ['year', 365 * 24 * 3600],
  ['month', 30 * 24 * 3600],
  ['day', 24 * 3600],
  ['hour', 3600],
  ['minute', 60],
]

// Under a minute there is no unit worth naming: "in 4 seconds" for something
// that just happened reads as a bug, and clock skew between a server and a
// browser routinely puts a fresh timestamp a second or two in the future.
const JUST_NOW = 'just now'

function parse(iso) {
  if (!iso) return null
  const t = new Date(iso)
  return Number.isNaN(t.getTime()) ? null : t
}

const pad = (n) => String(n).padStart(2, '0')

/** `2026-07-26-14:32:11`, in local time, 24-hour. Empty for a missing one. */
export function stamp(iso) {
  const t = parse(iso)
  if (!t) return ''
  return (
    `${t.getFullYear()}-${pad(t.getMonth() + 1)}-${pad(t.getDate())}` +
    `-${pad(t.getHours())}:${pad(t.getMinutes())}:${pad(t.getSeconds())}`
  )
}

/**
 * `3 days ago`, `in 2 minutes`, `just now`. An em dash for a missing one --
 * a run that has not finished has no finish time, and that is not an error.
 */
export function ago(iso, now = Date.now()) {
  const t = parse(iso)
  if (!t) return '—'
  const seconds = (t.getTime() - now) / 1000
  const size = Math.abs(seconds)
  if (size < 60) return JUST_NOW
  for (const [unit, span] of LADDER) {
    if (size < span) continue
    return rtf.format(Math.trunc(seconds / span), unit)
  }
  return JUST_NOW
}

// One interval for the whole page, not one per mounted component: a run list is
// twenty of these, and twenty timers that all say the same thing is twenty
// timers. Components read `clock.now` and re-render when it moves.
//
// A minute is the resolution of the coarsest thing this prints -- nothing below
// "1 minute ago" is ever shown -- so ticking faster would be redraws nobody can
// see. It is deliberately never cleared: the module lives as long as the page.
const TICK_MS = 30_000

export const clock = $state({ now: Date.now() })
setInterval(() => (clock.now = Date.now()), TICK_MS)
