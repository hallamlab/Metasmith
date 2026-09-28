import { TUTORIALS } from './tutorials/index.js'

// Which tutorial is open and where in it you are. Kept across a reload because
// the steps between here and a result include a deploy and a run, and a reload
// in the middle of either should land you back on the step you were on.
const KEY = 'metasmith.tour'

function restore() {
  try {
    const raw = JSON.parse(localStorage.getItem(KEY) ?? 'null')
    if (raw && TUTORIALS.some((t) => t.id === raw.id)) {
      return { id: raw.id, step: Number(raw.step) || 0, minimized: !!raw.minimized, nav: raw.nav ?? null }
    }
    return { id: null, step: 0, minimized: false, nav: raw?.nav ?? null }
  } catch {
    return { id: null, step: 0, minimized: false, nav: null }
  }
}

export const tour = $state({
  ...restore(),
  // 'forward' when the step was reached by moving on, anything else when it was
  // revisited. Only a forward arrival may be carried past by its own `done`: a
  // step you stepped back onto would otherwise bounce you straight off it.
  arrived: 'restore',
})

// Scratch shared by one pass through a tutorial -- a step records what existed
// when it opened, a later step reads it. Not persisted: a reload starts clean
// and each step's `done` has to hold up without it.
export const tourCtx = {}

function save() {
  try {
    localStorage.setItem(
      KEY,
      JSON.stringify({ id: tour.id, step: tour.step, minimized: tour.minimized, nav: tour.nav }),
    )
  } catch {
    /* storage denied: the tour still runs, it just forgets on reload */
  }
}

export const tutorial = () => TUTORIALS.find((t) => t.id === tour.id) ?? null
export const currentStep = () => tutorial()?.steps[tour.step] ?? null

function enter(direction) {
  tour.arrived = direction
  currentStep()?.enter?.(tourCtx)
  save()
}

export function startTour(id) {
  for (const k of Object.keys(tourCtx)) delete tourCtx[k]
  tour.id = id
  tour.step = 0
  tour.minimized = false
  enter('forward')
}

export function stopTour() {
  tour.id = null
  tour.step = 0
  save()
}

export function gotoStep(i, direction = 'jump') {
  const t = tutorial()
  if (!t) return
  tour.step = Math.max(0, Math.min(t.steps.length - 1, i))
  enter(direction)
}

export function nextStep() {
  const t = tutorial()
  if (!t) return
  if (tour.step >= t.steps.length - 1) stopTour()
  else gotoStep(tour.step + 1, 'forward')
}

export function prevStep() {
  gotoStep(tour.step - 1, 'back')
}

export function setMinimized(v) {
  tour.minimized = !!v
  save()
}

export function setNavPosition(pos) {
  tour.nav = pos
  save()
}

/** The element a step points at, or null while it is not on screen.
 *
 *  A selector or a function; a function is for the targets a selector cannot
 *  say, like "the recipe row whose type is X". Something with no box -- behind
 *  a closed `details`, `display: none` -- counts as absent, because a ring drawn
 *  around a zero rectangle points at nothing. */
export function resolveTarget(target) {
  if (!target) return null
  let el = null
  try {
    el = typeof target === 'function' ? target() : document.querySelector(target)
  } catch {
    return null
  }
  if (!el) return null
  const r = el.getBoundingClientRect()
  return r.width > 0 || r.height > 0 ? el : null
}
