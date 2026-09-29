import { TUTORIALS } from './tutorials/index.js'
import { app, select, selectSection } from './state.svelte.js'

// Which tutorial is open and where in it you are. Kept across a reload because
// the steps between here and a result include a deploy and a run, and a reload
// in the middle of either should land you back on the step you were on.
//
// `links` is what a pass through a tutorial made -- the agent, workflow and
// run -- per tutorial, so a later step, a jump from the Tutorials tab or a
// reload can find them again. `tourCtx` below is only the in-memory half.
const KEY = 'metasmith.tour'

const findTutorial = (id) => TUTORIALS.find((t) => t.id === id) ?? null

function restore() {
  const empty = { id: null, step: 0, hidden: false, nav: null, links: {}, finished: [] }
  try {
    const raw = JSON.parse(localStorage.getItem(KEY) ?? 'null')
    if (!raw || typeof raw !== 'object') return empty
    const base = {
      ...empty,
      nav: raw.nav ?? null,
      links: raw.links && typeof raw.links === 'object' ? raw.links : {},
      finished: Array.isArray(raw.finished) ? raw.finished.filter((x) => typeof x === 'string') : [],
    }
    const t = findTutorial(raw.id)
    if (!t) return base
    // by id first, so a tutorial that gained or lost a step reopens where you were
    const byId = t.steps.findIndex((s) => s.id === raw.stepId)
    const step = byId >= 0 ? byId : Math.max(0, Math.min(t.steps.length - 1, Math.trunc(Number(raw.step)) || 0))
    return { ...base, id: raw.id, step, hidden: !!(raw.hidden ?? raw.minimized) }
  } catch {
    return empty
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
// when it opened, a later step reads it. Not persisted: a reload retakes the
// open step's snapshot (see `settleStep`), and a `done` that needs one stays
// false until then -- fail closed, since true would record whatever happens
// to be selected.
export const tourCtx = {}

function save() {
  try {
    const { id, step, hidden, nav, links, finished } = tour
    const stepId = currentStep()?.id ?? null
    localStorage.setItem(KEY, JSON.stringify({ id, step, stepId, hidden, nav, links, finished }))
  } catch {
    /* storage denied: the tour still runs, it just forgets on reload */
  }
}

export const tutorial = () => findTutorial(tour.id)
export const currentStep = () => tutorial()?.steps[tour.step] ?? null
export const linksOf = (id) => tour.links[id] ?? {}

function enter(direction) {
  tour.arrived = direction
  for (const k of ['touched', 'entered']) delete tourCtx[k]
  save()
}

/** Run the current step's `enter` once its page is open and its list loaded,
 *  and say whether it has run. A snapshot taken earlier -- on a reload, or a
 *  restart from the Tutorials tab before the page it names has loaded -- is of
 *  an empty list, and everything already there would then count as new. */
export function settleStep(step = currentStep()) {
  if (tourCtx.entered || !step) return true
  if (!step.enter) return (tourCtx.entered = true)
  const w = whereOf(step)
  if (w && !(app.section === w.section && app.loaded[w.section])) return false
  step.enter(tourCtx, app)
  return (tourCtx.entered = true)
}

/** Start `id` from its first step, forgetting what an earlier pass made. */
export function startTour(id) {
  for (const k of Object.keys(tourCtx)) delete tourCtx[k]
  tour.links = { ...tour.links, [id]: {} }
  tour.finished = tour.finished.filter((x) => x !== id)
  tour.id = id
  tour.step = 0
  tour.hidden = false
  enter('forward')
}

/** Open `id` at step `i`, keeping what it already made, and take the page to
 *  where that step happens. The recovery path: a restart from the middle. */
export function resumeTour(id, i = null) {
  const t = findTutorial(id)
  if (!t) return
  const same = tour.id === id
  if (!same) for (const k of Object.keys(tourCtx)) delete tourCtx[k]
  tour.step = Math.max(0, Math.min(t.steps.length - 1, i ?? (same ? tour.step : 0)))
  tour.id = id
  tour.hidden = false
  enter('jump')
  goWhere()
}

export function stopTour() {
  tour.id = null
  tour.step = 0
  save()
}

function finishTour() {
  if (tour.id && !tour.finished.includes(tour.id)) tour.finished = [...tour.finished, tour.id]
  stopTour()
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
  if (tour.step >= t.steps.length - 1) finishTour()
  else gotoStep(tour.step + 1, 'forward')
}

export function prevStep() {
  gotoStep(tour.step - 1, 'back')
}

export function setHidden(v) {
  tour.hidden = !!v
  save()
}

export function setNavPosition(pos, persist = true) {
  tour.nav = pos
  if (persist) save()
}

/** Record what the current step made, e.g. `{ agent: 'x' }`. */
export function addLinks(patch) {
  if (!tour.id || !patch) return
  const clean = Object.fromEntries(Object.entries(patch).filter(([, v]) => v))
  if (!Object.keys(clean).length) return
  tour.links = { ...tour.links, [tour.id]: { ...linksOf(tour.id), ...clean } }
  save()
}

/** Where the current step happens, if it says: the section, and the thing in
 *  it this pass made. */
export function whereOf(step = currentStep()) {
  if (!step?.where || !tour.id) return null
  try {
    return step.where(linksOf(tour.id)) ?? null
  } catch {
    return null
  }
}

export function goWhere(step = currentStep()) {
  const w = whereOf(step)
  if (!w) return false
  if (w.id) select(w.section, w.id)
  else selectSection(w.section)
  return true
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

// -- editing the text in place ---------------------------------------------
//
// `?tourEdit=1` makes every callout's prose editable. Edits are kept in this
// browser, keyed by tutorial and step id, shown only while the flag is on, and
// exported as JSON for whoever writes them back into the tutorial's source.

const EDITS_KEY = 'metasmith.tour.edits'

export const tourEdit = (() => {
  try {
    return new URLSearchParams(window.location.search).get('tourEdit') === '1'
  } catch {
    return false
  }
})()

function loadEdits() {
  try {
    const raw = JSON.parse(localStorage.getItem(EDITS_KEY) ?? '{}')
    return raw && typeof raw === 'object' ? raw : {}
  } catch {
    return {}
  }
}

export const edits = $state(tourEdit ? loadEdits() : {})

function saveEdits() {
  try {
    localStorage.setItem(EDITS_KEY, JSON.stringify(edits))
  } catch {
    /* storage denied: the edit lasts until reload */
  }
}

/** A step's text for `field` (and paragraph `i` of `body`), edited or not. */
export function stepText(tid, step, field, i = null) {
  const e = tourEdit ? edits[tid]?.[step.id] : null
  if (field === 'body') return e?.body?.[i] ?? step.body?.[i] ?? ''
  return e?.[field] ?? step[field] ?? ''
}

export function setStepText(tid, step, field, value, i = null) {
  const byStep = (edits[tid] ??= {})
  const e = (byStep[step.id] ??= {})
  if (field === 'body') {
    const body = e.body ?? [...(step.body ?? [])]
    body[i] = value
    e.body = body
    if (body.every((p, k) => p === step.body?.[k])) delete e.body
  } else if (value === (step[field] ?? '')) {
    delete e[field]
  } else {
    e[field] = value
  }
  if (!Object.keys(e).length) delete byStep[step.id]
  if (!Object.keys(byStep).length) delete edits[tid]
  saveEdits()
}

export function editCount(tid) {
  return Object.keys(edits[tid] ?? {}).length
}

export function clearEdits(tid) {
  delete edits[tid]
  saveEdits()
}

/** The edited steps of `tid` in step order, each field as it now reads. */
export function exportEdits(tid) {
  const t = findTutorial(tid)
  if (!t) return '[]'
  const out = t.steps
    .filter((s) => edits[tid]?.[s.id])
    .map((s) => ({ step: s.id, ...edits[tid][s.id] }))
  return JSON.stringify({ tutorial: tid, steps: out }, null, 2)
}
