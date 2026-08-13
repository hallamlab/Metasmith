// The ink a DAG is drawn in, fetched once from the server.
//
// `metasmith.models.dag_renderer` defines the dark theme as the light one with
// only its colours replaced, precisely so a marker's shape, its scale and its
// stroke weight cannot differ between them -- the two are meant to be one
// drawing in two inks. Restating any of that in a stylesheet gives the guarantee
// away, so the page asks for it instead. Both themes arrive together because the
// theme toggle must not cost a round trip.
//
// One exception is deliberate. A hollow marker's fill is the plate's background
// hex in the served theme, because a file on disk cannot know what it will sit
// on; drawn here it sits on a card whose ground the page already owns, so the
// component substitutes that and the marker reads hollow wherever it is put.

import { api } from './api.svelte.js'

// a usable drawing before the fetch lands, and the one it falls back to if the
// route is unreachable -- shapes and weights matching `dag_renderer.STYLES`
const FALLBACK = {
  plate: { background: '#FFFFFF', edge: '#666666' },
  styles: {
    transform: {
      fill: '#FFFFFF', stroke: '#2B2B2B', text: '#7A7A7A', muted: '#A8A8A8',
      shape: 'triangle_down', marker_scale: 1.0, stroke_width: 1.5, rx: 0, solid: false,
    },
    data: {
      fill: '#FFFFFF', stroke: '#2B2B2B', text: '#111111', muted: '#8A8A8A',
      shape: 'circle', marker_scale: 1.0, stroke_width: 1.5, rx: 0, solid: false,
    },
    target: {
      fill: '#212121', stroke: '#2B2B2B', text: '#111111', muted: '#8A8A8A',
      shape: 'circle', marker_scale: 1.0, stroke_width: 3.0, rx: 0, solid: true,
    },
  },
}

const themes = $state({ light: FALLBACK, dark: FALLBACK })

let started = false

/** Both themes' ink. Reactive: the first read gets the fallback, and the
 *  component redraws itself once the real one lands. */
export function dagInk(theme) {
  if (!started) {
    started = true
    api
      .get('/dag/theme')
      .then((served) => {
        for (const name of Object.keys(themes)) {
          if (served?.[name]?.styles) themes[name] = served[name]
        }
      })
      .catch(() => {
        // a drawing in the fallback ink is a drawing; a blank panel is not
      })
  }
  return themes[theme === 'dark' ? 'dark' : 'light']
}
