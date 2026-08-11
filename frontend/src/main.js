import { mount } from 'svelte'
import './app.css'
import App from './App.svelte'
import { applyTheme, watchOsTheme } from './lib/state.svelte.js'

// index.html already stamped the attribute so the first paint is right; this
// puts the same answer on `ui.theme`, which is what the page and the diagram
// read, and starts following the OS for as long as nobody has picked.
applyTheme()
watchOsTheme()

export default mount(App, { target: document.getElementById('app') })
