import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// The bundle is generated, never committed: it lands directly in the python
// package so `msm gui` can serve it, and dev.sh --build-gui is what puts it
// there. See src/metasmith/gui/app.py for what happens when it is missing.
export default defineConfig({
  plugins: [svelte()],
  base: './',
  build: {
    outDir: '../src/metasmith/gui/static',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8090',
        changeOrigin: true,
      },
    },
  },
})
