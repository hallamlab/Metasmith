import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

// The bundle is generated, never committed: it lands directly in the python
// package so `msm gui` can serve it, and dev.sh --build-gui is what puts it
// there. See src/metasmith/gui/app.py for what happens when it is missing.
export default defineConfig({
  plugins: [svelte()],
  base: './',
  resolve: {
    alias: {
      // The brand marks are one source of truth, and it is the sibling gui/icon
      // dir rather than a copy under frontend/. They are build-time
      // inputs: vite emits them into static/assets, which is the only tree
      // setup.py packages, so nothing extra has to ship them.
      $icon: fileURLToPath(new URL('../gui/icon', import.meta.url)),
    },
  },
  build: {
    outDir: '../gui/static',
    emptyOutDir: true,
  },
  server: {
    // icon/ sits outside the vite root, so the dev server has to be told it may
    // read it; the build resolves absolute paths without this.
    fs: { allow: ['..'] },
    proxy: {
      '/api': {
        target: 'http://127.0.0.1:8090',
        changeOrigin: true,
      },
    },
  },
})
