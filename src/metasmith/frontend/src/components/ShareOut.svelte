<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt } from '../lib/state.svelte.js'
  import Modal from './Modal.svelte'

  // Handing this object to somebody else. What is shown is what will be sent:
  // an export carries a home directory, setup commands, sometimes a cluster
  // account or a path to your own files, and the moment to notice that is
  // before it is on the clipboard rather than after it is in a chat window.
  let { kind, name, onclose } = $props()

  // Only a workflow has this axis: unbound is the recipe, with every input path
  // deferred, and bound is "run exactly this", which only means something to
  // someone holding the same files.
  let bound = $state(false)
  let out = $state(null)
  let copied = $state(false)

  $effect(() => {
    const b = bound
    attempt(async () => {
      out = await api.post('/share/export', { kind, name, bound: b })
    })
  })

  async function copy() {
    if (!out) return
    try {
      await navigator.clipboard.writeText(out.payload)
    } catch {
      // clipboard permission is refusable even on localhost
      const ta = document.createElement('textarea')
      ta.value = out.payload
      ta.style.cssText = 'position:fixed;opacity:0'
      document.body.appendChild(ta)
      ta.select()
      document.execCommand('copy')
      ta.remove()
    }
    copied = true
    setTimeout(() => (copied = false), 1200)
  }
</script>

<Modal
  title={`share ${name}`}
  subtitle="paste this to a colleague — they import it on their side"
  {onclose}
>
  {#if kind === 'workflow'}
    <label class="row small">
      <input type="checkbox" bind:checked={bound} />
      <span>include the input paths — only useful to someone with the same files</span>
    </label>
  {/if}

  <label class="col small">
    <span class="muted">the string</span>
    <textarea class="mono payload" readonly rows="4" value={out?.payload ?? 'preparing…'}></textarea>
  </label>

  <details open>
    <summary class="small muted">what is in it</summary>
    <pre class="small body">{out ? JSON.stringify(out.body, null, 2) : ''}</pre>
  </details>

  {#snippet footer()}
    <button onclick={() => onclose?.()}>close</button>
    <button class="primary" disabled={!out} onclick={copy}>{copied ? 'copied' : 'copy'}</button>
  {/snippet}
</Modal>

<style>
  .payload {
    word-break: break-all;
    resize: vertical;
  }
  .body {
    max-height: 260px;
    overflow: auto;
    margin: 6px 0 0;
    padding: 8px;
    background: var(--sunken);
    border: 1px solid var(--line);
    border-radius: var(--radius);
  }
</style>
