<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt } from '../lib/state.svelte.js'
  import Modal from './Modal.svelte'

  // What gets saved is the recipe's shape, not this run's own files: every
  // input the workflow has becomes a fresh blank placeholder on the server
  // side (see `derive_template_library`), so nothing here has to ask about
  // that -- only what to call the result.
  let { workflowName, suggestedName, onclose, onsaved } = $props()

  let name = $state(suggestedName || workflowName)
  let description = $state('')
  let saving = $state(false)

  async function save() {
    saving = true
    const out = await attempt(() =>
      api.post(`/workflows/${workflowName}/save_as_template`, { name, description }),
    )
    saving = false
    if (out) {
      onsaved?.(out)
      onclose?.()
    }
  }
</script>

<Modal
  title="save as template"
  subtitle="a starting point to reuse -- inputs are saved as blanks, not as this run's own files"
  {onclose}
>
  <label class="col small">
    <span class="muted">name</span>
    <input class="mono" bind:value={name} placeholder="template name" />
  </label>

  <label class="col small">
    <span class="muted">description</span>
    <textarea rows="2" bind:value={description} placeholder="what this builds, for whoever picks it later"
    ></textarea>
  </label>

  {#snippet footer()}
    <button onclick={() => onclose?.()}>cancel</button>
    <button class="primary" disabled={saving || !name.trim()} onclick={save}>
      {saving ? 'saving…' : 'save'}
    </button>
  {/snippet}
</Modal>
