<script>
  import { api } from '../lib/api.js'
  import { attempt, loadWorkflows, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'

  let name = $state('')

  async function create() {
    const out = await attempt(async () => {
      const body = await api.post('/workflows', name.trim() ? { name } : {})
      await loadWorkflows()
      return body
    })
    if (out) select('workflows', out.name)
  }
</script>

<div class="col" style="gap:14px; max-width:520px">
  <h1>new workflow</h1>
  <div class="card col" style="gap:10px">
    <Field label="name" hint="leave blank and one will be made up — blazing-ape, and so on">
      <input bind:value={name} placeholder="(auto)" onkeydown={(e) => e.key === 'Enter' && create()} />
    </Field>
    <div><button class="primary" onclick={create}>create</button></div>
  </div>
  <p class="small muted">
    A workflow is a set of inputs and the types you want out of them. It is not
    tied to any one agent — once it plans, you can run the same workflow on any
    of them.
  </p>
</div>
