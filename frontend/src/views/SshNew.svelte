<script>
  import { api } from '../lib/api.js'
  import { attempt, loadSsh, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'

  // Deliberately four fields. Anything more specific belongs in the config
  // editor, where the whole file is visible.
  let form = $state({ alias: '', hostname: '', user: '', port: '', proxy_jump: '' })
  let shadowed = $state([])

  async function create() {
    const out = await attempt(async () => {
      const body = await api.post('/ssh/hosts', form)
      await loadSsh()
      return body
    })
    if (!out) return
    shadowed = out.shadowed_by ?? []
    select('ssh', `host:${out.host.alias}`)
  }
</script>

<div class="col" style="gap:14px; max-width:560px">
  <h1>new host</h1>
  <div class="card col" style="gap:10px">
    <Field label="alias" hint="the name you will type: ssh <alias>">
      <input bind:value={form.alias} placeholder="sockeye" />
    </Field>
    <Field label="hostname">
      <input bind:value={form.hostname} placeholder="sockeye.example.org" />
    </Field>
    <Field label="user">
      <input bind:value={form.user} placeholder="(your local username)" />
    </Field>
    <Field label="port">
      <input bind:value={form.port} placeholder="22" />
    </Field>
    <Field label="jump host" hint="an alias to bounce through, if this host is not reachable directly">
      <input bind:value={form.proxy_jump} placeholder="(none)" />
    </Field>
    <div>
      <button class="primary" onclick={create}>add host</button>
    </div>
  </div>

  {#if shadowed.length}
    <div class="card col small">
      <strong>note</strong>
      <span>
        These wildcard blocks also match this alias:
        {#each shadowed as s}<span class="mono"> {s.alias}</span>{/each}.
        The metasmith block is written above them, so its settings win — but the
        keywords it does not set will still come from there.
      </span>
    </div>
  {/if}

  <p class="small muted">
    An alias that already exists anywhere in your config, or in a file it
    includes, is refused rather than merged — metasmith only ever edits its own
    block.
  </p>
</div>
