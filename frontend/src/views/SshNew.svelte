<script>
  import { api } from '../lib/api.js'
  import { attempt, loadSsh, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'
  import IdentityField from '../components/IdentityField.svelte'

  // Deliberately five fields. Anything more specific belongs in the config
  // editor, where the whole file is visible.
  //
  // Only the fields ssh itself has a default for carry a placeholder. On the
  // rest an example reads as a value that is already there -- and for a hostname
  // that is exactly the kind of thing someone saves without noticing.
  let form = $state({
    alias: '',
    hostname: '',
    user: '',
    port: '',
    proxy_jump: '',
    identity_file: '',
  })
  let shadowed = $state([])
  let key = $state(null)

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

<div class="col" style="gap:14px; max-width:720px">
  <h1>new host</h1>

  <div class="col" style="gap:10px">
    <Field label="alias" hint="the name you will type: ssh <alias>">
      <input bind:value={form.alias} />
    </Field>
    <Field label="hostname" hint="where it actually is — a dns name or an address">
      <input bind:value={form.hostname} />
    </Field>
    <Field label="user">
      <input bind:value={form.user} placeholder="(your local username)" />
    </Field>
    <Field label="port">
      <input bind:value={form.port} placeholder="22" />
    </Field>
    <Field label="jump host" hint="an alias to bounce through, if this host is not reachable directly">
      <input bind:value={form.proxy_jump} />
    </Field>

    <IdentityField
      alias={form.alias}
      bind:value={form.identity_file}
      identity={key}
      onchange={(k) => (key = k)}
    />

    {#if key}
      <div class="col keybox" style="gap:6px">
        <div class="spread">
          <h3>{key.created ? 'new key' : 'existing key — reused'}</h3>
          <span class="small muted mono truncate">{key.path}</span>
        </div>
        {#if !key.created}
          <p class="small muted">
            A key was already at that path. It is left exactly as it was — metasmith
            will not overwrite a key that might be the only way into a machine.
          </p>
        {/if}
        <p class="small muted">Put this public half in the host's authorized_keys:</p>
        <pre class="log pub">{key.public_key}</pre>
      </div>
    {/if}

    <div>
      <button class="primary" onclick={create} disabled={!form.alias.trim() || !form.hostname.trim()}>
        add host
      </button>
    </div>
  </div>

  {#if shadowed.length}
    <div class="col small">
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

<style>
  .keybox { border-left: 2px solid var(--line); padding-left: 10px; }
  .pub { max-height: none; word-break: break-all; }
</style>
