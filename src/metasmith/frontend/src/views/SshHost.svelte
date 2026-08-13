<script>
  import { api } from '../lib/api.svelte.js'
  import { app, attempt, loadSsh, notify, select } from '../lib/state.svelte.js'
  import Field from '../components/Field.svelte'
  import IdentityField from '../components/IdentityField.svelte'
  import ShareOut from '../components/ShareOut.svelte'

  let { alias } = $props()

  let sharing = $state(false)

  let host = $derived(app.hosts.find((h) => h.alias === alias) ?? null)
  let draft = $state({
    alias: '', hostname: '', user: '', port: '', proxy_jump: '', identity_file: '',
  })
  let lastLoaded = $state(null)
  let identity = $state(null)

  $effect(() => {
    if (host && lastLoaded !== host.alias) {
      lastLoaded = host.alias
      draft = {
        alias: host.alias,
        hostname: host.hostname ?? '',
        user: host.user ?? '',
        port: host.port ?? '',
        proxy_jump: host.proxy_jump ?? '',
        identity_file: host.identity_file ?? '',
      }
    }
  })

  // The public key is read from disk, so it is fetched rather than derived from
  // the host list. Only the public half ever leaves the server.
  // `quiet`: this runs on mount, including the mount caused by a rename, and a
  // background read must not wipe what the rename just reported
  $effect(() => {
    const a = alias
    identity = null
    attempt(
      async () => (identity = (await api.get(`/ssh/hosts/${a}/identity`)).identity),
      { quiet: true },
    )
  })

  async function refreshIdentity() {
    identity = (await api.get(`/ssh/hosts/${alias}/identity`)).identity
  }

  // One PUT carrying the whole host, alias included -- the same convention the
  // agent and the workflow are saved by. An alias that differs is a rename, and
  // the server re-points every agent whose home was on the old name; it says
  // which, because that is an edit to objects the user is not looking at.
  async function save() {
    const out = await attempt(async () => {
      const body = await api.put(`/ssh/hosts/${alias}`, draft)
      await loadSsh()
      return body
    })
    if (!out) return
    if (out.renamed_from) {
      // the alias is the route: reselect, or the pane reloads against a name
      // that is no longer in the config
      select('ssh', `host:${out.host.alias}`)
      // written after `attempt` has resolved, not inside it -- see `attempt`
      if (out.agents_repointed?.length) {
        notify(
          `[${out.renamed_from}] is now [${out.host.alias}]; ` +
            `re-pointed agent(s) ${out.agents_repointed.join(', ')}`,
          'info',
        )
      }
      return
    }
    await refreshIdentity()
  }

  // Deleting the key that a saved host points at would otherwise leave the
  // config naming a file that is no longer there, so the removal is written
  // through immediately rather than waiting for a save the user may not make.
  async function onIdentityChange(key) {
    identity = key
    if (key === null) await save()
  }
</script>

{#if !host}
  <p class="muted">no host named [{alias}]</p>
{:else}
  <div class="col" style="gap:14px; max-width:760px">
    <div class="spread">
      <h1>{host.alias}</h1>
      <div class="row">
        <button class="small" onclick={() => (sharing = true)}>share</button>
        <span class="tag" class:ok={host.managed}>
          {host.managed ? 'managed by metasmith' : 'yours'}
        </span>
      </div>
    </div>
    <p class="small muted mono">{host.source}:{host.line}</p>

    {#if !host.managed}
      <div class="col">
        <p class="small">
          This host is defined outside the metasmith block, so it is shown but not
          edited here — metasmith never rewrites an entry it does not own. Change
          it in the config editor, where the native half of the file is editable.
        </p>
        <table class="mono small">
          <tbody>
            {#each Object.entries(host.keywords) as [k, v]}
              <tr><td class="muted">{k}</td><td>{v}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else}
      <div class="col" style="gap:10px">
        <Field label="alias" hint="what you type after `ssh`, and what an agent's home names">
          <input bind:value={draft.alias} />
        </Field>
        <Field label="hostname">
          <input bind:value={draft.hostname} />
        </Field>
        <Field label="user">
          <input bind:value={draft.user} placeholder="(your local username)" />
        </Field>
        <Field label="port">
          <input bind:value={draft.port} placeholder="22" />
        </Field>
        <Field label="jump host" hint="an alias to bounce through, if this host is not reachable directly">
          <input bind:value={draft.proxy_jump} />
        </Field>
        <IdentityField
          {alias}
          bind:value={draft.identity_file}
          {identity}
          onchange={onIdentityChange}
        />
        <div class="row">
          <button class="primary" onclick={save}>save</button>
          <span class="small muted">anything beyond these fields belongs in the config editor</span>
        </div>
      </div>
    {/if}

    {#if identity}
      <div class="col keybox" style="gap:6px">
        <div class="spread">
          <h3>identity</h3>
          <div class="row">
            {#if identity.generated}<span class="tag">generated</span>{/if}
            <span class="tag" class:bad={!identity.exists} class:ok={identity.exists}>
              {identity.exists ? 'present' : 'missing'}
            </span>
          </div>
        </div>
        <table class="small">
          <tbody>
            <tr><td class="muted">as written</td><td class="mono">{identity.value}</td></tr>
            <tr><td class="muted">resolves to</td><td class="mono">{identity.path}</td></tr>
          </tbody>
        </table>
        {#if identity.public_key}
          <p class="small muted">
            Public half — this is what belongs in the host's authorized_keys:
          </p>
          <pre class="log pub">{identity.public_key}</pre>
        {:else if identity.exists}
          <p class="small muted">
            No <span class="mono">.pub</span> beside the private key, so there is nothing
            to show. Regenerate it with
            <span class="mono">ssh-keygen -y -f {identity.path}</span>.
          </p>
        {:else}
          <p class="small muted">
            Nothing at that path. ssh will fall back to its usual keys, which may or
            may not be what you want.
          </p>
        {/if}
      </div>
    {/if}
  </div>

  {#if sharing}
    <ShareOut kind="ssh_host" name={host.alias} onclose={() => (sharing = false)} />
  {/if}
{/if}

<style>
  .keybox { border-left: 2px solid var(--line); padding-left: 10px; }
  .pub { max-height: none; word-break: break-all; }
</style>
