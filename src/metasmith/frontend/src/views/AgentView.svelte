<script>
  import { api } from '../lib/api.svelte.js'
  import { attempt, loadAgents, select } from '../lib/state.svelte.js'
  import { agentPayload, formFromAgent, formProblems } from '../lib/agentform.js'
  import AgentFields from './AgentFields.svelte'
  import EditableName from '../components/EditableName.svelte'
  import Icon from '../components/Icon.svelte'
  import JobLog from '../components/JobLog.svelte'
  import SaveChip from '../components/SaveChip.svelte'
  import ShareOut from '../components/ShareOut.svelte'
  import Spinner from '../components/Spinner.svelte'
  import SplitButton from '../components/SplitButton.svelte'
  import StageProgress from '../components/StageProgress.svelte'

  let { name } = $props()

  let agent = $state(null)
  let form = $state(null)
  let runtimes = $state(['APPTAINER', 'DOCKER', 'MAMBA'])
  let defaultContainer = $state('')
  let jobId = $state(null)
  let jobStatus = $state(null)
  let jobPhase = $state(null)
  let ping = $state(null)
  let pinging = $state(false)
  let sharing = $state(false)

  // The same bar `solve` shows, over the phases `Agent.Deploy` actually walks
  // through (see the `PHASE:` markers `deploy_agent` emits in gui/api.py),
  // plus a trailing `deployed` segment that isn't a job phase at all -- it
  // reflects `agent.deployed`, the same field the page used to check for the
  // "nothing installed at this home yet" text. An all-idle bar (no job ever
  // run, not deployed) is what says "not deployed" now, instead of that text.
  const JOB_STAGES = ['connecting', 'provisioning', 'staging', 'finishing']
  const DEPLOY_STAGES = [...JOB_STAGES, 'deployed']
  let requestingDeploy = $state(false)
  let jobRunning = $derived(!!jobId && jobStatus !== 'done' && jobStatus !== 'failed')
  let deploying = $derived(requestingDeploy || jobRunning)
  let jobStage = $derived(Math.max(0, JOB_STAGES.indexOf(jobPhase)))
  let deployStageStates = $derived.by(() => {
    const jobStates = !jobId
      ? JOB_STAGES.map(() => 'idle')
      : JOB_STAGES.map((_, i) =>
          i < jobStage ? 'done'
          : i > jobStage ? 'idle'
          : jobStatus === 'failed' ? 'failed'
          : jobStatus === 'done' ? 'done'
          : 'running',
        )
    const deployedState = agent?.deployed ? 'done' : jobStatus === 'failed' ? 'failed' : 'idle'
    return [...jobStates, deployedState]
  })

  // The name is a field like any other -- `PUT /agents/<name>` carries the whole
  // object, and a name that differs from the url is a rename. So the agent's
  // file moves and its run records are re-pointed by the save, not by a second
  // gesture somewhere else. It is edited on the heading, the way a workflow's is.
  let dirty = $derived(
    !!agent && !!form && JSON.stringify(agentPayload(form)) !== JSON.stringify(agentPayload(formFromAgent(agent))),
  )

  // Two sources, and they answer different questions. The form's own problems
  // are live as you type; the server's are what it saw at the last save, and
  // only it can say whether a host exists. While the form is clean they are the
  // same list, so the saved one is only shown once there is nothing pending.
  let problems = $derived(
    form ? (dirty ? formProblems(form) : (agent?.problems ?? [])) : [],
  )

  function adopt(a) {
    agent = a
    form = formFromAgent(a)
    naming = null
  }

  // -- the name, and whether it still follows the host ------------------------
  //
  // An auto-named agent is `<word>-<host>`, and pointing it at another machine
  // renames it on save. Typing a name is what stops that, for good -- which is
  // right, and left no way back. This is the way back: the button asks the
  // server for a fresh name for whatever host the form currently names, and the
  // naming record that comes with it rides along on the save, which is what
  // re-arms the following.
  //
  // Held until the save rather than applied through one: the name is a field of
  // this form like any other, and a button that renamed the object under an
  // unsaved form would be the only control here that did not wait.
  let naming = $state(null)
  let regenerating = $state(false)

  // what the name would be *about* -- the host as the form has it now, which is
  // not what is on disk while the tab has been changed and not yet saved
  let formHost = $derived(form?.kind === 'ssh' ? (form.host ?? '').trim() : 'local')

  async function regenerate() {
    regenerating = true
    const out = await attempt(() =>
      api.get(`/defaults/agent/name?host=${encodeURIComponent(formHost || 'local')}`),
    )
    regenerating = false
    if (!out) return
    naming = { prefix: out.prefix, sort_name: out.sort_name }
    form.name = out.name
  }

  $effect(() => {
    const n = name
    clearTimeout(saveTimer)
    agent = null
    form = null
    jobId = null
    jobStatus = null
    jobPhase = null
    ping = null
    attempt(async () => {
      const d = await api.get('/defaults/agent').catch(() => null)
      if (d?.runtimes?.length) runtimes = d.runtimes
      if (d?.container) defaultContainer = d.container
      adopt(await api.get(`/agents/${n}`))
      // The bar picks up where the last deploy left off, even one started
      // from a tab that is gone -- jobs live on the server, keyed by agent,
      // so the most recent one is what was last true here, reload or not.
      if (n !== name) return
      const jobs = await api.get(`/jobs?agent=${encodeURIComponent(n)}`).catch(() => [])
      if (n === name && jobs?.length) jobId = jobs[0].id
    })
  })

  // What the save did that was not asked for: an auto-named agent pointed at a
  // different machine is renamed by the save itself, and being renamed behind
  // your back is only acceptable if you are told. Kept against the name it is
  // about rather than cleared on navigation, because the rename *is* a
  // navigation -- the note has to survive the reselect that follows it, and
  // nothing else.
  let notice = $state(null) // {name, notes}

  async function save() {
    await attempt(async () => {
      const next = await api.put(`/agents/${name}`, {
        ...agentPayload(form),
        // only when the name in the box came from the button rather than from
        // a keyboard: that is the difference between an auto name and yours
        ...(naming ? { naming } : {}),
      })
      notice = next.notes?.length ? { name: next.name, notes: next.notes } : null
      await loadAgents()
      // a rename moved the object; the rail and this view are keyed by name, so
      // the selection has to follow it or the next read is a 404
      if (next.name !== name) select('agents', next.name)
      else adopt(next)
    })
  }

  // Autosave, the same as the recipe: a write in flight and a write not yet
  // sent both count as unsaved, and the two are serialised through one chain
  // so a rename landing mid-edit cannot cross an edit sent a moment later.
  let writing = Promise.resolve()
  let saveTimer = null

  function persist() {
    clearTimeout(saveTimer)
    saveTimer = setTimeout(() => {
      writing = writing.then(save)
    }, 600)
  }

  // `dirty` already answers "does the form differ from what is on disk" --
  // both a keystroke and a button-driven change (a tab, a param row) show up
  // in it, so watching it here is enough to autosave either without wiring
  // every control underneath by hand.
  $effect(() => {
    if (dirty) persist()
    return () => clearTimeout(saveTimer)
  })

  async function doPing() {
    pinging = true
    ping = await attempt(() => api.post(`/agents/${name}/ping`))
    pinging = false
  }

  async function deploy(assertive = false) {
    requestingDeploy = true
    jobStatus = null
    jobPhase = null
    const job = await attempt(() =>
      api.post(`/agents/${name}/deploy`, assertive ? { assertive: true } : {}),
    )
    requestingDeploy = false
    if (job) jobId = job.id
  }

  async function unarchive() {
    await attempt(async () => {
      await api.post(`/agents/${name}/archive`, { archived: false })
      await loadAgents()
      adopt(await api.get(`/agents/${name}`))
    })
  }
</script>

{#if !agent || !form}
  <p class="muted">loading…</p>
{:else}
  <div class="col" style="gap:14px; max-width:760px">
    <div class="spread">
      <div class="row grow">
        <EditableName
          value={form.name}
          hint="enter to accept — the rename is saved automatically"
          title="rename this agent"
          oncommit={(next) => {
            // typed, so it is yours: it stops following the host, and a record
            // left over from the button must not ride along and re-arm it
            naming = null
            form.name = next
          }}
        />
        <button
          class="regen"
          disabled={regenerating}
          onclick={regenerate}
          title={`make up another name for ${formHost || 'this machine'} — and let it follow that host again`}
          aria-label="generate another name"
        >
          <Icon name="regenerate" size={13} />
        </button>
        <SaveChip {dirty} />
        {#if agent.archived_at}<span class="tag warn">archived</span>{/if}
      </div>
      <div class="row">
        {#if agent.archived_at}<button onclick={unarchive}>restore</button>{/if}
        <button onclick={() => (sharing = true)}>share</button>
        <button
          class="ping-btn"
          class:ok={!pinging && ping?.ok}
          class:bad={!pinging && ping && !ping.ok}
          onclick={doPing}
          disabled={pinging}
          title={pinging ? 'pinging…' : ping ? (ping.ok ? 'reachable' : 'no answer — click to retry') : 'ping this agent'}
        >
          {#if pinging}
            <Spinner />
          {:else if ping?.ok}
            <Icon name="check" size={11} />
          {:else if ping}
            <Icon name="x" size={11} />
          {:else}
            <span class="dot" aria-hidden="true"></span>
          {/if}
          ping
        </button>
        <SplitButton
          label={deploying ? 'deploying…' : 'deploy'}
          disabled={problems.length > 0 || deploying}
          onclick={() => deploy(false)}
          options={[{
            label: 'force redeploy',
            title: 'redeploy even if this agent already looks up to date',
            onclick: () => deploy(true),
          }]}
        />
      </div>
    </div>

    <AgentFields
      bind:form
      {runtimes}
      realPath={agent.real_path}
      presets={Object.keys(agent.config_presets ?? {})}
      {defaultContainer}
    />

    <!-- An agent is saveable long before it can be run on: you know it is going
         on a cluster days before the host exists. So what is missing is stated
         rather than enforced here, and enforced where it costs something --
         launching a run refuses the same list. -->
    {#if notice?.name === name}
      {#each notice.notes as note}
        <p class="small muted" style="margin:0">{note}</p>
      {/each}
    {/if}

    {#if problems.length}
      <div class="row small wrap incomplete">
        <span class="tag warn">incomplete</span>
        <span>{problems.join(' · ')}</span>
      </div>
    {/if}

    {#if ping}
      <div class="col" style="gap:6px">
        <div class="spread">
          <h3>ping</h3>
          <span class="tag" class:ok={ping.ok} class:bad={!ping.ok}>
            {ping.ok ? 'reachable' : 'no answer'}
          </span>
        </div>
        <pre class="log">{[...ping.out, ...ping.err].join('\n') || '(no output)'}</pre>
      </div>
    {/if}

    <StageProgress stages={DEPLOY_STAGES} stageStates={deployStageStates} />

    <JobLog
      {jobId}
      bind:status={jobStatus}
      bind:phase={jobPhase}
      onend={async () => {
        await loadAgents()
        adopt(await api.get(`/agents/${name}`))
      }}
    />

    {#if agent.runs?.length}
      <div class="col" style="gap:6px">
        <h3>runs on this agent</h3>
        <table class="small">
          <tbody>
            {#each agent.runs as r}
              <tr>
                <td>
                  <button class="link" onclick={() => select('runs', `${r.workflow}/${r.name}`)}>
                    {r.name}
                  </button>
                </td>
                <td class="muted">{r.workflow}</td>
                <td>{r.state}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>

  {#if sharing}
    <ShareOut kind="agent" name={agent.name} onclose={() => (sharing = false)} />
  {/if}
{/if}

<style>
  .link {
    background: none;
    border: none;
    color: var(--accent);
    padding: 0;
    text-align: left;
  }
  .link:hover { text-decoration: underline; border: none; }
  .incomplete { color: var(--warn, #efe0bc); }
  /* beside the name, at the weight of the chip on its other side: an offer, not
     an action on the object */
  .regen {
    display: flex;
    padding: 4px;
    background: none;
    border-color: transparent;
    color: var(--muted);
  }
  .regen:hover:not(:disabled) { color: var(--text); background: var(--panel-2); }

  /* the last result rides in the button itself, so you do not have to look
     away from it to see whether the ping landed */
  .ping-btn {
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .ping-btn.ok { border-color: var(--tag-ok-line, var(--ok)); color: var(--ok); }
  .ping-btn.bad { border-color: var(--tag-bad-line, var(--bad)); color: var(--bad); }
  .ping-btn .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: currentColor;
    opacity: 0.5;
  }
</style>
