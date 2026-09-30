import { connect } from '/client/index.js'

const $ = (id) => document.getElementById(id)
const cookie = (key) =>
  decodeURIComponent(document.cookie.match(`(?:^|; )${key}=([^;]*)`)?.[1] ?? '')
const text = (content) =>
  content
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('\n')
const element = (tag, props) => Object.assign(document.createElement(tag), props)

$('signin').onsubmit = (event) => {
  event.preventDefault()
  document.cookie = `name=${encodeURIComponent($('name').value.trim())}; path=/; samesite=strict`
  document.cookie = `plant=${$('plant').value}; path=/; samesite=strict`
  start()
}
if (cookie('name') && cookie('plant')) start()

async function start() {
  $('signin').hidden = true
  $('app').hidden = false
  const plant = cookie('plant')
  const api = async (path, body) => {
    const init = { headers: { 'X-Tenant': plant, 'content-type': 'application/json' } }
    if (body) Object.assign(init, { method: 'POST', body: JSON.stringify(body) })
    return (await fetch(path, init)).json()
  }
  const client = await connect({ url: `ws://${location.host}/ws` })
  let open = null
  const here = new Map()

  async function channels() {
    const mine = await client.listThreads()
    $('channels').replaceChildren()
    for (const channel of await api('/channels')) {
      const threads = new Map()
      for (const thread of [...mine, ...channel.elevated])
        if (thread.metadata.line === channel.line) threads.set(thread.id, thread)
      const list = element('ul')
      for (const thread of threads.values()) {
        const label = thread.metadata.title + (thread.metadata.elevated ? ' · elevated' : '')
        const item = element('li')
        item.append(element('button', { textContent: label, onclick: () => show(thread) }))
        list.append(item)
      }
      const form = element('form')
      form.append(
        element('input', { placeholder: `Ask about ${channel.line}`, required: true }),
        element('select', {
          innerHTML:
            '<option value="">question</option><option>troubleshoot</option><option>procedure</option>',
        }),
        element('button', { textContent: 'Ask' })
      )
      form.onsubmit = async (event) => {
        event.preventDefault()
        const [question, task] = form.elements
        const thread = await api('/threads', {
          line: channel.line,
          title: question.value,
          task: task.value || null,
        })
        await show(thread)
        await client.send(thread.id, question.value)
        await channels()
      }
      $('channels').append(
        element('h3', { textContent: channel.line }),
        element('small', { textContent: channel.machines }),
        list,
        form
      )
    }
  }

  function header() {
    $('title').textContent = `${open.metadata.line}: ${open.metadata.title}`
    $('elevate').hidden = open.metadata.elevated || open.metadata.technician !== client.identity.id
    $('here').textContent = `In this thread now: ${[...here.values()].join(', ')}`
  }

  async function show(thread) {
    if (thread.metadata.elevated) await api(`/threads/${thread.id}/join`, {})
    open = thread
    $('thread').hidden = false
    $('log').replaceChildren()
    here.clear()
    await client.join(thread.id, { since: 0 })
    for (const who of await client.present(thread.id)) here.set(who.id, who.name)
    header()
  }

  client.subscribe(
    (item) => {
      if (item.kind === 'invited') channels()
      if (item.thread_id !== open?.id) return
      if (item.kind === 'message') {
        const p = element('p', { className: item.author.role, textContent: text(item.content) })
        p.dataset.who = item.author.name
        $('log').append(p)
      }
      if (item.kind === 'step') {
        $('log').append(
          element('div', {
            className: 'step',
            textContent: `· ${item.name} ${JSON.stringify(item.input)}`,
          })
        )
      }
      if (item.kind === 'working') $('status').textContent = `${item.sender.name}: ${item.label}`
      if (item.kind === 'idle') $('status').textContent = ''
      if (item.kind === 'thread_updated') open.metadata = item.metadata
      if (item.kind === 'present') here.set(item.sender.id, item.sender.name)
      if (item.kind === 'left') here.delete(item.sender.id)
      header()
    },
    { includeOwn: true }
  )

  $('elevate').onclick = async () => {
    open = await api(`/threads/${open.id}/elevate`, {})
    header()
  }
  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const said = $('text').value
    $('text').value = ''
    await client.send(open.id, said)
  }
  await channels()
}
