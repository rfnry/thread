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
  document.cookie = `clinic=${$('clinic').value}; path=/; samesite=strict`
  start()
}
if (cookie('name') && cookie('clinic')) start()

async function start() {
  $('signin').hidden = true
  $('app').hidden = false
  const headers = { 'X-Tenant': cookie('clinic'), 'content-type': 'application/json' }
  const post = async (path, body = {}) =>
    (await fetch(path, { method: 'POST', headers, body: JSON.stringify(body) })).json()
  const client = await connect({ url: `ws://${location.host}/ws` })
  let open = null

  async function threads() {
    $('threads').replaceChildren()
    for (const thread of await client.listThreads()) {
      const item = element('li')
      item.append(
        element('button', { textContent: thread.metadata.patient, onclick: () => show(thread) })
      )
      $('threads').append(item)
    }
  }

  function header() {
    $('title').textContent = open.metadata.patient
    $('handled').textContent = `Handled by ${open.metadata.handled_by}`
    $('handoff').hidden = open.metadata.handled_by === 'people'
  }

  async function show(thread) {
    open = thread
    header()
    $('thread').hidden = false
    $('log').replaceChildren()
    await client.join(thread.id, { since: 0 })
  }

  client.subscribe(
    (item) => {
      if (item.kind === 'invited') threads()
      if (item.thread_id !== open?.id) return
      if (item.kind === 'message') {
        const p = element('p', { className: item.author.role, textContent: text(item.content) })
        p.dataset.who = item.author.name
        $('log').append(p)
      }
      if (item.kind === 'working') $('status').textContent = `${item.sender.name}: ${item.label}`
      if (item.kind === 'idle') $('status').textContent = ''
      if (item.kind === 'thread_updated') {
        open.metadata = item.metadata
        header()
      }
    },
    { includeOwn: true }
  )

  $('open').onsubmit = async (event) => {
    event.preventDefault()
    await show(await post('/threads', { patient: $('patient').value.trim() }))
    $('patient').value = ''
  }
  $('handoff').onclick = () => post(`/threads/${open.id}/handoff`)
  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const said = $('text').value
    $('text').value = ''
    await client.send(open.id, said)
  }
  await threads()
}
