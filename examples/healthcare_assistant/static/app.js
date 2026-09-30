import { connect } from '/client/index.js'

const $ = (id) => document.getElementById(id)
const element = (tag, props) => Object.assign(document.createElement(tag), props)
const text = (content) =>
  content
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('\n')
const post = async (path, body) =>
  (
    await fetch(path, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    })
  ).json()

$('signin').onsubmit = (event) => {
  event.preventDefault()
  document.cookie = `name=${encodeURIComponent($('name').value.trim())}; path=/; samesite=strict`
  start()
}
if (/(^|; )name=/.test(document.cookie)) start()

async function start() {
  $('signin').hidden = true
  $('app').hidden = false
  const client = await connect({ url: `ws://${location.host}/ws` })
  let open = null

  client.subscribe(
    (item) => {
      if (item.thread_id !== open?.id) return
      if (item.kind === 'message') {
        const p = element('p', { className: item.author.role, textContent: text(item.content) })
        p.dataset.who = item.author.name
        $('log').append(p)
      }
      if (item.kind === 'member_added')
        $('log').append(element('div', { className: 'joined', textContent: `${item.identity.name} joined` }))
      if (item.kind === 'working') $('status').textContent = `${item.sender.name}: ${item.label}`
      if (item.kind === 'idle') $('status').textContent = ''
    },
    { includeOwn: true }
  )

  async function show(thread) {
    if (open) await client.leave(open.id)
    open = thread
    $('title').textContent = thread.metadata.patient
    $('log').replaceChildren()
    $('thread').hidden = false
    await client.join(thread.id, { since: 0 })
  }

  async function patients() {
    $('threads').replaceChildren()
    for (const thread of await (await fetch('/threads')).json()) {
      const join = async () => {
        await post(`/threads/${thread.id}/join`)
        await show(thread)
      }
      const item = element('li')
      item.append(element('button', { textContent: thread.metadata.patient, onclick: join }))
      $('threads').append(item)
    }
  }

  $('intake').onsubmit = async (event) => {
    event.preventDefault()
    const opened = await post('/threads', { patient: $('patient').value, message: $('said').value })
    $('patient').value = $('said').value = ''
    await show(opened.thread)
    await patients()
  }
  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const said = $('text').value
    $('text').value = ''
    await client.send(open.id, said)
  }
  await patients()
}
