import { connect } from '/client/index.js'

const ASSISTANT = { role: 'assistant', id: 'bookkeeper', name: 'Bookkeeper' }
const $ = (id) => document.getElementById(id)
const text = (content) =>
  content
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('\n')
const element = (tag, props) => Object.assign(document.createElement(tag), props)

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
  const cards = new Map()
  let open = null

  async function threads() {
    $('threads').replaceChildren()
    for (const thread of await client.listThreads()) {
      const item = element('li')
      item.append(
        element('button', { textContent: thread.metadata.title, onclick: () => show(thread) })
      )
      $('threads').append(item)
    }
  }

  async function show(thread) {
    open = thread
    cards.clear()
    $('thread').hidden = false
    $('log').replaceChildren()
    await client.join(thread.id, { since: 0 })
  }

  function card(message) {
    const box = element('div', { className: 'card' })
    box.append(
      `${message.author.name} wants to run:`,
      element('pre', { textContent: text(message.content) })
    )
    for (const allow of [true, false]) {
      const button = element('button', { textContent: allow ? 'Allow' : 'Deny' })
      button.onclick = () =>
        client.send(open.id, allow ? 'Allowed' : 'Denied', {
          inReplyTo: message.id,
          metadata: { allow },
        })
      box.append(button, ' ')
    }
    cards.set(message.id, box)
    return box
  }

  client.subscribe(
    (item) => {
      if (item.thread_id !== open?.id) return
      if (item.kind === 'message' && item.metadata.approval) $('log').append(card(item))
      else if (item.kind === 'message' && 'allow' in item.metadata) {
        const box = cards.get(item.in_reply_to)
        for (const button of box?.querySelectorAll('button') ?? []) button.disabled = true
        box?.append(
          element('div', {
            className: 'answer',
            textContent: `${text(item.content)} by ${item.author.name}`,
          })
        )
      } else if (item.kind === 'message') {
        const p = element('p', { className: item.author.role, textContent: text(item.content) })
        p.dataset.who = item.author.name
        $('log').append(p)
      }
      if (item.kind === 'step')
        $('log').append(
          element('div', {
            className: 'step',
            textContent: `· ${item.name} ${JSON.stringify(item.input)}`,
          })
        )
      if (item.kind === 'working') $('status').textContent = `${item.sender.name}: ${item.label}`
      if (item.kind === 'idle') $('status').textContent = ''
    },
    { includeOwn: true }
  )

  $('new').onsubmit = async (event) => {
    event.preventDefault()
    const task = $('task').value
    $('task').value = ''
    const thread = await client.createThread({ metadata: { title: task } })
    await client.addMember(thread.id, ASSISTANT)
    await show(thread)
    await client.send(thread.id, task)
    await threads()
  }
  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const said = $('text').value
    $('text').value = ''
    await client.send(open.id, said)
  }
  await threads()
}
