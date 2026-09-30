import { connect } from '/client/index.js'

const $ = (id) => document.getElementById(id)
const element = (tag, props) => Object.assign(document.createElement(tag), props)
const text = (content) =>
  content
    .filter((part) => part.type === 'text')
    .map((part) => part.text)
    .join('\n')

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

  async function show(channel) {
    await fetch(`/channels/${channel.id}/join`, { method: 'POST' })
    if (open) await client.leave(open.id)
    open = channel
    $('title').textContent = `${channel.metadata.line}: ${channel.metadata.machines}`
    $('log').replaceChildren()
    $('channel').hidden = false
    await client.join(channel.id, { since: 0 })
  }

  for (const channel of await (await fetch('/channels')).json()) {
    const item = element('li')
    item.append(element('button', { textContent: channel.metadata.line, onclick: () => show(channel) }))
    $('channels').append(item)
  }

  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const said = $('text').value
    $('text').value = ''
    await client.send(open.id, said)
  }
}
