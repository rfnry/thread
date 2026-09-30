import { connect } from '/client/index.js'

const $ = (id) => document.getElementById(id)
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
  $('chat').hidden = false
  const client = await connect({ url: `ws://${location.host}/ws` })
  const thread = await (await fetch('/threads', { method: 'POST' })).json()
  const bubbles = new Map()
  const replies = new Map()
  const stored = new Set()
  let assistant = null

  const bubble = (key, who, inReplyTo) => {
    if (!bubbles.has(key)) {
      const p = Object.assign(document.createElement('p'), { className: who.role })
      p.dataset.who = who.name
      $('log').append(p)
      bubbles.set(key, p)
    }
    if (inReplyTo) replies.set(inReplyTo, bubbles.get(key))
    return bubbles.get(key)
  }
  const busy = (who, label = '') => {
    assistant = who
    $('stop').disabled = !who
    $('status').textContent = label
  }

  client.subscribe(
    (item) => {
      if (item.thread_id !== thread.id) return
      if (item.kind === 'message') {
        stored.add(item.client_id)
        bubble(item.client_id, item.author, item.in_reply_to).textContent = text(item.content)
      }
      if (item.kind === 'stream.start') {
        bubble(item.stream_id, item.sender, item.in_reply_to)
        busy(item.sender, `${item.sender.name} is writing…`)
      }
      if (item.kind === 'stream.delta') bubble(item.stream_id, item.sender).textContent += item.text
      if (item.kind === 'stream.end' && !stored.has(item.stream_id))
        bubbles.get(item.stream_id)?.remove()
      if (item.kind === 'working') busy(item.sender, `${item.sender.name}: ${item.label}`)
      if (item.kind === 'idle') busy(null)
      if (item.kind === 'step') {
        const line = document.createElement('div')
        line.className = 'step'
        line.textContent = `· ${item.name} ${JSON.stringify(item.input)}`
        const reply = replies.get(item.in_reply_to)
        reply ? reply.before(line) : $('log').append(line)
      }
    },
    { includeOwn: true }
  )
  await client.join(thread.id, { since: 0 })

  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const said = $('text').value
    $('text').value = ''
    await client.send(thread.id, said)
  }
  $('stop').onclick = () => client.cancel(thread.id, assistant)
}
