import { connect } from '/client/index.js'

const ASSISTANT = { role: 'assistant', id: 'assistant', name: 'Assistant' }
const $ = (id) => document.getElementById(id)
const element = (tag, props) => Object.assign(document.createElement(tag), props)

$('signin').onsubmit = (event) => {
  event.preventDefault()
  document.cookie = `name=${encodeURIComponent($('name').value.trim())}; path=/; samesite=strict`
  start()
}
if (/(^|; )name=/.test(document.cookie)) start()

async function photo(file) {
  const bitmap = await createImageBitmap(file)
  const scale = Math.min(1, 1024 / Math.max(bitmap.width, bitmap.height))
  const canvas = element('canvas', { width: bitmap.width * scale, height: bitmap.height * scale })
  canvas.getContext('2d').drawImage(bitmap, 0, 0, canvas.width, canvas.height)
  const url = canvas.toDataURL('image/jpeg', 0.85)
  return { type: 'image', media_type: 'image/jpeg', data: url.slice(url.indexOf(',') + 1) }
}

async function start() {
  $('signin').hidden = true
  $('app').hidden = false
  const client = await connect({ url: `ws://${location.host}/ws` })
  const dm = await client.createThread({
    threadId: `dm-${client.identity.id}`,
    metadata: { dm: true },
  })
  await client.addMember(dm.id, ASSISTANT)
  let open = null

  async function threads() {
    $('threads').replaceChildren()
    for (const thread of await client.listThreads()) {
      const label = thread.metadata.reminder ? `Reminder: ${thread.metadata.reminder}` : 'Assistant'
      const item = element('li')
      item.append(element('button', { textContent: label, onclick: () => show(thread) }))
      $('threads').append(item)
    }
  }

  async function show(thread) {
    open = thread
    $('log').replaceChildren()
    await client.join(thread.id, { since: 0 })
  }

  client.subscribe(
    (item) => {
      if (item.kind === 'invited') threads()
      if (item.thread_id !== open?.id) return
      if (item.kind === 'message') {
        const p = element('p', { className: item.author.role })
        p.dataset.who = item.author.name
        for (const part of item.content) {
          if (part.type === 'text') p.append(part.text)
          if (part.type === 'image')
            p.append(element('img', { src: `data:${part.media_type};base64,${part.data}` }))
        }
        $('log').append(p)
      }
      if (item.kind === 'working') $('status').textContent = `${item.sender.name}: ${item.label}`
      if (item.kind === 'idle') $('status').textContent = ''
    },
    { includeOwn: true }
  )

  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const parts = []
    if ($('text').value) parts.push({ type: 'text', text: $('text').value })
    if ($('photo').files[0]) parts.push(await photo($('photo').files[0]))
    if (!parts.length) return
    $('text').value = ''
    $('photo').value = ''
    await client.send(open.id, parts)
  }
  await threads()
  await show(dm)
}
