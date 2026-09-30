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
  document.cookie = `firm=${$('firm').value}; path=/; samesite=strict`
  start()
}
if (cookie('name') && cookie('firm')) start()

async function start() {
  $('signin').hidden = true
  $('app').hidden = false
  const headers = { 'X-Tenant': cookie('firm'), 'content-type': 'application/json' }
  const post = async (path, body = {}) =>
    (await fetch(path, { method: 'POST', headers, body: JSON.stringify(body) })).json()
  const client = await connect({ url: `ws://${location.host}/ws` })
  let open = null

  const review = (message, box) => {
    const reviewed = (label) => {
      box.replaceChildren(`${label.review.stance} recorded on run ${label.run_id}`)
    }
    const approve = element('button', { textContent: 'Approve' })
    approve.onclick = async () =>
      reviewed(await post(`/runs/${message.metadata.run_id}/review`, { stance: 'approval' }))
    const correct = element('button', { textContent: 'Correct' })
    correct.onclick = async () => {
      const correction = prompt('What should it have said?')
      if (correction)
        reviewed(
          await post(`/runs/${message.metadata.run_id}/review`, { stance: 'objection', correction })
        )
    }
    box.append(approve, ' ', correct)
  }

  client.subscribe(
    (item) => {
      if (item.thread_id !== open?.id) return
      if (item.kind === 'message') {
        const p = element('p', { className: item.author.role, textContent: text(item.content) })
        p.dataset.who = item.author.name
        if (item.metadata.run_id)
          review(item, p.appendChild(element('span', { className: 'review' })))
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

  $('open').onsubmit = async (event) => {
    event.preventDefault()
    open = await post('/threads', { contract: $('contract').value })
    $('thread').hidden = false
    $('log').replaceChildren()
    await client.join(open.id, { since: 0 })
  }
  $('export').onclick = async () => {
    $('exported').textContent = JSON.stringify(await post('/export'), null, 2)
  }
  $('say').onsubmit = async (event) => {
    event.preventDefault()
    const said = $('text').value
    $('text').value = ''
    await client.send(open.id, said)
  }
}
