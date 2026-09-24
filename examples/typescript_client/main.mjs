import { connect, resolve } from '@rfnry/thread'

const client = await connect({ url: 'ws://127.0.0.1:8780', protocols: async () => ['thread', 'browser'] })
const thread = await client.createThread({ threadId: 'hello' })
await client.addMember(thread.id, { role: 'assistant', id: 'echo', name: 'Echo' })
const done = new Promise((finish) => {
  client.subscribe((item) => {
    if (item.kind === 'stream.delta') process.stdout.write(item.text)
    if (item.kind === 'stream.end') finish()
  })
})
await client.join(thread.id, { since: 0 })
await client.send(thread.id, 'hello from node')
await done
console.log('\n', resolve(await client.history(thread.id)).map((event) => `${event.seq} ${event.kind}`))
await client.close()
