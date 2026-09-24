import { describe, expect, it } from 'vitest'
import { connect, resolve } from '../../src/index.js'
import type { Item } from '../../src/types.js'

const url = process.env.THREAD_URL ?? ''
const helper = { role: 'assistant' as const, id: 'bot-helper', name: 'Helper' }

async function until(check: () => boolean, ms = 5000): Promise<void> {
  const started = Date.now()
  while (!check()) {
    if (Date.now() - started > ms) throw new Error('e2e: timed out')
    await new Promise((resolve) => setTimeout(resolve, 20))
  }
}

describe.skipIf(!url)('a live server', () => {
  it('carries a turn with a step, a stream and its message, in order', async () => {
    const client = await connect({ url, protocols: async () => ['thread', 'ana'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    const thread = await client.createThread({ threadId: 'e2e-turn' })
    await client.addMember(thread.id, helper)
    await client.join(thread.id, { since: 0 })
    const question = await client.send(thread.id, 'My invoice was charged twice.')
    await until(() => items.some((item) => 'kind' in item && item.kind === 'stream.end'))
    const kinds = items.flatMap((item) => ('kind' in item ? [item.kind] : []))
    expect(kinds).toContain('working')
    expect(kinds.indexOf('stream.start')).toBeLessThan(kinds.indexOf('message'))
    expect(kinds.lastIndexOf('message')).toBeLessThan(kinds.indexOf('stream.end'))
    const view = resolve(await client.history(thread.id))
    const underQuestion = view
      .filter((event) => event.in_reply_to === question.id)
      .map((event) => event.kind)
    expect(underQuestion).toEqual(['step', 'message'])
    await client.close()
  })

  it('resumes after the server drops the socket and misses nothing', async () => {
    const client = await connect({ url, protocols: ['thread', 'ana'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item), { includeOwn: true })
    const thread = await client.createThread({ threadId: 'e2e-resume' })
    await client.addMember(thread.id, helper)
    await client.join(thread.id)
    await client.custom(thread.id, 'drop', {})
    await until(() => items.some((item) => 'truncated' in item))
    const sent = await client.send(thread.id, 'after the drop')
    await until(() => items.some((item) => 'kind' in item && item.kind === 'stream.end'))
    const seqs = items.flatMap((item) => ('kind' in item && 'seq' in item ? [item.seq] : []))
    expect(new Set(seqs).size).toBe(seqs.length)
    expect(seqs).toContain(sent.seq)
    await client.close()
  })
})
