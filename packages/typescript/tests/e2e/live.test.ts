import { describe, expect, it } from 'vitest'
import { connect, ForbiddenError, InputError, resolve } from '../../src/index.js'
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
    const question = await client.send(thread.id, 'My invoice was charged twice.', {
      metadata: { ticket: 't-1' },
    })
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
    const answer = view.find((event) => event.kind === 'message' && event.in_reply_to)
    expect(view.find((event) => event.id === question.id)).toMatchObject({
      metadata: { ticket: 't-1' },
    })
    expect(answer).toMatchObject({ metadata: { run_id: 'run-1' } })
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
    await until(() => client.status === 'open')
    expect(items.flatMap((item) => ('status' in item ? [item.status] : []))).toEqual([
      'reconnecting',
      'open',
    ])
    const sent = await client.send(thread.id, 'after the drop')
    await until(() => items.some((item) => 'kind' in item && item.kind === 'stream.end'))
    const seqs = items.flatMap((item) => ('kind' in item && 'seq' in item ? [item.seq] : []))
    expect(new Set(seqs).size).toBe(seqs.length)
    expect(seqs).toContain(sent.seq)
    await client.close()
    expect(client.status).toBe('closed')
  })

  it('follows its threads, resumes after a drop without repeats, and marks', async () => {
    const client = await connect({ url, protocols: async () => ['thread', 'ana'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item), { includeOwn: true })
    const thread = await client.createThread({ threadId: 'e2e-follow' })
    await client.addMember(thread.id, helper)
    const joined = await client.follow({ resume: true })
    expect(joined.map((one) => one.thread.id)).toContain('e2e-follow')
    await client.custom(thread.id, 'drop', {})
    await until(() => client.status === 'open' && items.some((item) => 'status' in item))
    const sent = await client.send(thread.id, 'after the drop')
    expect(await client.mark(thread.id, sent.seq)).toBe(sent.seq)
    const listed = (await client.listThreads({ order: 'activity' })).find((t) => t.id === thread.id)
    expect(listed?.marked).toBe(sent.seq)
    const seqs = items.flatMap((item) =>
      'author' in item && item.thread_id === thread.id ? [item.seq] : []
    )
    expect(new Set(seqs).size).toBe(seqs.length)
    await client.close()
  })

  it('joins with resume and hears what was said while it was away', async () => {
    const ana = await connect({ url, protocols: ['thread', 'ana'] })
    const thread = await ana.createThread({ threadId: 'e2e-join-resume' })
    await ana.addMember(thread.id, { role: 'user', id: 'bruno', name: 'Bruno' })
    const first = await connect({ url, protocols: ['thread', 'bruno'] })
    await first.join(thread.id, { resume: true })
    const hello = await ana.send(thread.id, 'hello')
    expect(await first.mark(thread.id, hello.seq)).toBe(hello.seq)
    await first.close()
    const away = await ana.send(thread.id, 'sent while away')
    const again = await connect({ url, protocols: ['thread', 'bruno'] })
    const items: Item[] = []
    again.subscribe((item) => items.push(item))
    const joined = await again.join(thread.id, { resume: true })
    expect(joined.seq).toBe(away.seq)
    await until(() => items.some((item) => 'author' in item && item.id === away.id))
    expect(items.flatMap((item) => ('author' in item ? [item.seq] : []))).toEqual([away.seq])
    await expect(again.join(thread.id, { since: 0, resume: true })).rejects.toBeInstanceOf(
      InputError
    )
    await ana.close()
    await again.close()
  })

  it('ends closed with a ForbiddenError when the endpoint refuses a reconnect with 1008', async () => {
    let refused = false
    const client = await connect({
      url,
      protocols: async () => ['thread', refused ? 'refused' : 'ana'],
    })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    const thread = await client.createThread({ threadId: 'e2e-refused' })
    await client.addMember(thread.id, helper)
    await client.join(thread.id)
    await until(() =>
      items.some(
        (item) => 'kind' in item && item.kind === 'present' && item.sender.id === helper.id
      )
    )
    refused = true
    await client.custom(thread.id, 'drop', {})
    const reason = await client.closed
    expect(reason).toBeInstanceOf(ForbiddenError)
    expect(reason?.message).toBe(
      'client: the server refused the connection with code 1008: session expired'
    )
    expect(client.status).toBe('closed')
    expect(items.flatMap((item) => ('status' in item ? [item.status] : []))).toEqual([
      'reconnecting',
      'closed',
    ])
    await expect(connect({ url, protocols: ['thread', 'refused'] })).rejects.toBeInstanceOf(
      ForbiddenError
    )
  })

  it('keeps a followed thread it left left after a drop, until it joins it', async () => {
    const ana = await connect({ url, protocols: ['thread', 'ana'] })
    const bruno = await connect({ url, protocols: ['thread', 'bruno'] })
    const items: Item[] = []
    ana.subscribe((item) => items.push(item))
    const left = await bruno.createThread({ threadId: 'e2e-left' })
    await bruno.addMember(left.id, ana.identity)
    const dropper = await ana.createThread({ threadId: 'e2e-left-drop' })
    await ana.addMember(dropper.id, helper)
    await ana.follow()
    await ana.leave(left.id)
    await until(() =>
      items.some(
        (item) => 'kind' in item && item.kind === 'present' && item.thread_id === dropper.id
      )
    )
    await ana.custom(dropper.id, 'drop', {})
    await until(() => items.some((item) => 'status' in item && item.status === 'open'))
    await bruno.send(left.id, 'after the drop')
    await ana.join(left.id)
    await bruno.send(left.id, 'joined again')
    const said = () =>
      items.flatMap((item) =>
        'author' in item && item.kind === 'message' && item.thread_id === left.id
          ? [item.content[0]?.type === 'text' ? item.content[0].text : '']
          : []
      )
    await until(() => said().length > 0)
    expect(said()).toEqual(['joined again'])
    await ana.close()
    await bruno.close()
  })
})
