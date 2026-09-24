import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { type Client, connect } from '../src/client.js'
import { ForbiddenError, TransportError } from '../src/errors.js'
import { timing } from '../src/timing.js'
import type { Item } from '../src/types.js'
import { FakeServer, helper, message } from './fakes.js'

const saved = { ...timing }
let server: FakeServer

beforeEach(() => {
  server = new FakeServer().install()
  Object.assign(timing, { reconnectMs: 1, backoffMs: 1 })
})

afterEach(() => {
  Object.assign(timing, saved)
  vi.unstubAllGlobals()
})

async function joined(): Promise<{ client: Client; items: Item[] }> {
  const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
  const items: Item[] = []
  client.subscribe((item) => items.push(item), { includeOwn: true })
  await client.join('t1', { since: 0 })
  return { client, items }
}

describe('client', () => {
  it('opens with the protocols and a hello, and knows who and where it is', async () => {
    const protocols = vi.fn(async () => ['thread', 'tok'])
    const client = await connect({ url: 'ws://fake', protocols })
    expect(client.identity.id).toBe('ana')
    expect(client.scope).toBe('acme')
    expect(server.opened).toEqual([['thread', 'tok']])
    expect(server.hellos).toEqual([{ type: 'hello', protocol: 1 }])
  })

  it('raises an unreachable server and a protocols function that throws', async () => {
    server.down = true
    await expect(connect({ url: 'ws://fake' })).rejects.toBeInstanceOf(TransportError)
    server.down = false
    const failing = connect({
      url: 'ws://fake',
      protocols: async () => {
        throw new Error('session expired')
      },
    })
    await expect(failing).rejects.toBeInstanceOf(ForbiddenError)
    await expect(failing).rejects.toThrow('client: the protocols function threw: session expired')
  })

  it('speaks the protocol verb for verb', async () => {
    const { client } = await joined()
    await client.createThread({ threadId: 't1', metadata: { topic: 'billing' } })
    await client.history('t1', { before: 5, limit: 2 })
    await client.working('t1', 'Reading')
    await client.cancel('t1', helper)
    const methods = server.sent().map((frame) => [frame.method, frame.payload])
    expect(methods).toContainEqual([
      'thread.create',
      { thread_id: 't1', metadata: { topic: 'billing' } },
    ])
    expect(methods).toContainEqual([
      'thread.history',
      { thread_id: 't1', before: 5, limit: 2, kinds: null },
    ])
    expect(methods).toContainEqual([
      'signal.send',
      { thread_id: 't1', kind: 'working', label: 'Reading' },
    ])
    expect(methods).toContainEqual(['signal.send', { thread_id: 't1', kind: 'cancel', to: helper }])
  })

  it('hands items to listeners, leaves out its own unless asked, and drops repeats', async () => {
    const { client, items } = await joined()
    const others: Item[] = []
    const unsubscribe = client.subscribe((item) => others.push(item))
    server.push('event', message(1, 'hi'))
    server.push('event', message(1, 'hi'))
    server.push('event', message(2, 'mine', client.identity))
    server.push('signal', { kind: 'working', thread_id: 't1', sender: helper, label: 'Reading' })
    await vi.waitFor(() => expect(items).toHaveLength(3))
    expect(others.map((item) => ('kind' in item ? item.kind : 'joined'))).toEqual([
      'message',
      'working',
    ])
    unsubscribe()
  })

  it('sends a string as one text part and returns the stored event', async () => {
    const { client } = await joined()
    const sent = await client.send('t1', 'Can you check my invoice?', { inReplyTo: 'e0' })
    expect(sent.content).toEqual([{ type: 'text', text: 'Can you check my invoice?' }])
    expect(sent.in_reply_to).toBe('e0')
  })

  it('stores a caller-supplied clientId once and returns the stored event both times', async () => {
    const { client } = await joined()
    const first = await client.send('t1', 'Your plan renews Friday.', {
      clientId: 'renewal-ana-2026-10',
    })
    const again = await client.send('t1', 'Your plan renews Friday, again.', {
      clientId: 'renewal-ana-2026-10',
    })
    expect(again).toEqual(first)
    expect(first.client_id).toBe('renewal-ana-2026-10')
    await client.step('t1', { name: 'Read', clientId: 'read-1' })
    await client.edit('t1', first.id, 'Renews Friday.', { clientId: 'edit-1' })
    await client.retract('t1', first.id, { clientId: 'retract-1' })
    await client.custom('t1', 'rating', { stars: 5 }, { clientId: 'rating-1' })
    const ids = server.sent('event.send').map((frame) => frame.payload.client_id)
    expect(ids).toEqual([
      'renewal-ana-2026-10',
      'renewal-ana-2026-10',
      'read-1',
      'edit-1',
      'retract-1',
      'rating-1',
    ])
    expect(server.stored.size).toBe(5)
  })

  it('streams: start, deltas as notifies in order, the message with the stream id, then end', async () => {
    const { client } = await joined()
    const reply = client.stream('t1', { inReplyTo: 'e1' })
    reply.write('Refunded ')
    reply.write('in full.')
    const stored = await reply.finish()
    expect(stored.client_id).toBe(reply.id)
    const frames = server
      .sent()
      .filter((f) => f.method === 'signal.send' || f.method === 'event.send')
    expect(frames.map((f) => (f.method === 'event.send' ? 'message' : f.payload.kind))).toEqual([
      'stream.start',
      'stream.delta',
      'stream.delta',
      'message',
      'stream.end',
    ])
    expect(frames.filter((f) => f.type === 'notify').map((f) => f.payload.text)).toEqual([
      'Refunded ',
      'in full.',
    ])
    const abandoned = client.stream('t1')
    abandoned.write('never mind')
    await abandoned.abort()
    expect(server.sent('event.send')).toHaveLength(1)
  })

  it('reconnects with fresh protocols, says hello again, and re-joins from the cursor', async () => {
    const protocols = vi.fn(async () => ['thread', 'tok'])
    const client = await connect({ url: 'ws://fake', protocols })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.join('t1', { since: 0 })
    server.push('event', message(4, 'before'))
    await vi.waitFor(() => expect(items).toHaveLength(1))
    server.drop()
    await vi.waitFor(() => expect(protocols).toHaveBeenCalledTimes(2))
    await vi.waitFor(() => expect(server.hellos).toHaveLength(2))
    await vi.waitFor(() => expect(items.some((item) => 'truncated' in item)).toBe(true))
    const joins = server.sent('thread.join').map((frame) => frame.payload.since)
    expect(joins).toEqual([0, 4])
  })

  it('stores a write whose ack was lost exactly once', async () => {
    const { client } = await joined()
    server.dropAck = true
    const sent = await client.send('t1', 'exactly once')
    const attempts = server.sent('event.send').map((frame) => frame.payload.client_id)
    expect(attempts).toHaveLength(2)
    expect(new Set(attempts).size).toBe(1)
    expect(server.stored.size).toBe(1)
    expect(sent.seq).toBe(1)
  })

  it('stops for good when the protocols function throws on a reconnect', async () => {
    let expired = false
    const client = await connect({
      url: 'ws://fake',
      protocols: async () => {
        if (expired) throw new Error('session expired')
        return ['thread', 'tok']
      },
    })
    await client.join('t1', { since: 0 })
    expired = true
    server.drop()
    const reason = await client.closed
    expect(reason).toBeInstanceOf(ForbiddenError)
    await expect(client.send('t1', 'late')).rejects.toBeInstanceOf(ForbiddenError)
  })

  it('refuses calls after close', async () => {
    const { client } = await joined()
    await client.close()
    expect(await client.closed).toBeNull()
    await expect(client.getThread('t1')).rejects.toThrow('client: the client is closed')
  })
})
