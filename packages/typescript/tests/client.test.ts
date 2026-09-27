import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { type Client, connect } from '../src/client.js'
import { ForbiddenError, InputError, TransportError } from '../src/errors.js'
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

  it('sends metadata with a message and with the message a stream ends in, and reads it back', async () => {
    const { client, items } = await joined()
    const plain = await client.send('t1', 'Charged twice?')
    const sent = await client.send('t1', 'Refunded.', {
      inReplyTo: plain.id,
      metadata: { runId: 'r1' },
    })
    const reply = client.stream('t1', { inReplyTo: plain.id, metadata: { runId: 'r2' } })
    reply.write('Refunded in full.')
    const streamed = await reply.finish()
    await client.step('t1', { name: 'Read' })
    const payloads = server.sent('event.send').map((frame) => frame.payload)
    expect(payloads.map((payload) => payload.metadata)).toEqual([
      {},
      { runId: 'r1' },
      { runId: 'r2' },
      undefined,
    ])
    expect([plain.metadata, sent.metadata, streamed.metadata]).toEqual([
      {},
      { runId: 'r1' },
      { runId: 'r2' },
    ])
    server.push('event', { ...message(9, 'from elsewhere'), metadata: { runId: 'r9' } })
    await vi.waitFor(() => expect(items.some((item) => 'seq' in item && item.seq === 9)).toBe(true))
    const pushed = items.find((item) => 'seq' in item && item.seq === 9)
    expect(pushed && 'metadata' in pushed ? pushed.metadata : null).toEqual({ runId: 'r9' })
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

  it('ends the stream when the server refuses the message it finishes with', async () => {
    const { client } = await joined()
    server.handlers.set('event.send', () => {
      throw Object.assign(new Error('server: refused'), { code: 'input' })
    })
    const reply = client.stream('t1')
    reply.write('4111 1111 1111 1111')
    await expect(reply.finish()).rejects.toBeInstanceOf(InputError)
    const kinds = server
      .sent('signal.send')
      .map((frame) => frame.payload.kind)
      .filter((kind) => kind !== 'stream.delta')
    expect(kinds).toEqual(['stream.start', 'stream.end'])
    expect(server.sent('event.send')).toHaveLength(1)
  })

  it('raises a refused stream start from finish and stores nothing', async () => {
    server.handlers.set('signal.send', (payload) => {
      if (payload.kind === 'stream.start')
        throw new ForbiddenError("server: signals need a join; 'ana' has not joined thread 't1'")
      return {}
    })
    const client = await connect({ url: 'ws://fake' })
    const reply = client.stream('t1')
    reply.write('half ')
    const refused = reply.finish()
    await expect(refused).rejects.toBeInstanceOf(ForbiddenError)
    await expect(refused).rejects.toThrow(
      "server: signals need a join; 'ana' has not joined thread 't1'"
    )
    expect(server.sent().map((frame) => frame.payload.kind ?? frame.method)).toEqual([
      'stream.start',
    ])
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

  it('follows a thread made again under the same id from the head the server gives', async () => {
    const { items } = await joined()
    server.push('event', message(8, 'old'))
    await vi.waitFor(() => expect(items).toHaveLength(1))
    server.handlers.set('thread.join', (payload) => ({
      thread: {
        id: payload.thread_id,
        scope: 'acme',
        metadata: {},
        created_at: '2026-01-01T12:00:00Z',
      },
      seq: 2,
      truncated: true,
    }))
    server.drop()
    await vi.waitFor(() => expect(items.some((item) => 'truncated' in item)).toBe(true))
    server.push('event', message(3, 'new'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 3 }))
    expect(server.sent('thread.join').map((frame) => frame.payload.since)).toEqual([0, 8])
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

  it('closes a socket a reconnect opened after close()', async () => {
    let calls = 0
    let held = false
    let release = () => {}
    const client = await connect({
      url: 'ws://fake',
      protocols: async () => {
        calls += 1
        if (calls === 2)
          await new Promise<void>((resolve) => {
            held = true
            release = () => resolve()
          })
        return ['thread', 'tok']
      },
    })
    await client.join('t1', { since: 0 })
    server.drop()
    await vi.waitFor(() => expect(held).toBe(true))
    await client.close()
    release()
    await vi.waitFor(() => expect(server.hellos).toHaveLength(2))
    await vi.waitFor(() =>
      expect(server.sockets.filter((socket) => socket.readyState === 1)).toEqual([])
    )
    expect(await client.closed).toBeNull()
  })

  it('refuses calls after close', async () => {
    const { client } = await joined()
    await client.close()
    expect(await client.closed).toBeNull()
    await expect(client.getThread('t1')).rejects.toThrow('client: the client is closed')
  })
})
