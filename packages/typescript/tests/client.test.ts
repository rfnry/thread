import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { type Client, connect } from '../src/client.js'
import { ConfigurationError, ForbiddenError, InputError, TransportError } from '../src/errors.js'
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

  it('raises an unreachable server, a protocols function that refuses and one that fails', async () => {
    server.down = true
    await expect(connect({ url: 'ws://fake' })).rejects.toBeInstanceOf(TransportError)
    server.down = false
    const refusing = connect({
      url: 'ws://fake',
      protocols: async () => {
        throw new ForbiddenError('app: session expired')
      },
    })
    await expect(refusing).rejects.toBeInstanceOf(ForbiddenError)
    await expect(refusing).rejects.toThrow('app: session expired')
    const failing = connect({
      url: 'ws://fake',
      protocols: async () => {
        throw new TypeError('Failed to fetch')
      },
    })
    await expect(failing).rejects.toBeInstanceOf(TransportError)
    await expect(failing).rejects.toThrow('client: the protocols function threw: Failed to fetch')
    expect(server.sockets).toHaveLength(1)
  })

  it('speaks the protocol verb for verb', async () => {
    const { client } = await joined()
    await client.createThread({ threadId: 't1', metadata: { topic: 'billing' } })
    await client.history('t1', { before: 5, limit: 2 })
    await client.getEvent('t1', 'e1')
    await client.working('t1', 'Reading')
    await client.cancel('t1', helper)
    await client.away('t1')
    const methods = server.sent().map((frame) => [frame.method, frame.payload])
    expect(methods).toContainEqual([
      'thread.create',
      { thread_id: 't1', metadata: { topic: 'billing' } },
    ])
    expect(methods).toContainEqual([
      'thread.history',
      { thread_id: 't1', before: 5, limit: 2, kinds: null },
    ])
    expect(methods).toContainEqual(['event.get', { thread_id: 't1', event_id: 'e1' }])
    expect(methods).toContainEqual([
      'signal.send',
      { thread_id: 't1', kind: 'working', label: 'Reading' },
    ])
    expect(methods).toContainEqual(['signal.send', { thread_id: 't1', kind: 'cancel', to: helper }])
    expect(methods).toContainEqual(['thread.away', { thread_id: 't1' }])
  })

  it('joins a thread it is away from again after a drop without presence', async () => {
    const { client } = await joined()
    await client.away('t1')
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.join')).toHaveLength(2))
    expect(server.sent('thread.join')[1]?.payload).toEqual({
      thread_id: 't1',
      since: 0,
      away: true,
    })
    await client.join('t1')
    expect(server.sent('thread.join')[2]?.payload).toEqual({ thread_id: 't1', since: null })
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.join')).toHaveLength(4))
    expect(server.sent('thread.join')[3]?.payload).toEqual({ thread_id: 't1', since: 0 })
    await client.close()
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

  it("sets the metadata it finishes with over the stream's", async () => {
    const { client } = await joined()
    const opening = { run_id: 'r1', through: 0 }
    const reply = client.stream('t1', { metadata: opening })
    reply.write('Refunded.')
    await reply.finish(undefined, { metadata: { through: 7 } })
    expect(server.sent('event.send').at(-1)?.payload.metadata).toEqual({ run_id: 'r1', through: 7 })
    expect(opening).toEqual({ run_id: 'r1', through: 0 })
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

  it('is reconnecting from a dropped socket until every thread is resumed, then open', async () => {
    const { client, items } = await joined()
    expect(client.status).toBe('open')
    server.push('event', message(4, 'before'))
    await vi.waitFor(() => expect(items).toHaveLength(1))
    server.down = true
    server.drop()
    await vi.waitFor(() => expect(client.status).toBe('reconnecting'))
    await vi.waitFor(() => expect(server.sockets.length).toBeGreaterThan(3))
    expect(client.status).toBe('reconnecting')
    server.down = false
    await vi.waitFor(() => expect(client.status).toBe('open'))
    expect(
      items.map((item) => ('status' in item ? item.status : 'truncated' in item ? 'joined' : 4))
    ).toEqual([4, 'reconnecting', 'joined', 'open'])
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

  it('stops for good when the protocols function refuses on a reconnect', async () => {
    let expired = false
    const client = await connect({
      url: 'ws://fake',
      protocols: async () => {
        if (expired) throw new ForbiddenError('app: session expired')
        return ['thread', 'tok']
      },
    })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.join('t1', { since: 0 })
    expired = true
    server.drop()
    const reason = await client.closed
    expect(reason).toBeInstanceOf(ForbiddenError)
    expect(client.status).toBe('closed')
    expect(items).toEqual([
      { kind: 'status', status: 'reconnecting' },
      { kind: 'status', status: 'closed' },
    ])
  })

  it('raises a library error from the protocols function as it is, and stops on one on a reconnect', async () => {
    const misconfigured = connect({
      url: 'ws://fake',
      protocols: async () => {
        throw new ConfigurationError('app: no token endpoint')
      },
    })
    await expect(misconfigured).rejects.toBeInstanceOf(ConfigurationError)
    await expect(misconfigured).rejects.toThrow('app: no token endpoint')
    let broken = false
    const protocols = vi.fn(async () => {
      if (broken) throw new ConfigurationError('app: no token endpoint')
      return ['thread', 'tok']
    })
    const client = await connect({ url: 'ws://fake', protocols })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    broken = true
    server.drop()
    const reason = await client.closed
    expect(reason).toBeInstanceOf(ConfigurationError)
    expect(reason?.message).toBe('app: no token endpoint')
    expect(protocols).toHaveBeenCalledTimes(2)
    expect(items).toEqual([
      { kind: 'status', status: 'reconnecting' },
      { kind: 'status', status: 'closed' },
    ])
  })

  it('keeps reconnecting while the protocols function fails with anything but a refusal', async () => {
    let failures = 2
    const protocols = vi.fn(async () => {
      if (protocols.mock.calls.length > 1 && failures > 0) {
        failures -= 1
        throw new TypeError('Failed to fetch')
      }
      return ['thread', 'tok']
    })
    const client = await connect({ url: 'ws://fake', protocols })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.join('t1', { since: 0 })
    server.drop()
    await vi.waitFor(() => expect(protocols).toHaveBeenCalledTimes(4))
    await vi.waitFor(() => expect(client.status).toBe('open'))
    expect(items.filter((item) => 'status' in item)).toEqual([
      { kind: 'status', status: 'reconnecting' },
      { kind: 'status', status: 'open' },
    ])
    await client.close()
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
    expect(client.status).toBe('closed')
  })

  it('refuses calls after close', async () => {
    const { client } = await joined()
    await client.close()
    expect(await client.closed).toBeNull()
    await expect(client.getThread('t1')).rejects.toThrow('client: the client is closed')
  })

  it('is closed after close(), and listeners hear it once', async () => {
    const { client, items } = await joined()
    await client.close()
    await client.close()
    expect(client.status).toBe('closed')
    expect(items).toEqual([{ kind: 'status', status: 'closed' }])
  })

  it('follows, and after a drop follows again once from the cursors it holds', async () => {
    server.handlers.set('thread.follow', () => ({
      joined: [
        {
          thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
          seq: 4,
          truncated: false,
        },
      ],
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item), { includeOwn: true })
    const [joined] = await client.follow({ resume: true })
    expect(joined?.thread.id).toBe('t1')
    server.push('event', message(5, 'live'))
    await vi.waitFor(() => expect(items.some((item) => 'seq' in item && item.seq === 5)).toBe(true))
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.follow')).toHaveLength(2))
    expect(server.sent('thread.follow').map((frame) => frame.payload)).toEqual([
      { resume: true, since: null },
      { resume: true, since: { t1: 5 } },
    ])
    expect(server.sent('thread.join')).toEqual([])
    await client.close()
  })

  it('takes a thread the server starts delivering and keeps its cursor', async () => {
    server.handlers.set('thread.follow', () => ({ joined: [] }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    server.push('joined', {
      thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
      seq: 6,
      truncated: false,
    })
    await vi.waitFor(() => expect(items.some((item) => 'truncated' in item)).toBe(true))
    server.push('event', message(6, 'old'))
    server.push('event', message(7, 'new'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 7 }))
    expect(items.filter((item) => 'kind' in item && 'seq' in item && item.seq === 6)).toEqual([])
    await client.close()
  })

  it('does not follow after a refused follow', async () => {
    server.handlers.set('thread.follow', () => {
      throw { code: 'input', message: 'server: since cannot be negative' }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    await expect(client.follow()).rejects.toBeInstanceOf(InputError)
    expect((client as unknown as { following: boolean }).following).toBe(false)
    server.drop()
    await vi.waitFor(() => expect(server.hellos).toHaveLength(2))
    await vi.waitFor(() => expect(client.status).toBe('open'))
    expect(server.sent('thread.follow')).toHaveLength(1)
    await client.close()
  })

  it('falls back to joining the cursor threads when the re-follow fails', async () => {
    let calls = 0
    server.handlers.set('thread.follow', () => {
      if (++calls > 1) throw { code: 'input', message: 'server: refused' }
      return {
        joined: [
          {
            thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
            seq: 4,
            truncated: false,
          },
        ],
      }
    })
    server.handlers.set('thread.join', () => ({
      thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
      seq: 4,
      truncated: false,
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    await client.follow()
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.join')).toHaveLength(1))
    expect(server.sent('thread.join')[0]?.payload).toEqual({
      thread_id: 't1',
      since: 4,
      away: true,
    })
    await client.close()
  })

  it('tries the re-follow again after a transport loss instead of joining', async () => {
    let calls = 0
    server.handlers.set('thread.follow', () => {
      if (++calls === 2) {
        server.drop()
        return new Promise(() => undefined)
      }
      return {
        joined: [
          {
            thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
            seq: 4,
            truncated: false,
          },
        ],
      }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    await client.follow()
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.follow')).toHaveLength(3))
    await vi.waitFor(() => expect(client.status).toBe('open'))
    expect(server.sent('thread.join')).toEqual([])
    expect(server.sent('thread.follow').at(-1)?.payload).toEqual({ resume: true, since: { t1: 4 } })
    await client.close()
  })

  it('marks, and hears its own marks from its other connections', async () => {
    server.handlers.set('thread.mark', (payload) => ({ seq: payload.seq, unread: 0 }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    expect(await client.mark('t1', 3)).toBe(3)
    server.push('signal', {
      kind: 'marked',
      thread_id: 't1',
      sender: server.identity,
      seq: 3,
      unread: 0,
    })
    await vi.waitFor(() =>
      expect(items).toContainEqual(expect.objectContaining({ kind: 'marked' }))
    )
    expect(await client.mark('t1', 1, { back: true })).toBe(1)
    expect(server.sent('thread.mark').map((frame) => frame.payload)).toEqual([
      { thread_id: 't1', seq: 3 },
      { thread_id: 't1', seq: 1, back: true },
    ])
  })

  it('reads its own mark, null without one, without a write', async () => {
    const marks = new Map<string, number>()
    server.handlers.set('thread.marked', (payload) => ({
      seq: marks.get(payload.thread_id as string) ?? null,
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    expect(await client.marked('t1')).toBeNull()
    marks.set('t1', 3)
    expect(await client.marked('t1')).toBe(3)
    expect(server.sent('thread.marked').at(-1)?.payload).toEqual({ thread_id: 't1' })
    expect(server.sent('thread.mark')).toEqual([])
    await client.close()
  })

  it('reads forward and lists an inbox by activity with previews', async () => {
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    await client.history('t1', { after: 3 })
    await client.listThreads({ order: 'activity', preview: true })
    expect(server.sent('thread.history').at(-1)?.payload).toMatchObject({ after: 3 })
    expect(server.sent('thread.list').at(-1)?.payload).toMatchObject({
      order: 'activity',
      preview: true,
    })
    await client.close()
  })

  it('leaves out the new fields at their defaults, so a server from before them still answers', async () => {
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    await client.history('t1')
    await client.listThreads()
    await client.listThreads({ order: 'created', preview: false })
    expect(server.sent('thread.history').map((frame) => frame.payload)).toEqual([
      { thread_id: 't1', before: null, limit: 50, kinds: null },
    ])
    expect(server.sent('thread.list').map((frame) => frame.payload)).toEqual([
      { before: null, limit: 50 },
      { before: null, limit: 50 },
    ])
    await client.close()
  })

  it('follows a thread made again under the same id from the head the re-follow gives', async () => {
    let calls = 0
    server.handlers.set('thread.follow', () => ({
      joined: [
        {
          thread: { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
          seq: ++calls === 1 ? 4 : 2,
          truncated: calls > 1,
        },
      ],
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    server.push('event', message(8, 'old'))
    await vi.waitFor(() => expect(items.some((item) => 'seq' in item && item.seq === 8)).toBe(true))
    server.drop()
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    expect(server.sent('thread.follow').at(-1)?.payload).toEqual({ resume: true, since: { t1: 8 } })
    server.push('event', message(3, 'new'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 3 }))
    await client.close()
  })

  it('keeps a followed thread it left left after a drop, until it joins it', async () => {
    const followed = (...ids: string[]) => ({
      joined: ids.map((id) => ({
        thread: { id, scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
        seq: 4,
        truncated: false,
      })),
    })
    server.handlers.set('thread.follow', () => followed('t1', 't2'))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    await client.leave('t1')
    server.handlers.set('thread.follow', () => followed('t2'))
    items.length = 0
    server.drop()
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    expect(server.sent().map((frame) => [frame.method, frame.payload.thread_id])).toEqual([
      ['thread.follow', undefined],
      ['thread.leave', 't1'],
      ['thread.follow', undefined],
    ])
    expect(server.sent('thread.follow').at(-1)?.payload).toEqual({
      resume: true,
      since: { t2: 4 },
      left: { t1: { seq: 4, created_at: '2026-01-01T12:00:00Z' } },
    })
    expect(items.flatMap((item) => ('truncated' in item ? [item.thread.id] : []))).toEqual(['t2'])
    await client.join('t1')
    server.push('event', message(7, 'joined again'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 7 }))
    expect(items.flatMap((item) => ('author' in item ? [item.seq] : []))).toEqual([7])
    await client.close()
  })

  const followed = (...ids: string[]) => ({
    joined: ids.map((id) => ({
      thread: { id, scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' },
      seq: 4,
      truncated: false,
    })),
  })

  it('keeps the position a thread was left at when it is left again', async () => {
    server.handlers.set('thread.follow', () => followed('t1', 't2'))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    await client.follow()
    await client.leave('t1')
    await client.leave('t1')
    server.handlers.set('thread.follow', () => followed('t2'))
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.follow')).toHaveLength(2))
    expect(server.sent('thread.follow').at(-1)?.payload.left).toEqual({
      t1: { seq: 4, created_at: '2026-01-01T12:00:00Z' },
    })
    await client.close()
  })

  it('leaves again a thread left while the re-follow was in flight', async () => {
    server.handlers.set('thread.follow', () => followed('t1', 't2'))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    server.handlers.set('thread.follow', () => {
      void client.leave('t1')
      return followed('t1', 't2')
    })
    items.length = 0
    server.drop()
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    await vi.waitFor(() => expect(server.sent('thread.leave')).toHaveLength(2))
    expect(items.flatMap((item) => ('truncated' in item ? [item.thread.id] : []))).toEqual(['t2'])
    server.handlers.set('thread.follow', () => followed('t2'))
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.follow')).toHaveLength(3))
    expect(server.sent('thread.follow').at(-1)?.payload.left).toEqual({
      t1: { seq: 4, created_at: '2026-01-01T12:00:00Z' },
    })
    await client.close()
  })

  it('follows again without the left threads when the server is from before them', async () => {
    server.handlers.set('thread.follow', () => followed('t1', 't2'))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    await client.leave('t1')
    server.handlers.set('thread.follow', (payload) => {
      if (payload.left !== undefined)
        throw {
          code: 'input',
          message: 'server: invalid thread.follow payload: extra inputs are not permitted',
        }
      return followed('t1', 't2')
    })
    items.length = 0
    server.drop()
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    await vi.waitFor(() => expect(server.sent('thread.leave')).toHaveLength(2))
    expect(server.sent('thread.follow').map((frame) => frame.payload)).toEqual([
      { resume: false, since: null },
      {
        resume: true,
        since: { t2: 4 },
        left: { t1: { seq: 4, created_at: '2026-01-01T12:00:00Z' } },
      },
      { resume: true, since: { t2: 4, t1: 4 } },
    ])
    expect(items.flatMap((item) => ('truncated' in item ? [item.thread.id] : []))).toEqual(['t2'])
    expect(server.sent('thread.join')).toEqual([])
    await client.close()
  })

  it('forgets a leave once a re-follow hands that thread back after a new add', async () => {
    const t1 = { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' }
    server.handlers.set('thread.follow', () => ({
      joined: [{ thread: t1, seq: 4, truncated: false }],
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    await client.leave('t1')
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.follow')).toHaveLength(2))
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    expect(items.filter((item) => 'truncated' in item)).toHaveLength(2)
    server.drop()
    await vi.waitFor(() => expect(server.sent('thread.follow')).toHaveLength(3))
    expect(server.sent('thread.follow').at(-1)?.payload).toEqual({ resume: true, since: { t1: 4 } })
    await client.close()
  })

  for (const gone of ['removed', 'deleted'] as const) {
    it(`does not open again after a drop a thread it had open before it was ${gone}`, async () => {
      const t1 = { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' }
      server.handlers.set('thread.follow', () => ({
        joined: [{ thread: t1, seq: 4, truncated: false }],
      }))
      const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
      const items: Item[] = []
      client.subscribe((item) => items.push(item), { includeOwn: true })
      await client.follow()
      await client.join('t1')
      if (gone === 'removed')
        server.push('event', {
          kind: 'member_removed',
          id: 'r5',
          thread_id: 't1',
          seq: 5,
          author: helper,
          created_at: '2026-01-01T12:00:00Z',
          client_id: 'r5',
          identity: server.identity,
        })
      else server.push('signal', { kind: 'thread.deleted', thread_id: 't1', sender: helper })
      server.push('joined', { thread: t1, seq: 6, truncated: false })
      await vi.waitFor(() => expect(items.filter((item) => 'truncated' in item)).toHaveLength(2))
      server.drop()
      await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
      expect(server.sent('thread.join')).toHaveLength(1)
      await client.close()
    })
  }

  it('opens again after a drop only the followed threads it joined', async () => {
    const thread = (id: string) => ({
      id,
      scope: 'acme',
      metadata: {},
      created_at: '2026-01-01T12:00:00Z',
    })
    server.handlers.set('thread.follow', () => ({
      joined: ['t1', 't2'].map((id) => ({ thread: thread(id), seq: 4, truncated: false })),
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    await client.join('t2')
    server.drop()
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    expect(server.sent('thread.join').map((frame) => frame.payload.thread_id)).toEqual(['t2', 't2'])
    await client.close()
  })

  it('hands a follow replay for a thread without a cursor once, before its Joined', async () => {
    const t1 = { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' }
    server.handlers.set('thread.follow', () => {
      server.push('event', message(3, 'one'))
      server.push('event', message(4, 'two'))
      return { joined: [{ thread: t1, seq: 4, truncated: false }] }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow({ resume: true })
    server.push('event', message(4, 'two'))
    server.push('event', message(5, 'three'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 5 }))
    expect(
      items.map((item) => ('truncated' in item ? 'joined' : 'seq' in item ? item.seq : item.kind))
    ).toEqual([3, 4, 'joined', 5])
    await client.close()
  })

  it('re-joins each thread from the cursor it holds when that thread is re-joined', async () => {
    const log: Record<string, Array<Record<string, unknown>>> = {
      t1: [1, 2, 3, 4].map((seq) => ({ ...message(seq, `t1 ${seq}`), thread_id: 't1' })),
      t2: [1, 2, 3, 4].map((seq) => ({ ...message(seq, `t2 ${seq}`), thread_id: 't2' })),
    }
    const thread = (id: string) => ({
      id,
      scope: 'acme',
      metadata: {},
      created_at: '2026-01-01T12:00:00Z',
    })
    server.handlers.set('thread.follow', () => ({
      joined: ['t1', 't2'].map((id) => ({ thread: thread(id), seq: 4, truncated: false })),
    }))
    let landed: string | null = null
    server.handlers.set('thread.join', ({ thread_id, since }) => {
      if (landed === null && since !== null) {
        landed = thread_id === 't1' ? 't2' : 't1'
        const live = { ...message(5, 'live during the re-joins'), thread_id: landed }
        log[landed]?.push(live)
        server.push('event', live)
      }
      const events = log[thread_id] ?? []
      if (since !== null)
        for (const event of events.filter((event) => (event.seq as number) > since))
          server.push('event', event)
      return { thread: thread(thread_id), seq: events.length, truncated: false }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    await client.join('t1')
    await client.join('t2')
    server.drop()
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    expect(server.sent('thread.join').map((frame) => frame.payload.thread_id)).toEqual([
      't1',
      't2',
      't1',
      't2',
    ])
    expect(items.flatMap((item) => ('author' in item ? [[item.thread_id, item.seq]] : []))).toEqual(
      [[landed, 5]]
    )
    await client.close()
  })

  it('hands a followed thread joined since a seq its history once while a live event lands', async () => {
    const log = [1, 2, 3].map((seq) => message(seq, `m${seq}`))
    const t1 = { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' }
    server.handlers.set('thread.follow', () => ({
      joined: [{ thread: t1, seq: 3, truncated: false }],
    }))
    server.handlers.set('thread.join', ({ since }) => {
      const live = message(4, 'live')
      log.push(live)
      server.push('event', live)
      for (const event of log.filter((event) => event.seq > (since ?? log.length)))
        server.push('event', event)
      return { thread: t1, seq: log.length, truncated: false }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    const joined = await client.join('t1', { since: 0 })
    expect(joined.seq).toBe(4)
    const seqs = items.flatMap((item) => ('author' in item ? [item.seq] : []))
    expect([...seqs].sort((a, b) => a - b)).toEqual([1, 2, 3, 4])
    server.push('event', message(4, 'live'))
    server.push('event', message(5, 'after'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 5 }))
    expect(items.flatMap((item) => ('author' in item ? [item.seq] : []))).toEqual([4, 1, 2, 3, 5])
    await client.close()
  })

  it('replays each overlapping join since a followed thread its own window once', async () => {
    const log = Array.from({ length: 20 }, (_, n) => message(n + 1, `m${n + 1}`))
    const t1 = { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' }
    server.handlers.set('thread.follow', () => ({
      joined: [{ thread: t1, seq: 20, truncated: false }],
    }))
    server.handlers.set('thread.join', ({ since }) => {
      if (log.length === 20) {
        const live = message(21, 'live')
        log.push(live)
        server.push('event', live)
      }
      for (const event of log.filter((event) => event.seq > (since ?? log.length)))
        server.push('event', event)
      return { thread: t1, seq: log.length, truncated: false }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    const first = client.join('t1', { since: 5 })
    const second = client.join('t1', { since: 10 })
    expect([(await first).seq, (await second).seq]).toEqual([21, 21])
    const seqs = items.flatMap((item) => ('author' in item ? [item.seq] : []))
    const expected = [
      ...Array.from({ length: 15 }, (_, n) => n + 6),
      ...Array.from({ length: 10 }, (_, n) => n + 11),
      21,
    ]
    expect([...seqs].sort((a, b) => a - b)).toEqual(expected.sort((a, b) => a - b))
    server.push('event', message(22, 'after'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 22 }))
    expect(items.filter((item) => 'author' in item && item.seq > 20)).toHaveLength(2)
    await client.close()
  })

  it('keeps the cursor of a followed thread when a join of it fails', async () => {
    const thread = (id: string) => ({
      id,
      scope: 'acme',
      metadata: {},
      created_at: '2026-01-01T12:00:00Z',
    })
    server.handlers.set('thread.follow', () => ({
      joined: [{ thread: thread('t1'), seq: 3, truncated: false }],
    }))
    server.handlers.set('thread.join', ({ thread_id }) => {
      throw { code: 'forbidden', message: `server: 'ana' may not read in thread '${thread_id}'` }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    const cursors = (client as unknown as { cursors: Map<string, number> }).cursors
    for (const since of [undefined, 0]) {
      await expect(client.join('t1', { since })).rejects.toBeInstanceOf(ForbiddenError)
      expect(cursors.get('t1')).toBe(3)
      server.push('event', message(3, 'repeat'))
    }
    await expect(client.join('t2')).rejects.toBeInstanceOf(ForbiddenError)
    expect(cursors.has('t2')).toBe(false)
    server.push('event', message(4, 'live'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 4 }))
    expect(items.flatMap((item) => ('author' in item ? [item.seq] : []))).toEqual([4])
    await client.close()
  })

  it('joins from its mark with resume, and sends resume only when asked', async () => {
    server.handlers.set('thread.join', ({ thread_id, resume }) => {
      if (resume) for (const seq of [4, 5]) server.push('event', message(seq, `m${seq}`))
      const thread = {
        id: thread_id,
        scope: 'acme',
        metadata: {},
        created_at: '2026-01-01T12:00:00Z',
      }
      return {
        thread: resume ? { ...thread, marked: 3 } : thread,
        seq: resume ? 5 : 0,
        truncated: false,
      }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    expect((await client.join('t2')).thread.marked).toBeUndefined()
    const joined = await client.join('t1', { resume: true })
    expect([joined.seq, joined.thread.marked]).toEqual([5, 3])
    expect(server.sent('thread.join').map((frame) => frame.payload)).toEqual([
      { thread_id: 't2', since: null },
      { thread_id: 't1', since: null, resume: true },
    ])
    server.push('event', message(5, 'repeat'))
    server.push('event', message(6, 'live'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 6 }))
    expect(items.flatMap((item) => ('author' in item ? [item.seq] : []))).toEqual([4, 5, 6])
    await client.close()
  })

  it('takes a resume beyond the replay limit as the latest events, truncated, with the mark to read the rest from', async () => {
    server.handlers.set('thread.join', ({ thread_id }) => {
      for (const seq of [9, 10]) server.push('event', message(seq, `m${seq}`))
      return {
        thread: {
          id: thread_id,
          scope: 'acme',
          metadata: {},
          created_at: '2026-01-01T12:00:00Z',
          marked: 2,
        },
        seq: 10,
        truncated: true,
      }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    const joined = await client.join('t1', { resume: true })
    expect([joined.truncated, joined.thread.marked]).toEqual([true, 2])
    expect(items.flatMap((item) => ('author' in item ? [item.seq] : []))).toEqual([9, 10])
    await client.history('t1', { after: joined.thread.marked, before: 9 })
    expect(server.sent('thread.history').at(-1)?.payload).toMatchObject({ after: 2, before: 9 })
    await client.close()
  })

  it('hands a followed thread joined with resume its history from the mark once while a live event lands', async () => {
    const log = [1, 2, 3].map((seq) => message(seq, `m${seq}`))
    const t1 = { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' }
    server.handlers.set('thread.follow', () => ({
      joined: [{ thread: t1, seq: 3, truncated: false }],
    }))
    server.handlers.set('thread.join', () => {
      const live = message(4, 'live')
      log.push(live)
      server.push('event', live)
      for (const event of log.filter((event) => event.seq > 1)) server.push('event', event)
      return { thread: t1, seq: log.length, truncated: false }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    const joined = await client.join('t1', { resume: true })
    expect(joined.seq).toBe(4)
    server.push('event', message(4, 'live'))
    server.push('event', message(5, 'after'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 5 }))
    expect(items.flatMap((item) => ('author' in item ? [item.seq] : []))).toEqual([4, 2, 3, 5])
    await client.close()
  })

  it('raises a join refused for since with resume and keeps no cursor for it', async () => {
    server.handlers.set('thread.join', ({ since, resume }) => {
      if (since !== null && resume)
        throw { code: 'input', message: 'server: a join takes since or resume, not both' }
      return { thread: {}, seq: 0, truncated: false }
    })
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const refused = client.join('t1', { since: 0, resume: true })
    await expect(refused).rejects.toBeInstanceOf(InputError)
    await expect(refused).rejects.toThrow('server: a join takes since or resume, not both')
    expect((client as unknown as { cursors: Map<string, number> }).cursors.has('t1')).toBe(false)
    await client.close()
  })

  it('forgets a leave once the server delivers that thread again from a new add', async () => {
    const t1 = { id: 't1', scope: 'acme', metadata: {}, created_at: '2026-01-01T12:00:00Z' }
    server.handlers.set('thread.follow', () => ({
      joined: [{ thread: t1, seq: 4, truncated: false }],
    }))
    const client = await connect({ url: 'ws://fake', protocols: ['thread', 'tok'] })
    const items: Item[] = []
    client.subscribe((item) => items.push(item))
    await client.follow()
    await client.leave('t1')
    server.push('joined', { thread: t1, seq: 6, truncated: false })
    await vi.waitFor(() => expect(items.filter((item) => 'truncated' in item)).toHaveLength(2))
    server.drop()
    await vi.waitFor(() => expect(items).toContainEqual({ kind: 'status', status: 'open' }))
    expect(server.sent('thread.leave')).toHaveLength(1)
    expect(server.sent('thread.follow').at(-1)?.payload).toEqual({ resume: true, since: { t1: 6 } })
    server.push('event', message(7, 'still delivered'))
    await vi.waitFor(() => expect(items.at(-1)).toMatchObject({ seq: 7 }))
    await client.close()
  })
})
