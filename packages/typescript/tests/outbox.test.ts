import { afterEach, describe, expect, it } from 'vitest'
import { InputError, TransportError } from '../src/errors.js'
import { Outbox } from '../src/outbox.js'
import { timing } from '../src/timing.js'

const saved = { ...timing }
afterEach(() => Object.assign(timing, saved))

const hooks = { isConnected: () => true, isClosed: () => false, waitConnected: async () => {} }

describe('outbox', () => {
  it('keeps per-thread order even when an earlier write retries', async () => {
    timing.backoffMs = 1
    const order: string[] = []
    let failures = 1
    const outbox = new Outbox(async (payload) => {
      if (payload.client_id === 'a' && failures-- > 0)
        throw new TransportError('client: connection lost')
      order.push(String(payload.client_id))
      return { event: payload }
    }, hooks)
    await Promise.all([
      outbox.send('t1', { client_id: 'a' }),
      outbox.send('t1', { client_id: 'b' }),
    ])
    expect(order).toEqual(['a', 'b'])
  })

  it('gives up after the deadline and never retries what the server refused', async () => {
    timing.deadlineMs = 30
    timing.backoffMs = 5
    const outbox = new Outbox(async () => {
      throw new TransportError('client: not connected')
    }, hooks)
    await expect(outbox.send('t1', { client_id: 'c1' })).rejects.toThrow(
      "client: event 'c1' for thread 't1' was not acknowledged within 0.03s"
    )
    let calls = 0
    const refusing = new Outbox(async () => {
      calls += 1
      throw new InputError('server: unknown event kind')
    }, hooks)
    await expect(refusing.send('t1', { client_id: 'c2' })).rejects.toBeInstanceOf(InputError)
    expect(calls).toBe(1)
  })
})
