import { describe, expect, it } from 'vitest'
import { resolve } from '../src/resolve.js'
import type { Event, Identity } from '../src/types.js'

const ana: Identity = { role: 'user', id: 'ana', name: 'Ana', metadata: {} }
const base = (seq: number) => ({
  id: `e${seq}`,
  thread_id: 't1',
  seq,
  author: ana,
  created_at: '2026-01-01T12:00:00Z',
  client_id: `c${seq}`,
  in_reply_to: null,
  triggered_by: null,
})
const message = (seq: number, text: string): Event => ({
  ...base(seq),
  kind: 'message',
  content: [{ type: 'text', text }],
  metadata: { runId: `r${seq}` },
})

describe('resolve', () => {
  it('applies the latest edit and drops retractions and the events that carry them', () => {
    const view = resolve([
      message(3, 'c'),
      message(1, 'a'),
      { ...base(4), kind: 'edit', target: 'e1', content: [{ type: 'text', text: 'A' }] },
      { ...base(5), kind: 'edit', target: 'e1', content: [{ type: 'text', text: 'A!' }] },
      message(2, 'b'),
      { ...base(6), kind: 'retract', target: 'e2' },
    ])
    expect(view.map((event) => event.seq)).toEqual([1, 3])
    expect(view[0]).toMatchObject({
      content: [{ type: 'text', text: 'A!' }],
      metadata: { runId: 'r1' },
    })
  })
})
