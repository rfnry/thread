import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { decode, encode, type Push, type Response } from '../src/protocol.js'
import type { Event, Message, Signal, Thread } from '../src/types.js'

const FIXTURES = join(import.meta.dirname, '../../python/tests/fixtures/protocol')
const files = readdirSync(FIXTURES).filter((name) => name.endsWith('.json'))
const EVENT_KINDS = Object.keys({
  message: true,
  step: true,
  edit: true,
  retract: true,
  read: true,
  member_added: true,
  member_removed: true,
  thread_updated: true,
  custom: true,
} satisfies Record<Event['kind'], true>)
const SIGNAL_KINDS = Object.keys({
  working: true,
  idle: true,
  'stream.start': true,
  'stream.delta': true,
  'stream.end': true,
  cancel: true,
  present: true,
  left: true,
  invited: true,
  'thread.deleted': true,
  marked: true,
} satisfies Record<Signal['kind'], true>)

describe('golden fixtures', () => {
  it('exist', () => {
    expect(files.length).toBe(45)
  })

  it.each(files)('%s round-trips', (name) => {
    const text = readFileSync(join(FIXTURES, name), 'utf8')
    expect(JSON.parse(encode(decode(text)))).toEqual(JSON.parse(text))
  })

  it('cover every event and signal kind the types name', () => {
    const pushes = files
      .map((name) => JSON.parse(readFileSync(join(FIXTURES, name), 'utf8')))
      .filter((frame) => frame.type === 'push')
    const kinds = (name: string) =>
      new Set(pushes.filter((frame) => frame.name === name).map((frame) => frame.payload.kind))
    expect(kinds('event')).toEqual(new Set(EVENT_KINDS))
    expect(kinds('signal')).toEqual(new Set(SIGNAL_KINDS))
  })

  it('carry a message with its metadata', () => {
    const text = readFileSync(join(FIXTURES, 'push-event-message-metadata.json'), 'utf8')
    const event = (decode(text) as Push).payload as unknown as Message
    expect(event.kind).toBe('message')
    expect(event.metadata).toEqual({
      run_id: 'r1',
      attempt: 2,
      tags: ['refund'],
      reviewed: false,
    })
  })

  it('carry a listed thread with what a listing adds', () => {
    const text = readFileSync(join(FIXTURES, 'response-thread-list.json'), 'utf8')
    const [thread] = ((decode(text) as Response).payload as { threads: Thread[] }).threads
    expect(thread).toMatchObject({
      id: 't1',
      last_seq: 9,
      last_message_at: '2026-01-01T12:00:00Z',
      marked: 7,
      unread: 2,
      last_message: { kind: 'message', seq: 1, content: [{ type: 'text', text: 'hello!' }] },
    })
  })

  it('refuses a malformed frame', () => {
    expect(() => decode('not json')).toThrow(/^protocol: malformed frame/)
    expect(() => decode('{"type":"shout"}')).toThrow(/^protocol: malformed frame/)
  })
})
