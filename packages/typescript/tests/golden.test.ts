import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { decode, encode, type Push } from '../src/protocol.js'
import { EVENT_KINDS, type Message, SIGNAL_KINDS } from '../src/types.js'

const FIXTURES = join(import.meta.dirname, '../../python/tests/fixtures/protocol')
const files = readdirSync(FIXTURES).filter((name) => name.endsWith('.json'))

describe('golden fixtures', () => {
  it('exist', () => {
    expect(files.length).toBe(28)
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

  it('refuses a malformed frame', () => {
    expect(() => decode('not json')).toThrow(/^protocol: malformed frame/)
    expect(() => decode('{"type":"shout"}')).toThrow(/^protocol: malformed frame/)
  })
})
