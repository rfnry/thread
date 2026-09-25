import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { decode, encode } from '../src/protocol.js'
import { EVENT_KINDS, SIGNAL_KINDS } from '../src/types.js'

const FIXTURES = join(import.meta.dirname, '../../../tests/fixtures/protocol')
const files = readdirSync(FIXTURES).filter((name) => name.endsWith('.json'))

describe('golden fixtures', () => {
  it('exist', () => {
    expect(files.length).toBe(27)
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

  it('refuses a malformed frame', () => {
    expect(() => decode('not json')).toThrow(/^protocol: malformed frame/)
    expect(() => decode('{"type":"shout"}')).toThrow(/^protocol: malformed frame/)
  })
})
