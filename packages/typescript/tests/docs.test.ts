import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { Client, Stream } from '../src/client.js'
import * as api from '../src/index.js'

const docs = readFileSync(join(import.meta.dirname, '../../../docs/thread.md'), 'utf8')
const blocks = [...docs.matchAll(/```ts\n([\s\S]*?)```/g)].map((match) => match[1] ?? '')
const section = docs.split('### The TypeScript client')[1]?.split('\n## ')[0] ?? ''
const NOT_VERBS = new Set([
  'connect',
  'resolve',
  'thread_id',
  'in_reply_to',
  'includeOwn',
  'inReplyTo',
  'metadata',
  'null',
  'protocols',
])

describe('docs', () => {
  it('names only verbs the client has', () => {
    const used = blocks.flatMap((block) => [...block.matchAll(/client\.(\w+)\(/g)].map((m) => m[1]))
    const listed = [...section.matchAll(/`(\w+)`/g)]
      .map((m) => m[1])
      .filter((name) => /^[a-z]/.test(name ?? ''))
    const verbs = new Set([...used, ...listed].filter((name) => !NOT_VERBS.has(name ?? '')))
    for (const verb of verbs)
      expect(
        typeof (Client.prototype as unknown as Record<string, unknown>)[verb ?? ''],
        verb
      ).toBe('function')
  })

  it('names only stream methods the stream has', () => {
    const used = blocks.flatMap((block) => [...block.matchAll(/reply\.(\w+)\(/g)].map((m) => m[1]))
    for (const name of used)
      expect(typeof (Stream.prototype as unknown as Record<string, unknown>)[name ?? '']).toBe(
        'function'
      )
  })

  it('imports only what the package exports', () => {
    const imported = blocks.flatMap((block) =>
      [...block.matchAll(/import \{([^}]*)\} from '@rfnry\/thread'/g)].flatMap((m) =>
        (m[1] ?? '').split(',').map((s) => s.trim())
      )
    )
    for (const name of imported) expect(api, name).toHaveProperty(name)
  })
})
