import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import {
  ConfigurationError,
  ForbiddenError,
  InputError,
  NotFoundError,
  raised,
  StorageError,
  ThreadException,
  TransportError,
} from '../src/errors.js'

const SRC = join(import.meta.dirname, '../src')
const PREFIXES: Record<string, string> = { outbox: 'client' }

const FAMILY = [
  NotFoundError,
  InputError,
  ConfigurationError,
  ForbiddenError,
  StorageError,
  TransportError,
]

describe('errors', () => {
  it('maps every wire code to its class with the same message', () => {
    for (const Class of [ThreadException, ...FAMILY]) {
      const error = raised(new Class('x').code, "server: thread 'x' was not found")
      expect(error).toBeInstanceOf(Class)
      expect(error).toBeInstanceOf(ThreadException)
      expect(error.message).toBe("server: thread 'x' was not found")
      expect(error.name).toBe(Class.name)
    }
  })

  it('maps an unknown code to the root', () => {
    expect(raised('teapot', 'server: no').constructor).toBe(ThreadException)
  })

  it('starts every message with its module name, then lowercase', () => {
    const messages = readdirSync(SRC).flatMap((name) => {
      const stem = name.replace(/\.ts$/, '')
      return [
        ...readFileSync(join(SRC, name), 'utf8').matchAll(
          /new \w+(?:Error|Exception)\(\s*['`]([^'`]*)/g
        ),
      ].map((match) => ({ name, prefix: `${PREFIXES[stem] ?? stem}: `, text: match[1] ?? '' }))
    })
    expect(messages.length).toBeGreaterThan(0)
    const wrong = messages.filter(
      ({ prefix, text }) =>
        !text.startsWith(prefix) || /^[A-Z\s]|^$/.test(text.slice(prefix.length))
    )
    expect(wrong).toEqual([])
  })
})
