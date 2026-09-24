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
})
