import { InputError } from './errors.js'

export const PROTOCOL = 1
export const HELLO = 'hello'

type Payload = Record<string, unknown>
export type Hello = { type: 'hello'; protocol: number }
export type Request = { type: 'request'; id: string; method: string; payload: Payload }
export type Response = { type: 'response'; id: string; payload: Payload }
export type ErrorFrame = { type: 'error'; id: string; error: { code: string; message: string } }
export type Push = { type: 'push'; name: 'event' | 'signal' | 'joined'; payload: Payload }
export type Notify = { type: 'notify'; method: string; payload: Payload }
export type Frame = Hello | Request | Response | ErrorFrame | Push | Notify

const TYPES = new Set(['hello', 'request', 'response', 'error', 'push', 'notify'])

export function encode(frame: Frame): string {
  return JSON.stringify(frame)
}

export function decode(text: string): Frame {
  let frame: unknown
  try {
    frame = JSON.parse(text)
  } catch (error) {
    throw new InputError(`protocol: malformed frame: ${(error as Error).message.toLowerCase()}`)
  }
  if (
    typeof frame !== 'object' ||
    frame === null ||
    !TYPES.has((frame as { type?: string }).type ?? '')
  ) {
    throw new InputError('protocol: malformed frame: unknown type')
  }
  return frame as Frame
}
