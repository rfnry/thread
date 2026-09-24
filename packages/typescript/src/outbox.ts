import { TransportError } from './errors.js'
import { timing } from './timing.js'

type Payload = Record<string, unknown>

export type OutboxHooks = {
  isConnected(): boolean
  isClosed(): boolean
  waitConnected(ms: number): Promise<void>
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

export class Outbox {
  private readonly tails = new Map<string, Promise<unknown>>()

  constructor(
    private readonly deliver: (payload: Payload) => Promise<Payload>,
    private readonly hooks: OutboxHooks
  ) {}

  send(threadId: string, payload: Payload): Promise<Payload> {
    const previous = this.tails.get(threadId) ?? Promise.resolve()
    const run = () => this.attempt(threadId, payload)
    const result = previous.then(run, run)
    this.tails.set(
      threadId,
      result.catch(() => undefined)
    )
    return result
  }

  private async attempt(threadId: string, payload: Payload): Promise<Payload> {
    const started = Date.now()
    for (let attempt = 0; ; attempt++) {
      try {
        return await this.deliver(payload)
      } catch (error) {
        if (!(error instanceof TransportError) || this.hooks.isClosed()) throw error
        const elapsed = Date.now() - started
        if (elapsed >= timing.deadlineMs) {
          throw new TransportError(
            `client: event '${String(payload.client_id)}' for thread '${threadId}' was not acknowledged within ${timing.deadlineMs / 1000}s`
          )
        }
        const delay = Math.min(
          timing.backoffMs * 2 ** attempt,
          timing.backoffCapMs,
          timing.deadlineMs - elapsed
        )
        await (this.hooks.isConnected() ? sleep(delay) : this.hooks.waitConnected(delay))
      }
    }
  }
}
