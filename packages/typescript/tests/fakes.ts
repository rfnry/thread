import { vi } from 'vitest'

type Frame = Record<string, any>
export type Handler = (payload: Frame) => Frame | Promise<Frame>

export class FakeSocket {
  readyState = 0
  onopen: ((event: unknown) => void) | null = null
  onmessage: ((event: { data: string }) => void) | null = null
  onclose: ((event: { code: number }) => void) | null = null
  onerror: ((event: unknown) => void) | null = null
  readonly sent: Frame[] = []

  constructor(
    readonly url: string,
    readonly server: FakeServer,
    protocols?: string | string[]
  ) {
    server.sockets.push(this)
    server.opened.push(protocols)
    setTimeout(() => {
      if (server.down) {
        this.readyState = 3
        this.onerror?.({})
        this.onclose?.({ code: 1006 })
        return
      }
      this.readyState = 1
      this.onopen?.({})
    }, 0)
  }

  send(text: string): void {
    if (this.readyState !== 1) throw new Error('fake: socket is not open')
    const frame = JSON.parse(text)
    this.sent.push(frame)
    setTimeout(() => void this.server.receive(this, frame), 0)
  }

  close(code = 1000, _reason?: string): void {
    if (this.readyState === 3) return
    this.readyState = 3
    setTimeout(() => this.onclose?.({ code }), 0)
  }

  deliver(frame: Frame): void {
    if (this.readyState === 1) this.onmessage?.({ data: JSON.stringify(frame) })
  }
}

export class FakeServer {
  readonly sockets: FakeSocket[] = []
  readonly handlers = new Map<string, Handler>()
  readonly hellos: Frame[] = []
  readonly opened: Array<string | string[] | undefined> = []
  readonly stored = new Map<string, Frame>()
  down = false
  dropAck = false
  seq = 0
  identity: Frame = { role: 'user', id: 'ana', name: 'Ana', metadata: {} }

  install(): this {
    const server = this
    vi.stubGlobal(
      'WebSocket',
      class extends FakeSocket {
        constructor(url: string, protocols?: string | string[]) {
          super(url, server, protocols)
        }
      }
    )
    this.handlers.set('event.send', (payload) => {
      const key = `${payload.thread_id}:${payload.client_id}`
      if (!this.stored.has(key)) {
        this.seq += 1
        this.stored.set(key, {
          ...payload,
          id: `e${this.seq}`,
          seq: this.seq,
          author: this.identity,
          created_at: '2026-01-01T12:00:00Z',
          in_reply_to: payload.in_reply_to ?? null,
          triggered_by: payload.triggered_by ?? null,
        })
      }
      return { event: this.stored.get(key) }
    })
    this.handlers.set('thread.join', (payload) => ({
      thread: {
        id: payload.thread_id,
        scope: 'acme',
        metadata: {},
        created_at: '2026-01-01T12:00:00Z',
      },
      seq: this.seq,
      truncated: false,
    }))
    return this
  }

  get current(): FakeSocket | undefined {
    return this.sockets.filter((socket) => socket.readyState === 1).at(-1)
  }

  sent(method?: string): Frame[] {
    return this.sockets
      .flatMap((socket) => socket.sent)
      .filter((frame) => frame.type !== 'hello' && (!method || frame.method === method))
  }

  async receive(socket: FakeSocket, frame: Frame): Promise<void> {
    if (frame.type === 'hello') {
      this.hellos.push(frame)
      socket.deliver({
        type: 'response',
        id: 'hello',
        payload: { identity: this.identity, scope: 'acme' },
      })
      return
    }
    if (frame.type !== 'request') return
    try {
      const payload = await (this.handlers.get(frame.method) ?? (() => ({})))(frame.payload)
      if (this.dropAck && frame.method === 'event.send') {
        this.dropAck = false
        socket.close(1006)
        return
      }
      socket.deliver({ type: 'response', id: frame.id, payload })
    } catch (error) {
      const { code, message } = error as { code: string; message: string }
      socket.deliver({ type: 'error', id: frame.id, error: { code, message } })
    }
  }

  push(name: 'event' | 'signal' | 'joined', payload: Frame): void {
    this.current?.deliver({ type: 'push', name, payload })
  }

  drop(): void {
    this.current?.close(1006)
  }
}

export const helper = { role: 'assistant', id: 'helper', name: 'Helper', metadata: {} } as const

export function message(seq: number, text: string, author: Frame = helper): Frame {
  return {
    kind: 'message',
    id: `m${seq}`,
    thread_id: 't1',
    seq,
    author,
    created_at: '2026-01-01T12:00:00Z',
    client_id: `c${seq}`,
    in_reply_to: null,
    triggered_by: null,
    content: [{ type: 'text', text }],
  }
}
