import {
  ConfigurationError,
  ForbiddenError,
  InputError,
  raised,
  type ThreadException,
  TransportError,
} from './errors.js'
import { Outbox } from './outbox.js'
import { decode, encode, type Frame, HELLO, PROTOCOL } from './protocol.js'
import { timing } from './timing.js'
import type {
  Content,
  Custom,
  Edit,
  Event,
  Identity,
  Item,
  Joined,
  Member,
  Message,
  Part,
  Read,
  Retract,
  Signal,
  Status,
  Step,
  Thread,
} from './types.js'

type Payload = Record<string, unknown>
export type Protocols = string | string[]
export type ConnectOptions = { url: string; protocols?: Protocols | (() => Promise<Protocols>) }
export type Listener = (item: Item) => void
export type StreamOptions = {
  inReplyTo?: string
  triggeredBy?: Identity
  metadata?: Record<string, unknown>
}
export type SendOptions = StreamOptions & { clientId?: string }
export type WriteOptions = { clientId?: string }
export type StepOptions = Omit<SendOptions, 'metadata'> & {
  name: string
  input?: Record<string, unknown>
  output?: Part[]
  isError?: boolean
  durationMs?: number
}

const parts = (content: Content): Part[] =>
  typeof content === 'string' ? [{ type: 'text', text: content }] : content
const newId = (): string => crypto.randomUUID().replaceAll('-', '')
const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

function open(url: string, protocols: Protocols | undefined): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    let socket: WebSocket
    try {
      socket = new WebSocket(url, protocols)
    } catch (error) {
      reject(new TransportError(`client: could not connect: ${String(error)}`))
      return
    }
    socket.onopen = () => resolve(socket)
    socket.onerror = () => reject(new TransportError(`client: could not connect to ${url}`))
  })
}

function handshake(socket: WebSocket): Promise<Payload> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new TransportError('client: the hello was not answered')),
      timing.handshakeMs
    )
    socket.onmessage = (message) => {
      clearTimeout(timer)
      let frame: Frame
      try {
        frame = decode(String(message.data))
      } catch (error) {
        reject(error)
        return
      }
      if (frame.type === 'error' && frame.id === HELLO)
        reject(raised(frame.error.code, frame.error.message))
      else if (frame.type === 'response' && frame.id === HELLO) resolve(frame.payload)
      else reject(new TransportError('client: the server did not answer the hello'))
    }
    socket.onclose = () => {
      clearTimeout(timer)
      reject(new TransportError('client: the hello was not answered'))
    }
    socket.send(encode({ type: 'hello', protocol: PROTOCOL }))
  })
}

type Wire = {
  request(method: string, payload: Payload): Promise<Payload>
  notify(method: string, payload: Payload): void
  message(
    threadId: string,
    content: Content,
    options: StreamOptions,
    clientId: string
  ): Promise<Message>
}

export class Stream {
  readonly id = newId()
  text = ''
  private index = 0
  private ended = false
  private readonly started: Promise<unknown>

  constructor(
    private readonly wire: Wire,
    private readonly threadId: string,
    private readonly options: StreamOptions
  ) {
    this.started = wire.request('signal.send', {
      thread_id: threadId,
      kind: 'stream.start',
      stream_id: this.id,
      in_reply_to: options.inReplyTo ?? null,
    })
    this.started.catch(() => undefined)
  }

  write(text: string): void {
    this.text += text
    const index = this.index++
    void this.started.then(
      () =>
        this.wire.notify('signal.send', {
          thread_id: this.threadId,
          kind: 'stream.delta',
          stream_id: this.id,
          index,
          text,
        }),
      () => undefined
    )
  }

  async finish(content?: Content): Promise<Message> {
    if (this.ended) throw new InputError(`client: stream '${this.id}' has already ended`)
    this.ended = true
    await this.started
    try {
      return await this.wire.message(this.threadId, content ?? this.text, this.options, this.id)
    } finally {
      await this.end()
    }
  }

  async abort(): Promise<void> {
    if (this.ended) return
    this.ended = true
    await this.started.catch(() => undefined)
    await this.end()
  }

  private async end(): Promise<void> {
    await this.wire
      .request('signal.send', { thread_id: this.threadId, kind: 'stream.end', stream_id: this.id })
      .catch(() => undefined)
  }
}

let establish: (client: Client) => Promise<void>

export class Client {
  static {
    establish = (client) => client.establish()
  }

  identity!: Identity
  scope!: string
  readonly closed: Promise<ThreadException | null>
  private settle!: (reason: ThreadException | null) => void
  private socket: WebSocket | null = null
  private readonly pending = new Map<
    string,
    { resolve: (payload: Payload) => void; reject: (error: Error) => void }
  >()
  private nextId = 1
  private readonly listeners = new Map<Listener, boolean>()
  private readonly cursors = new Map<string, number>()
  private readonly rewinds = new Map<string, { since: number; held: number; taken: Set<number> }>()
  private following = false
  private readonly held = new Set<string>()
  private readonly left = new Set<string>()
  private leaving = new Set<string>()
  private stopped = false
  private current: Status = 'open'
  private failure: ThreadException | null = null
  private waiting: Array<() => void> = []
  private readonly outbox: Outbox
  private readonly wire: Wire

  constructor(private readonly options: ConnectOptions) {
    this.closed = new Promise((resolve) => {
      this.settle = resolve
    })
    this.outbox = new Outbox((payload) => this.request('event.send', payload), {
      isConnected: () => this.socket !== null,
      isClosed: () => this.stopped,
      waitConnected: (ms) =>
        new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, ms)
          this.waiting.push(() => {
            clearTimeout(timer)
            resolve()
          })
        }),
    })
    this.wire = {
      request: (method, payload) => this.request(method, payload),
      notify: (method, payload) => this.notify(method, payload),
      message: (threadId, content, options, clientId) =>
        this.message(threadId, content, options, clientId),
    }
  }

  private async establish(): Promise<void> {
    const socket = await open(this.options.url, await this.offered())
    let reply: Payload
    try {
      reply = await handshake(socket)
    } catch (error) {
      socket.close()
      throw error
    }
    if (this.stopped) {
      socket.close(1000, 'client: closed')
      throw new TransportError('client: the client is closed')
    }
    this.identity = reply.identity as Identity
    this.scope = String(reply.scope)
    this.socket = socket
    socket.onmessage = (message) => this.receive(String(message.data))
    socket.onclose = () => this.lost(socket)
    for (const wake of this.waiting.splice(0)) wake()
  }

  private async offered(): Promise<Protocols | undefined> {
    const { protocols } = this.options
    if (typeof protocols !== 'function') return protocols
    try {
      return await protocols()
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error)
      throw new ForbiddenError(`client: the protocols function threw: ${reason}`)
    }
  }

  private receive(text: string): void {
    let frame: Frame
    try {
      frame = decode(text)
    } catch {
      return
    }
    if (frame.type === 'response' || frame.type === 'error') {
      const waiter = this.pending.get(frame.id)
      if (!waiter) return
      this.pending.delete(frame.id)
      if (frame.type === 'error') waiter.reject(raised(frame.error.code, frame.error.message))
      else waiter.resolve(frame.payload)
      return
    }
    if (frame.type !== 'push') return
    if (frame.name === 'joined') {
      const joined = frame.payload as unknown as Joined
      this.left.delete(joined.thread.id)
      this.leaving.delete(joined.thread.id)
      this.cursors.set(
        joined.thread.id,
        Math.max(this.cursors.get(joined.thread.id) ?? 0, joined.seq)
      )
      this.emit(joined)
      return
    }
    if (frame.name === 'event') {
      const event = frame.payload as unknown as Event
      if (this.leaving.has(event.thread_id)) return
      const rewind = this.rewinds.get(event.thread_id)
      if (rewind && event.seq > rewind.since && event.seq <= rewind.held) {
        if (rewind.taken.has(event.seq)) return
        rewind.taken.add(event.seq)
        this.emit(event)
        return
      }
      const cursor = this.cursors.get(event.thread_id)
      if (cursor !== undefined) {
        if (event.seq <= cursor) return
        this.cursors.set(event.thread_id, event.seq)
      }
      if (event.kind === 'member_removed' && event.identity.id === this.identity.id)
        this.cursors.delete(event.thread_id)
      this.emit(event)
      return
    }
    const signal = frame.payload as unknown as Signal
    if (this.leaving.has(signal.thread_id) && signal.kind !== 'marked') return
    if (signal.kind === 'thread.deleted') this.cursors.delete(signal.thread_id)
    this.emit(signal)
  }

  private emit(item: Item): void {
    const who = 'author' in item ? item.author : 'sender' in item ? item.sender : null
    const own =
      who !== null && who.id === this.identity.id && !('kind' in item && item.kind === 'marked')
    for (const [listener, includeOwn] of this.listeners) {
      if (!includeOwn && own) continue
      try {
        listener(item)
      } catch (error) {
        queueMicrotask(() => {
          throw error
        })
      }
    }
  }

  private change(status: Status): void {
    if (this.current === status || this.current === 'closed') return
    this.current = status
    this.emit({ kind: 'status', status })
  }

  private lost(socket: WebSocket): void {
    if (this.socket !== socket) return
    this.socket = null
    for (const waiter of this.pending.values())
      waiter.reject(new TransportError('client: connection lost'))
    this.pending.clear()
    if (this.stopped) return
    this.change('reconnecting')
    void this.reconnect()
  }

  private async reconnect(): Promise<void> {
    for (let attempt = 0; !this.stopped; attempt++) {
      const ceiling =
        attempt === 0 ? 0 : Math.min(timing.reconnectCapMs, timing.reconnectMs * 2 ** (attempt - 1))
      await sleep(Math.random() * ceiling)
      if (this.stopped) return
      try {
        await this.establish()
      } catch (error) {
        if (error instanceof ForbiddenError || error instanceof ConfigurationError) {
          this.fail(error)
          return
        }
        continue
      }
      await this.rejoin()
      if (this.socket) this.change('open')
      return
    }
  }

  private async rejoin(): Promise<void> {
    let followed = false
    if (this.following) {
      this.leaving = new Set(this.left)
      try {
        await this.followFrom(true, Object.fromEntries(this.cursors))
        followed = true
      } catch (error) {
        if (error instanceof TransportError) return
      }
      for (const threadId of [...this.leaving].sort()) {
        if (!this.leaving.has(threadId)) continue
        try {
          await this.request('thread.leave', { thread_id: threadId })
        } catch (error) {
          if (error instanceof TransportError) return
        }
      }
      this.leaving = new Set()
    }
    for (const threadId of [...this.cursors.keys()]) {
      const cursor = this.cursors.get(threadId)
      if (cursor === undefined || (followed && !this.held.has(threadId))) continue
      try {
        this.emit(await this.joinFrom(threadId, cursor))
      } catch (error) {
        if (error instanceof TransportError) return
        this.cursors.delete(threadId)
        this.held.delete(threadId)
      }
    }
  }

  private fail(error: ThreadException): void {
    this.failure = error
    this.stopped = true
    this.change('closed')
    this.settle(error)
  }

  private request(method: string, payload: Payload): Promise<Payload> {
    if (this.stopped)
      return Promise.reject(this.failure ?? new TransportError('client: the client is closed'))
    const socket = this.socket
    if (!socket) return Promise.reject(new TransportError('client: not connected'))
    const id = String(this.nextId++)
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject })
      try {
        socket.send(encode({ type: 'request', id, method, payload }))
      } catch (error) {
        this.pending.delete(id)
        reject(new TransportError(`client: ${method} failed: ${String(error)}`))
      }
    })
  }

  private notify(method: string, payload: Payload): void {
    try {
      this.socket?.send(encode({ type: 'notify', method, payload }))
    } catch {
      return
    }
  }

  private async event(threadId: string, fields: Payload, clientId?: string): Promise<Event> {
    const reply = await this.outbox.send(threadId, {
      thread_id: threadId,
      client_id: clientId ?? newId(),
      ...fields,
    })
    return reply.event as Event
  }

  private async message(
    threadId: string,
    content: Content,
    options: StreamOptions,
    clientId?: string
  ): Promise<Message> {
    const fields = {
      kind: 'message',
      content: parts(content),
      in_reply_to: options.inReplyTo ?? null,
      triggered_by: options.triggeredBy ?? null,
      metadata: options.metadata ?? {},
    }
    return (await this.event(threadId, fields, clientId)) as Message
  }

  private async joinFrom(threadId: string, since: number | null): Promise<Joined> {
    const held = this.cursors.get(threadId)
    const rewind =
      since !== null && held !== undefined && since < held
        ? { since, held, taken: new Set<number>() }
        : null
    if (rewind) this.rewinds.set(threadId, rewind)
    else this.cursors.set(threadId, since ?? 0)
    let joined: Joined
    try {
      joined = (await this.request('thread.join', {
        thread_id: threadId,
        since,
      })) as unknown as Joined
    } finally {
      if (rewind && this.rewinds.get(threadId) === rewind) this.rewinds.delete(threadId)
    }
    this.cursors.set(
      threadId,
      joined.seq < (since ?? 0) ? joined.seq : Math.max(this.cursors.get(threadId) ?? 0, joined.seq)
    )
    return joined
  }

  async createThread(
    options: { threadId?: string; metadata?: Record<string, unknown> } = {}
  ): Promise<Thread> {
    const reply = await this.request('thread.create', {
      thread_id: options.threadId ?? null,
      metadata: options.metadata ?? {},
    })
    return reply.thread as Thread
  }

  async getThread(threadId: string): Promise<Thread> {
    return (await this.request('thread.get', { thread_id: threadId })).thread as Thread
  }

  async listThreads(
    options: {
      before?: string
      limit?: number
      order?: 'created' | 'activity'
      preview?: boolean
    } = {}
  ): Promise<Thread[]> {
    const payload: Payload = { before: options.before ?? null, limit: options.limit ?? 50 }
    if (options.order !== undefined && options.order !== 'created') payload.order = options.order
    if (options.preview) payload.preview = true
    const reply = await this.request('thread.list', payload)
    return reply.threads as Thread[]
  }

  async updateThread(threadId: string, metadata: Record<string, unknown>): Promise<Thread> {
    return (await this.request('thread.update', { thread_id: threadId, metadata })).thread as Thread
  }

  async deleteThread(threadId: string): Promise<void> {
    await this.request('thread.delete', { thread_id: threadId })
    this.cursors.delete(threadId)
  }

  async join(threadId: string, options: { since?: number } = {}): Promise<Joined> {
    this.left.delete(threadId)
    this.leaving.delete(threadId)
    try {
      const joined = await this.joinFrom(threadId, options.since ?? null)
      this.held.add(threadId)
      return joined
    } catch (error) {
      this.cursors.delete(threadId)
      throw error
    }
  }

  async leave(threadId: string): Promise<void> {
    this.cursors.delete(threadId)
    this.held.delete(threadId)
    if (this.following) this.left.add(threadId)
    await this.request('thread.leave', { thread_id: threadId })
  }

  async follow(options: { resume?: boolean } = {}): Promise<Joined[]> {
    this.left.clear()
    this.leaving.clear()
    return this.followFrom(options.resume ?? false, null)
  }

  private async followFrom(
    resume: boolean,
    since: Record<string, number> | null
  ): Promise<Joined[]> {
    const reply = await this.request('thread.follow', { resume, since })
    this.following = true
    const found = (reply.joined as Joined[]).filter((joined) => !this.leaving.has(joined.thread.id))
    for (const joined of found) {
      const id = joined.thread.id
      const sent = since?.[id]
      this.cursors.set(
        id,
        sent !== undefined && joined.seq < sent
          ? joined.seq
          : Math.max(this.cursors.get(id) ?? 0, joined.seq)
      )
      this.emit(joined)
    }
    return found
  }

  async mark(threadId: string, seq: number): Promise<number> {
    const reply = await this.request('thread.mark', { thread_id: threadId, seq })
    return reply.seq as number
  }

  async history(
    threadId: string,
    options: { before?: number; after?: number; limit?: number; kinds?: string[] } = {}
  ): Promise<Event[]> {
    const payload: Payload = {
      thread_id: threadId,
      before: options.before ?? null,
      limit: options.limit ?? 50,
      kinds: options.kinds ?? null,
    }
    if (options.after !== undefined) payload.after = options.after
    const reply = await this.request('thread.history', payload)
    return reply.events as Event[]
  }

  async present(threadId: string): Promise<Identity[]> {
    return (await this.request('thread.present', { thread_id: threadId })).identities as Identity[]
  }

  async addMember(threadId: string, identity: Identity): Promise<Member> {
    return (await this.request('member.add', { thread_id: threadId, identity })).member as Member
  }

  async removeMember(threadId: string, identity: Identity): Promise<void> {
    await this.request('member.remove', { thread_id: threadId, identity_id: identity.id })
  }

  async listMembers(threadId: string): Promise<Member[]> {
    return (await this.request('member.list', { thread_id: threadId })).members as Member[]
  }

  send(threadId: string, content: Content, options: SendOptions = {}): Promise<Message> {
    return this.message(threadId, content, options, options.clientId)
  }

  stream(threadId: string, options: StreamOptions = {}): Stream {
    return new Stream(this.wire, threadId, options)
  }

  async step(threadId: string, options: StepOptions): Promise<Step> {
    return (await this.event(
      threadId,
      {
        kind: 'step',
        name: options.name,
        input: options.input ?? {},
        output: options.output ?? [],
        is_error: options.isError ?? false,
        duration_ms: options.durationMs ?? null,
        in_reply_to: options.inReplyTo ?? null,
        triggered_by: options.triggeredBy ?? null,
      },
      options.clientId
    )) as Step
  }

  async edit(
    threadId: string,
    target: string,
    content: Content,
    options: WriteOptions = {}
  ): Promise<Edit> {
    return (await this.event(
      threadId,
      { kind: 'edit', target, content: parts(content) },
      options.clientId
    )) as Edit
  }

  async retract(threadId: string, target: string, options: WriteOptions = {}): Promise<Retract> {
    return (await this.event(threadId, { kind: 'retract', target }, options.clientId)) as Retract
  }

  async read(threadId: string, upToSeq: number): Promise<Read> {
    return (await this.event(threadId, { kind: 'read', up_to_seq: upToSeq })) as Read
  }

  async custom(
    threadId: string,
    name: string,
    payload: Record<string, unknown>,
    options: WriteOptions = {}
  ): Promise<Custom> {
    return (await this.event(
      threadId,
      { kind: 'custom', name, payload },
      options.clientId
    )) as Custom
  }

  async working(threadId: string, label?: string): Promise<void> {
    await this.request('signal.send', {
      thread_id: threadId,
      kind: 'working',
      label: label ?? null,
    })
  }

  async idle(threadId: string): Promise<void> {
    await this.request('signal.send', { thread_id: threadId, kind: 'idle' })
  }

  async cancel(threadId: string, to: Identity): Promise<void> {
    await this.request('signal.send', { thread_id: threadId, kind: 'cancel', to })
  }

  get status(): Status {
    return this.current
  }

  subscribe(listener: Listener, options: { includeOwn?: boolean } = {}): () => void {
    this.listeners.set(listener, options.includeOwn ?? false)
    return () => {
      this.listeners.delete(listener)
    }
  }

  async close(): Promise<void> {
    if (this.stopped && this.socket === null) return
    this.stopped = true
    const socket = this.socket
    this.socket = null
    for (const waiter of this.pending.values())
      waiter.reject(new TransportError('client: the client is closed'))
    this.pending.clear()
    socket?.close(1000, 'client: closed')
    this.change('closed')
    this.settle(null)
  }
}

export async function connect(options: ConnectOptions): Promise<Client> {
  const client = new Client(options)
  await establish(client)
  return client
}
