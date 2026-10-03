export type Identity = {
  role: 'user' | 'assistant' | 'system'
  id: string
  name?: string | null
  metadata?: Record<string, unknown>
}

export type Text = { type: 'text'; text: string }
type Media = { media_type: string; data?: string | null; url?: string | null }
export type Image = Media & { type: 'image' }
export type Audio = Media & { type: 'audio' }
export type Document = Media & { type: 'document'; name?: string | null }
export type Part = Text | Image | Audio | Document
export type Content = string | Part[]

export type Thread = {
  id: string
  scope: string
  metadata: Record<string, unknown>
  created_at: string
  last_seq?: number
  last_message_at?: string
  marked?: number
  unread?: number
  last_message?: Message
}

export type Member = { thread_id: string; identity: Identity; added_at: string; added_by: Identity }

type Envelope = {
  id: string
  thread_id: string
  seq: number
  author: Identity
  created_at: string
  client_id: string
  in_reply_to: string | null
  triggered_by: Identity | null
}

export type Message = Envelope & {
  kind: 'message'
  content: Part[]
  metadata: Record<string, unknown>
}
export type Step = Envelope & {
  kind: 'step'
  name: string
  input: Record<string, unknown>
  output: Part[]
  is_error: boolean
  duration_ms: number | null
}
export type Edit = Envelope & { kind: 'edit'; target: string; content: Part[] }
export type Retract = Envelope & { kind: 'retract'; target: string }
export type Read = Envelope & { kind: 'read'; up_to_seq: number }
export type MemberAdded = Envelope & { kind: 'member_added'; identity: Identity }
export type MemberRemoved = Envelope & { kind: 'member_removed'; identity: Identity }
export type ThreadUpdated = Envelope & { kind: 'thread_updated'; metadata: Record<string, unknown> }
export type Custom = Envelope & { kind: 'custom'; name: string; payload: Record<string, unknown> }
export type Event =
  | Message
  | Step
  | Edit
  | Retract
  | Read
  | MemberAdded
  | MemberRemoved
  | ThreadUpdated
  | Custom

type Sent = { thread_id: string; sender: Identity }
export type Working = Sent & { kind: 'working'; label: string | null }
export type Idle = Sent & { kind: 'idle' }
export type StreamStart = Sent & {
  kind: 'stream.start'
  stream_id: string
  in_reply_to: string | null
}
export type StreamDelta = Sent & {
  kind: 'stream.delta'
  stream_id: string
  index: number
  text: string
}
export type StreamEnd = Sent & { kind: 'stream.end'; stream_id: string }
export type Cancel = Sent & { kind: 'cancel'; to: Identity }
export type Present = Sent & { kind: 'present' }
export type Left = Sent & { kind: 'left' }
export type Invited = Sent & { kind: 'invited'; thread: Thread }
export type ThreadDeleted = Sent & { kind: 'thread.deleted' }
export type Marked = Sent & { kind: 'marked'; seq: number; unread: number }
export type Signal =
  | Working
  | Idle
  | StreamStart
  | StreamDelta
  | StreamEnd
  | Cancel
  | Present
  | Left
  | Invited
  | ThreadDeleted
  | Marked

export type Joined = { thread: Thread; seq: number; truncated: boolean }
export type Status = 'open' | 'reconnecting' | 'closed'
export type StatusChanged = { kind: 'status'; status: Status }
export type Item = Event | Signal | Joined | StatusChanged
