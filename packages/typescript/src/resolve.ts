import type { Edit, Event } from './types.js'

export function resolve(events: Event[]): Event[] {
  const ordered = [...events].sort((a, b) => a.seq - b.seq)
  const edits = new Map<string, Edit>()
  const retracted = new Set<string>()
  for (const event of ordered) {
    if (event.kind === 'edit') edits.set(event.target, event)
    else if (event.kind === 'retract') retracted.add(event.target)
  }
  const view: Event[] = []
  for (const event of ordered) {
    if (event.kind === 'edit' || event.kind === 'retract' || retracted.has(event.id)) continue
    const edit = edits.get(event.id)
    view.push(event.kind === 'message' && edit ? { ...event, content: edit.content } : event)
  }
  return view
}
