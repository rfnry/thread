# customer_assistant

## Description

Acme Home's support assistant, in a chat with one customer in the customer's own thread. It shows a streamed reply: the answer is written into the thread as the run produces it, each tool call shows as working and is kept as a step under the reply, and Stop sends a cancel that ends the run and keeps what was written. Look at `run` in harness.py: `me.stream(..., in_reply_to=)` with `reply.write` and `reply.finish`, `me.working`, `me.step`, and the `Cancel` signal calling `stop.now()` on `engine.run(..., stop=)`.

## Running

```bash
export ANTHROPIC_API_KEY=...
npm --prefix ../../packages/typescript install
npm --prefix ../../packages/typescript run build
uv run src/main.py
```

## Example

Open http://localhost:8000

```bash
curl -s -X POST localhost:8000/threads -b 'name=Dana'
```

```json
{"id": "support-dana", "scope": "acme-home", "metadata": {}, "created_at": "2026-09-30T14:26:14.626811Z"}
```
