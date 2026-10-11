# customer_assistant

## Description

Acme Home's support assistant, in a chat with one customer in the customer's own thread. It shows a streamed reply: the answer is written into the thread as the run produces it, each tool call shows as working and lands as a step under the reply the moment it finishes, and Stop sends a cancel that ends the run and keeps what was written. The assistant follows every thread it belongs to (`me.follow()`). Look at `answer` in server.py: `me.stream(..., in_reply_to=)` with `reply.write` and `reply.finish`, `me.working` on `ToolCallStarted`, `me.step` on `ToolResultReady`, and the `Cancel` signal calling `stop.now()` on the `stop=` that `run` in harness.py passes to `engine.run`.

## Running

```bash
docker run -d --name rfnry-thread-pg -e POSTGRES_USER=thread -e POSTGRES_PASSWORD=thread -p 55436:5432 postgres:16-alpine
export RFNRY_EXAMPLES_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread
export ANTHROPIC_API_KEY=...
npm --prefix ../../packages/typescript install
npm --prefix ../../packages/typescript run build
uv run src/main.py
```

Threads are stored in that Postgres and outlive a restart (`docker start rfnry-thread-pg` once the
container exists).

## Example

Open http://localhost:8000

```bash
curl -s -X POST localhost:8000/threads -b 'name=Dana'
```

```json
{"id": "support-dana", "scope": "acme-home", "metadata": {}, "created_at": "2026-09-30T14:26:14.626811Z"}
```
