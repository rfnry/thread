# personal_assistant

## Description

Jordan's personal assistant, which speaks first. POST /nudge has it open a thread, add the person and write before anyone has said anything, and the person's reply is answered with that conversation. Look at `nudge` in server.py: `me.create_thread`, `me.add_member` and `me.send`, then `answer` for the reply; both call `run` in harness.py.

## Running

```bash
docker run -d --name rfnry-thread-pg -e POSTGRES_USER=thread -e POSTGRES_PASSWORD=thread -p 55436:5432 postgres:16-alpine
export RFNRY_EXAMPLES_POSTGRES_URL=postgresql://thread:thread@127.0.0.1:55436/thread
export ANTHROPIC_API_KEY=...
npm --prefix ../../packages/typescript install
npm --prefix ../../packages/typescript run build
uv run src/main.py
```

Threads are stored in that Postgres and outlive a restart; `docker start rfnry-thread-pg` starts it
again after a reboot.

## Example

Open http://localhost:8000

```bash
curl -s localhost:8000/nudge -H 'content-type: application/json' -d '{"to": "Jordan", "about": "the washing machine warranty has ended; ask whether to book a paid service check before winter"}'
```

```json
{"thread_id": "fa8c5a2667d9436c9927d84e3000bd72", "reply": "Jordan — heads up: the washing machine (Acme WM-700) warranty ended back on 2025-06-15, so any repairs now are out of pocket. Winter's coming and it gets heavier use then. Want me to book a paid service check before winter, or hold off?", "run_id": "d58ab2a3a97b41e68baba9190f3a01ae"}
```
