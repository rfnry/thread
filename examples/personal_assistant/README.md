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
curl -s localhost:8000/nudge -H 'content-type: application/json' -d '{"to": "Jordan", "about": "the washing machine warranty has ended; ask whether to book a paid service check before winter"}'
```

```json
{"thread_id": "fa8c5a2667d9436c9927d84e3000bd72", "reply": "Jordan — heads up: the washing machine (Acme WM-700) warranty ended back on 2025-06-15, so any repairs now are out of pocket. Winter's coming and it gets heavier use then. Want me to book a paid service check before winter, or hold off?", "run_id": "d58ab2a3a97b41e68baba9190f3a01ae"}
```
