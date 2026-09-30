## Running

```bash
export ANTHROPIC_API_KEY=...
npm --prefix ../../packages/typescript install
npm --prefix ../../packages/typescript run build
uv run main.py
```

## Example

Open http://localhost:8000

```bash
curl -s localhost:8000/nudge -H 'content-type: application/json' -d '{"to": "Jordan", "about": "the washing machine warranty has ended; ask whether to book a paid service check before winter"}'
```

```json
{"thread_id": "d1f9ddcf919e4656bcb682c0c1797a83", "reply": "Hi Jordan — heads up: the washing machine (Acme WM-700, serial WX-30317) warranty ended back on 2025-06-15, so any repairs now would be out of pocket. Want me to book a paid service check before winter?", "run_id": "a73317ea0cc44d1891caa206ca9f04f1"}
```
