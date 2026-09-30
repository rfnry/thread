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
curl -s -X POST localhost:8000/threads -b 'name=Dana'
```

```json
{"id": "support-dana", "scope": "acme-home", "metadata": {}, "created_at": "2026-09-30T05:07:04.184698Z"}
```
