## Running

```bash
export ANTHROPIC_API_KEY=...
export DECISION_API_KEY=...
export LOCAL_DECISION_MODEL=...
npm --prefix ../../packages/typescript install
npm --prefix ../../packages/typescript run build
uv run main.py
```

## Example

Open http://localhost:8000

```bash
curl -s localhost:8000/threads -H 'content-type: application/json' -H 'X-Tenant: clinic-northside' -b 'name=Kim' -d '{"patient": "Maria Lopez"}'
```

```json
{"id": "patient-maria-lopez", "scope": "clinic-northside", "metadata": {"patient": "Maria Lopez", "handled_by": "assistant"}, "created_at": "2026-09-30T05:13:01.988130Z"}
```

```bash
curl -s -X POST localhost:8000/threads/patient-maria-lopez/handoff -H 'X-Tenant: clinic-northside'
```

```json
{"id": "patient-maria-lopez", "scope": "clinic-northside", "metadata": {"patient": "Maria Lopez", "handled_by": "people"}, "created_at": "2026-09-30T05:13:01.988130Z"}
```
