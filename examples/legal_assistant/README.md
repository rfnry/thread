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
curl -s localhost:8000/threads -H 'content-type: application/json' -H 'X-Tenant: firm-acme' -b 'name=Priya' -d '{"contract": "northwind-msa"}'
```

```json
{"id": "matter-northwind-msa", "scope": "firm-acme", "metadata": {"contract": "northwind-msa"}, "created_at": "2026-09-30T05:16:42.440731Z"}
```

```bash
curl -s localhost:8000/runs/e504200770134782a69425c0cda3c75d/review -H 'content-type: application/json' -H 'X-Tenant: firm-acme' -d '{"stance": "objection", "correction": "Clause 2 departs from the playbook: invoices due in 15 days and 2% a month interest, against 30 days and 1% at most. Say so without calling it compliant first."}'
```

```json
{"label_id": "705e144e1f0c4cf1ae31a4c46734c1d0", "run_id": "e504200770134782a69425c0cda3c75d", "scope": "firm-acme", "at": "2026-09-30T05:17:17.365861Z", "source": "human", "review": {"stance": "objection", "correction": "Clause 2 departs from the playbook: invoices due in 15 days and 2% a month interest, against 30 days and 1% at most. Say so without calling it compliant first."}}
```

```bash
curl -s -X POST localhost:8000/export -H 'X-Tenant: firm-acme'
```

```json
{"keys": ["firm-acme/clauses/decision/20260930T051717373064Z.jsonl", "firm-acme/reviewer/generation/20260930T051717373064Z.jsonl", "firm-acme/reviewer/preference/20260930T051717373064Z.jsonl"], "rows": {"decision": 12, "generation": 1, "preference": 1}, "watermark": "2026-09-30T05:17:17.365861Z"}
```
