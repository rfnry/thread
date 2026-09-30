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
curl -s localhost:8000/threads -H 'content-type: application/json' -H 'X-Tenant: plant-reno' -b 'name=Ana' -d '{"line": "line 1", "title": "M-13 short shots since this morning", "task": "troubleshoot"}'
```

```json
{"id": "b576ef6a75f440f58c4e39f77b390e7e", "scope": "plant-reno", "metadata": {"line": "line 1", "title": "M-13 short shots since this morning", "task": "troubleshoot", "technician": "ana", "elevated": false}, "created_at": "2026-09-30T05:10:39.043616Z"}
```

```bash
curl -s -X POST localhost:8000/threads/b576ef6a75f440f58c4e39f77b390e7e/elevate -H 'X-Tenant: plant-reno' -b 'name=Ana'
```

```json
{"id": "b576ef6a75f440f58c4e39f77b390e7e", "scope": "plant-reno", "metadata": {"line": "line 1", "title": "M-13 short shots since this morning", "task": "troubleshoot", "technician": "ana", "elevated": true}, "created_at": "2026-09-30T05:10:39.043616Z"}
```

```bash
curl -s localhost:8000/channels -H 'X-Tenant: plant-reno'
```

```json
[{"line": "line 1", "machines": "Molders M-11 to M-14: 200-ton hydraulic, caps and closures", "elevated": [{"id": "b576ef6a75f440f58c4e39f77b390e7e", "scope": "plant-reno", "metadata": {"line": "line 1", "title": "M-13 short shots since this morning", "task": "troubleshoot", "technician": "ana", "elevated": true}, "created_at": "2026-09-30T05:10:39.043616Z"}]}, {"line": "line 2", "machines": "Molders M-21 and M-22: 350-ton hydraulic, crates", "elevated": []}]
```
