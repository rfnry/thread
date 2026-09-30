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
curl -s localhost:8000/channels
```

```json
[{"id": "line-2", "scope": "plant-reno", "metadata": {"channel": true, "line": "line 2", "machines": "Molders M-21 and M-22: 350-ton hydraulic, crates"}, "created_at": "2026-09-30T14:28:28.236700Z"}, {"id": "line-1", "scope": "plant-reno", "metadata": {"channel": true, "line": "line 1", "machines": "Molders M-11 to M-14: 200-ton hydraulic, caps and closures"}, "created_at": "2026-09-30T14:28:28.236593Z"}]
```

```bash
curl -s -X POST localhost:8000/channels/line-1/join -b 'name=Bruno'
```

```json
{"thread_id": "line-1", "identity": {"role": "user", "id": "bruno", "name": "Bruno", "metadata": {}}, "added_at": "2026-09-30T14:28:28.755573Z", "added_by": {"role": "system", "id": "system", "name": "system", "metadata": {}}}
```
