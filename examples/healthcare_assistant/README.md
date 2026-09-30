# healthcare_assistant

## Description

A clinic's nurse line assistant: the front desk opens a patient's thread from an intake note, a nurse joins, and the assistant prepares the callback. It shows redaction: `Server(redact=)` masks record numbers and phone numbers before anything is stored, so the note comes back masked. Look at `redact` in harness.py and `mask` in utils.py, then `open_thread` and `join` behind the endpoints.

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
curl -s localhost:8000/threads -H 'content-type: application/json' -b 'name=Kim' -d '{"patient": "Maria Lopez", "message": "Maria Lopez, MRN 00482913, called from 555-201-7788: since starting the new blood pressure pills on Monday she gets dizzy when she stands up, and nearly fainted this morning."}'
```

```json
{"thread": {"id": "patient-maria-lopez", "scope": "clinic-northside", "metadata": {"patient": "Maria Lopez"}, "created_at": "2026-09-30T14:29:30.939134Z"}, "message": {"id": "fa71368a2561486aa5a83fae422b7692", "thread_id": "patient-maria-lopez", "seq": 3, "author": {"role": "user", "id": "kim", "name": "Kim", "metadata": {}}, "created_at": "2026-09-30T14:29:30.939598Z", "client_id": "7d87d243198b4078b31150fc6e07fff1", "in_reply_to": null, "triggered_by": null, "kind": "message", "content": [{"type": "text", "text": "Maria Lopez, [record number], called from [phone number]: since starting the new blood pressure pills on Monday she gets dizzy when she stands up, and nearly fainted this morning."}], "metadata": {}}}
```

```bash
curl -s -X POST localhost:8000/threads/patient-maria-lopez/join -b 'name=Ruth'
```

```json
{"thread_id": "patient-maria-lopez", "identity": {"role": "user", "id": "ruth", "name": "Ruth", "metadata": {}}, "added_at": "2026-09-30T14:29:30.951620Z", "added_by": {"role": "system", "id": "system", "name": "system", "metadata": {}}}
```
