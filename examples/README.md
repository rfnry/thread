# Examples

Each folder is a standalone project: a `main.py` that reads top to bottom and a `pyproject.toml`
that shows how few dependencies it needs. A folder that uses a model runs it through rfnry-engine
and carries a `providers.py` copied byte for byte from rfnry-engine's or rfnry-memory's examples —
rfnry-thread ships no model. Servers are served with the `websockets` package through a five-line
adapter, which also stands in for your authentication: it refuses an upgrade without a known token
and hands `serve` who it is. Your framework and your authentication are yours.

| example | what it shows |
| --- | --- |
| `streaming_with_steps` | a working label, a step under the question, a streamed reply stored as one message, the resolved log |
| `proactive_assistant` | an assistant opens a thread, adds a person, and speaks first; the person's socket client receives `Invited`, joins with `since=0`; presence before and after |
| `person_and_assistant` | you in the terminal over a WebSocket, an engine agent in the server's process answering with a stream |
| `two_assistants` | two engine agents on two vendors debating in one thread; when to stop is their code |
| `engine_agent_in_a_thread` | `ThreadTools` mounted on an engine run; the run's tool calls mirrored as steps and a working label |
| `two_replicas` | two servers on one Postgres, a person on each; events and signals cross through `LISTEN/NOTIFY` |
| `typescript_client` | `@rfnry/thread` in Node against a Python server with an echo assistant; the token rides in the subprotocol list, as a browser's would |
| `channels_and_dms` | a workspace in one scope: `#general` and `#support` as threads, a DM as a two-member thread; presence, an edit, a retraction, read cursors, `history`, `resolve()` and one person's event stream as a timeline; no model |
| `factory_assistant` | family example (thread, memory, engine): two factories as two scopes, manuals ingested per machine; a decision agent picks the machine, a generation agent answers with `MemoryTools` pinned to its corpus, steps under the reply; a manual shared as a `Document` is ingested and answers the next question; the south floor cannot reach press-1 |
| `support_copilot` | family example (thread, engine): support agents bring a complaint, a receipt `Image` and an invoice PDF `Document`; the assistant reads them, delegates to a decision agent for approve, deny or escalate, replies with its steps under the reply; a second agent objects and a `review` lands in the refinery |

## Running

The keys live outside the repository, in a file with `ANTHROPIC_API_KEY` and `GOOGLE_API_KEY`;
`factory_assistant` also needs `VOYAGE_API_KEY` and `DECISION_API_KEY`, and `support_copilot`
`DECISION_API_KEY`.
Run an example from inside its folder:

```bash
cd examples/person_and_assistant
uv run --env-file ../../../.env.examples main.py
```

`two_replicas` also needs `RFNRY_EXAMPLES_POSTGRES_URL`, a Postgres database, in the same file.
`typescript_client` needs the package built first (`npm run build` in `packages/typescript`), then
`npm install`, `uv run serve.py` in one terminal and `node main.mjs` in another.
`uv` builds each folder's environment on first run; the per-folder `uv.lock` files are ignored by
git. Lint an example with its own ruff: `uv run ruff format . && uv run ruff check .`.
