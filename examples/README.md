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
| `approve_in_a_thread` | an engine agent with a workspace whose `Shell`, `Write` and `Edit` calls wait for the user's answer in the thread through `approve=`: the request is a message naming the tool and its command or path, the user replies allow, deny or always (no more questions this run), the tools that read the agent's folder pass, and each step records whether it was approved |
| `steering` | a person steers an engine assistant mid-run: a follow-up lands through the run's `inbox` at the next step and the plan adapts; "hold on, let's pause" is just another message the model wraps up on; a `cancel` signal calls `stop.now()` and the finished steps are kept; editing an earlier message rewinds the conversation to it, and the abandoned run is reviewed in the refinery |
| `two_replicas` | two servers on one Postgres, a person on each; events and signals cross through `LISTEN/NOTIFY` |
| `typescript_client` | `@rfnry/thread` in Node against a Python server with an echo assistant; the token rides in the subprotocol list, as a browser's would |
| `channels_and_dms` | a workspace in one scope: `#general` and `#support` as threads, a DM as a two-member thread; presence, an edit, a retraction, read cursors, `history`, `resolve()` and one person's event stream as a timeline; no model |
| `factory_assistant` | family example (thread, memory, engine): the operator picks the factory (the scope) and the machine up front, no model involved, and the thread carries `{factory, machine}`; the assistant's `MemoryTools` are pinned to that machine's corpus, and the operator invites a colleague with `add_member` who asks a follow-up; the decision model resolves entity identity on write and judges hits on read — asked a torque, the assistant searches with `order_by="evidence"`, sees two die-change revisions disagree, and answers from rev B, which supersedes rev A; a manual shared as a `Document` is filed through the `ingest` tool and answers the next question; the south thread is pinned to south corpora only and cannot reach press-1 |
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

`two_replicas` reads `THREAD_POSTGRES_URL`, defaulting to the development container on port 55436.
`typescript_client` needs the package built first (`npm run build` in `packages/typescript`), then
`npm install`, `uv run serve.py` in one terminal and `node main.mjs` in another.
`uv` builds each folder's environment on first run; the per-folder `uv.lock` files are ignored by
git. Lint an example with its own ruff: `uv run ruff format . && uv run ruff check .`.
