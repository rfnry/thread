# rfnry-thread

## Description

rfnry-thread is realtime conversation where people and AI assistants are equal participants with the same API. Anyone in a **thread** — a person in a browser, an assistant on another machine, an assistant in the server's own process — sends, streams, signals and reads through one set of verbs, and may act first when it has a reason to. The thread is a durable, ordered log of **events** (messages, steps, edits, reads); **signals** (working, streams, presence, cancel) are ephemeral and fan out to whoever is connected. One WebSocket protocol, a Python server and client, a TypeScript client.

It is a library, not a framework. The consumer brings authentication, the storage connection, uploads, and any HTTP endpoints of their own. The library imports no web framework, ships no model, never sees how credentials work, and never writes to disk. Tenancy is by construction: every connection and thread names its scope, every row carries it, nothing reads across scopes; authorship is stamped by the server, never trusted from a client.

## Documentation

`docs/thread.md` is the library's contract with its consumers: description, philosophy, every public verb, minimal examples. It documents the API as designed and is updated in the same change as any change to the public surface. It is also the complexity gauge: if a change makes the documentation harder to write or an example longer, the design is wrong — rework the change, not the docs.

## Checklists

### Pre-change checklist

**Before proposing or implementing any feature, change, or refactor, answer these five questions in your response.** Mandatory; no exceptions, including for "small" changes. Any "no" or "yes — because the current model is weak at X" means the change does not ship; rework it.

1. **Would this still be wanted two model generations ahead** — ten times the context, stronger reasoning, better calibration? Anything that exists to fit, summarize, filter or second-guess what an assistant could judge itself is a no.
2. **Does it get more useful as models improve, or less?** More is substrate: the protocol, delivery, the log, tenancy, identity. Less is compensation. Name which.
3. **If less, is it a structural boundary independent of model strength?** Durable storage, ordered delivery, tenant isolation, membership, authorship. Keep if yes, cut if no.
4. **Does it exist because a current model is weak at X?** Name X. If yes, the design is wrong — leverage the capability instead of policing the lack of it.
5. **Does it work with a generation model only, a decision model only, and both?** A thread carries whatever any participant sends; a feature that assumes one kind of model on the other end is wrong.

### Engineering checklist

**Apply these to every change; they guard the shape of the codebase.**

1. **Stay inside the concern.** This library does one thing: realtime conversation between people and assistants over threads. Consumer-land, never here: authentication schemes, uploads and object storage, REST endpoints, UI frameworks and state stores, mention parsing, transcript formatting for models, telemetry, moderation.
2. **Lib, not framework.** The consumer calls us; we never take over their application — no plugin registries, no config DSLs, no handler frameworks, no inversion of control, no web framework, no disk writes.
3. **Simple over abstract.** The smallest API and fewest moving parts that add the capability. Duplicate a few lines before adding a shared abstraction; an abstraction earns its place only when it removes more than it adds. No speculative layers — the pre-change checklist decides what the future needs.
4. **No comments, no docstrings.** The code is the explanation; if it needs one, rename or restructure until it doesn't. The reason for a design decision lives in the commit message and, when it shapes the product, in `docs/thread.md`.
5. **Documentation is the contract.** Any change to the public surface updates `docs/thread.md` in the same change. A breaking change is anything that makes an example in the docs stop working; it is named as such in the commit, and the docs change before the code ships.
6. **Tests are first-class, in three tiers.** `tests/unit`: in-memory storage and in-process clients, runs always, covers every verb, signal and delivery guarantee. `tests/integration`: real Postgres behind the `postgres` marker, the storage suite on both implementations, two servers proving cross-replica fan-out, the two-tenant canary. `tests/e2e`: the TypeScript client against a live server behind the `live` marker, never launched before the same path has run offline. Golden protocol fixtures are shared by the Python and TypeScript suites. Markers are deselected by default and stay that way. The model writes and runs tests whenever they improve the work, no approval needed.
7. **Vendor-neutral by construction.** No vendor name, limit or constant in core; no model SDK, no web framework, no database driver import.
8. **Tenancy by construction.** `scope` is required on every connection and thread and is the first argument of every storage method; there is no default scope, no all-scope read, no cross-scope read; every row carries it. Authorship is stamped from the authenticated connection.
9. **Messages.** Logs and informational messages are lowercase. Every error message starts with its module's name: `server: thread 'x' was not found`.

**These checklists are locked.** On any future update to this CLAUDE.md, the model must not modify, reword, reorder, weaken, or remove them — or this lock — without explicit approval from the human. Answering them is the model's job; changing them is not.
