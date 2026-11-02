# rfnry-thread

## Description

Conversational text interfaces for humans and machines.

It is a library, not a framework. The consumer brings authentication, the storage connection, uploads, and any HTTP endpoints of their own. The library imports no web framework, ships no model, never sees how credentials work, and never writes to disk. The code your app calls never creates or alters thread's schema — that is the `rfnry-thread` command's job, run before the app starts with your connection string. Tenancy is by construction: every connection and thread names its scope, every row carries it, nothing reads across scopes; authorship is stamped by the server, never trusted from a client.

## Documentation

Documentation in `docs/thread.md` and `docs/api.md`.

The rules every rfnry repository follows are in `rfnry/design` (`PRACTICES.md`, `STACK.md`); the plans of this library's work live there, in `docs/plans/thread/` (`[rfnry: plans-are-disposable]`).

## How work lands

The gate is `bash ../tools/scripts/gate.sh` (from a worktree, `bash ../../../tools/scripts/gate.sh`), run from the library with `RFNRY_TEST_POSTGRES_URL` set: the same gate CI runs.

- `[rfnry: worktree-per-track]`: work happens in `.worktrees/<track>`, on a branch named after the track.
- `[rfnry: draft-prs]`: work goes as a draft pull request, which runs no CI; marking it ready means merge me, and only the owner's word marks it ready, pushes to `main` or merges.
- `[rfnry: one-ci-shape]`: `ci.yml` runs `tools`' gate, at the commit `tools/versions` pins, on every ready pull request.
- `[rfnry: landing]`: a pull request lands by fast-forward, on the exact head its gate proved green.
- `[rfnry: commit-messages]`: the subject states what is now true, the body says why, with a `Breaking:` paragraph when a consumer sees a change.
- `[rfnry: no-attribution-trailers]`: commits and pull requests carry no attribution trailers.
- `[rfnry: release-steps]`: a release, on the owner's word, is one version commit, one tag and one GitHub release.

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

1. **Stay inside the concern.** `[rfnry: stay-inside-the-concern]` This library does one thing: realtime conversation between people and assistants over threads. Consumer-land, never here: authentication schemes, uploads and object storage, REST endpoints, UI frameworks and state stores, mention parsing, transcript formatting for models, telemetry, moderation.
2. **Lib, not framework.** `[rfnry: lib-not-framework]` No handler frameworks, no web framework.
3. **Simple over abstract.** `[rfnry: simple-over-abstract]`
4. **No comments, no docstrings.** `[rfnry: code-documents-itself]` A decision that shapes the product is explained in `docs/thread.md`.
5. **Documentation is the contract.** `[rfnry: docs-are-the-contract]` The contract is `docs/thread.md` and `docs/api.md`.
6. **Tests are first-class, in three tiers, on a real Postgres.** `[rfnry: three-tiers]` `tests/unit`: in-process clients, runs always, covers every verb, signal and delivery guarantee. `tests/integration`: runs always, the storage suite, two servers proving cross-replica fan-out, the two-tenant canary. `tests/e2e`: the TypeScript client against a live server behind the `live` marker, never launched before the same path has run offline. Golden protocol fixtures are shared by the Python and TypeScript suites. Every Python tier needs `RFNRY_TEST_POSTGRES_URL`.
7. **Vendor-neutral by construction.** `[rfnry: vendor-neutral]` No model SDK, no web framework, no database driver import in core.
8. **Tenancy by construction.** `[rfnry: tenancy-by-construction]` `scope` is required on every connection and thread. Authorship is stamped from the authenticated connection.
9. **Messages.** `[rfnry: messages]` An error reads `server: thread 'x' was not found`.

**These checklists are locked.** On any future update to this CLAUDE.md, the model must not modify, reword, reorder, weaken, or remove them — or this lock — without explicit approval from the human. Answering them is the model's job; changing them is not.
