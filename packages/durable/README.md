# Durable XState execution

<!-- package exports and supported versions from package.json and src/index.ts -->

Private workspace experiment for XState `6.0.0-alpha.70` and RivetKit `2.3.31`.
The neutral execution driver is exported from `@demo/xstate-durable`.
The Rivet entry point exports one runtime function, `xstate`. No module imports the café.
A hypothetical published package could expose that function as `@xstate/rivet`.

## Use a normal Rivet actor

<!-- xstate signature from src/rivet/run.ts and plain consumer from ../../examples/cafe/src/fixtures/approval-worker.ts -->

```ts
import { actor, queue, setup } from 'rivetkit';
import type { EventFromLogic, InputFrom } from 'xstate';
import { xstate } from '@demo/xstate-durable/rivet';
import { approvalMachine } from './approval.ts';

type Input = InputFrom<typeof approvalMachine>;
type Event = EventFromLogic<typeof approvalMachine>;

const approval = actor({
  createState: (_c, input: Input) => input,
  queues: { events: queue<Event>() },
  run: xstate(approvalMachine, {
    queue: 'events',
    input: (c) => c.state,
    inspect: true,
  }),
  actions: {
    approve: (c, reviewer: string) =>
      c.queue.send('events', { type: 'approve', reviewer }),
  },
});

const registry = setup({ use: { approval } });
await registry.startAndWait();
```

`xstate` returns a Rivet `run` handler. You own the actor's state, variables, database configuration,
actions, other queues, broadcasts, and lifecycle hooks. One Rivet actor hosts the XState actor tree.
The selected queue must carry the machine's event type. The input callback runs once in a journaled step.
Its CBOR-compatible result is copied before journaling, so later actor-state mutations cannot change machine input.
Nested objects, dates, big integers, and domain identifiers retain their values.
Validate external input in your Rivet actions and `createState`, using your application's schemas.

`queue.send` confirms native durable enqueue. It does not confirm that the machine processed the event.
Use `onSnapshot(context, view, delivery)` to publish processed state through your own state or database.
This callback runs in a journaled step after async effects settle.

## Optional capabilities

<!-- XStateOptions and logic resources from src/rivet/run.ts -->

`inspect: true` connects the real Stately SDK. An inspection object accepts `name`, a `transport` factory,
and an `enabled(context)` predicate. It does not require `createVars`, `onSleep`, or `onDestroy` hooks.

`mapEvent(body)` supports application queue envelopes. Return `{ event, id? }` to extract the machine event
and optionally deduplicate deliveries by an application request ID. The first delivered ID wins.
Validate conflicting payloads in application code. Replay rebuilds the set of processed IDs.

A logic factory can return `{ logic, dispose? }` when the machine needs an execution-scoped service.
It receives `{ executionId, signal }`. Resources are disposed after each workflow reconstruction attempt.
Application effects belong in journaled async actors. External services still require idempotency keys
for a crash between their commit and recording the result.

`historyVersion` identifies the application's replay contract. An incompatible version fails before
input or logic creation. Start a new actor key when changing machine replay semantics.
The adapter rejects existing native workflow history without its protocol marker. Histories from the
previous `durableRivetActor` factory require a new actor key or fresh storage. No migration is supplied.

## Application request receipts

<!-- application-owned command receipts from ../../examples/cafe/src/rivet/commands.ts -->

The café's optional command helper supplies request-ID conflict checks, closing-time admission, and receipts.
These are application behavior, not required adapter setup. The plain approval actor uses only `xstate`.

A SQL submission initially reports `submitting` with `acceptedAt: null`. Native enqueue confirms acceptance
and supplies the accepted timestamp. Until that confirmation, an interrupted caller must retry its request ID.
A matching retry re-enqueues safely. A processed retry returns its original acknowledgement.
Receipts then report `pending`, `delivered`, or `rejected`. Delivery need not cause a state change.
The terminal snapshot and rejection of unfinished submissions commit together.
A completed machine leaves other native queued messages unconsumed. Callers that use native `enqueueAndWait`
must supply their own terminal admission and completion policy. The café uses enqueue-only sends and SQL receipts.
Admission examines submitted commands, including unconfirmed submissions, conservatively.
Event transforms run at the application boundary. Replay uses recorded parsed events.

## Durability boundaries

<!-- replay, timer and inspection implementation from src/execution.ts and src/rivet -->

`DurableActorExecution` owns logical time, root and child delivery, effect execution, mailbox draining,
and inspection checkpoints and turn buffers.
`RivetExecution` supplies timer scheduling and async-result journaling. The `xstate` handler owns Rivet
workflow steps, native queue consumption, publication, and inspection lifecycle.

The handler uses native Rivet workflow journaling and a completable native queue.
The handler journals persisted native completion after effects and publication, then settles workflow completion.
This order also removes retained messages when a replayed workflow has no live completion callback. RivetKit `2.3.31` retains completable messages
until completion, as implemented in its [pinned native queue source](https://github.com/rivet-dev/rivet/blob/f6926c3103809c0119f06c85f0c38ccc4671073c/rivetkit-rust/packages/rivetkit-core/src/actor/queue.rs#L615-L650).
A native fault test kills the runner after receive and before workflow history records the event.
Expired workflow waits check the native queue before choosing a due timer. Timer selection is journaled.
Native queue timestamps order events against timers. Timers win equal timestamps.
Events queued before initialization use the execution's current logical time to prevent clock reversal.

The default embedded SQLite provider works. A caller's `db({ onMigrate })` can own additional tables.
The adapter reserves `_xstate_rivet_protocol`; Rivet reserves `_rivet_wf_kv` for workflow history.
Database providers must support the SQLite `execute` and `transaction` methods used by native workflows.
The returned handler does not expose Rivet's workflow-specific dashboard Inspector tab or replay controls.
Stately SDK inspection works independently.

Inspection belongs to the Rivet actor activation. Workflow waits reuse the connection and do not publish
historical snapshots. The adapter calls `execution.resumeInspection(inspector)` at fresh input boundaries
after replay and `execution.commitInspection(inspector)` after a fresh turn commits. The neutral
driver records the actor tree automatically. A delivery replaces the previous turn's pending events;
it also captures the complete actor tree before that delivery changes state. Committing first restores
that checkpoint with `Inspector.resume`, then sends the fresh inspection events through `Inspector.publish`
without deduplicating identical fresh events. A commit after replay therefore includes every existing actor,
including stopped children, before publishing new events. The SDK binds logical actor identities in event order, even when
pure transitions reconstruct actor handles, while retaining the preceding snapshot for each real event.
Explicit resume uses the current checkpoint and discards both pending replay events and their pre-turn
checkpoint. An empty commit after resume stays quiet. Process restart publishes current state without
invented transitions or replacement actor registrations.
Relay reconnects receive the SDK's current checkpoint.
Completion, sleep, and destruction release inspection resources.

The handler retains full history and drains bounded async jobs serially. Idle waits add history entries.
It does not compact history, migrate machines, distribute child actors, or run arbitrary overlapping async jobs.
Custom actions execute again during replay. Arbitrary external actions do not gain exactly-once execution.

### SDK dependency

<!-- SDK inspection resume requirement from src/execution.ts and package.json -->

The hosts require `@statelyai/sdk` `0.42.0`, which supplies `Inspector.resume` checkpoints and
`Inspector.publish` committed batches. This package and the café pin the same registry version.
Install the café workspace dependencies normally; no SDK checkout or local tarball is required.

### Verifying SDK changes

For SDK development, build and pack the SDK change, then temporarily install its tarball in a
dedicated worktree. Do not commit local tarball paths or publish this private package.

Use Node `>=24.17.0` and pnpm. From the Viz SDK checkout, build and pack the SDK:

```sh
pnpm --filter @statelyai/sdk build
pnpm --filter @statelyai/sdk pack --pack-destination /tmp/durable-sdk
```

From this repository's dedicated worktree, verify the packed SDK:

```sh
node scripts/verify-durable-sdk.mjs /tmp/durable-sdk/statelyai-sdk-VERSION.tgz
```

Use the filename printed by `pack`. The script installs the packed SDK in this worktree, runs the café
workspace typecheck and complete test suite, and restores its original package manifests and lockfile.
Pass optional Vitest file or test-name arguments after the tarball to narrow a rerun.
Use the helper for further checks. A normal pnpm command can reinstall the pinned registry SDK.

## Verify

<!-- native tests from ../../examples/cafe/src/rivet and repository scripts -->

```sh
pnpm --dir examples/cafe install
pnpm test:cafe
pnpm typecheck:cafe
```

Native-engine tests cover the café, an unrelated approval machine, plain Rivet composition, queue acceptance,
request conflicts, deadlines, async-effect recovery, terminal rejection, protocol compatibility, and SDK inspection.
The local host retains its separate saved-history compatibility test.
