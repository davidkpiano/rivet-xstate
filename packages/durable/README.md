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

Inspection belongs to the real Rivet activation. Workflow replay suppresses historical emissions.
An actual activation or runner restart hydrates the current tree once. SDK reconnects receive its current checkpoint.
Completion, sleep, and destruction release inspection resources.

The handler retains full history and drains bounded async jobs serially. Idle waits add history entries.
It does not compact history, migrate machines, distribute child actors, or run arbitrary overlapping async jobs.
Custom actions execute again during replay. Arbitrary external actions do not gain exactly-once execution.

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
