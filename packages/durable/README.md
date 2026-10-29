# Durable XState hosts

<!-- package exports and supported versions from package.json and src/index.ts -->

Experimental, private workspace package for XState `6.0.0-alpha.70` and RivetKit `2.3.31`.
The neutral execution driver is exported from `@demo/xstate-durable`.
The Rivet adapter and actor host are exported from `@demo/xstate-durable/rivet`.
No module in this package imports the café.

## Register a machine on Rivet

<!-- durableRivetActor options and actions from src/rivet/actor.ts -->

```ts
import { setup } from 'rivetkit';
import { durableRivetActor } from '@demo/xstate-durable/rivet';
import { approvalMachine, approvalInput, approvalEvent } from './approval.ts';

const approval = durableRivetActor({
  logic: approvalMachine,
  inputSchema: approvalInput,
  eventSchema: approvalEvent,
});

const registry = setup({ use: { approval } });
await registry.startAndWait();
```

The machine and schemas define application behavior. The host supplies durable command acceptance,
request-ID deduplication, timer ordering, journaled async results, replay, snapshot queries, and inspection.
One Rivet actor hosts the complete XState actor tree.

Create an instance with `{ config: machineInput, inspect: true }` as `createWithInput`.
Set `inspect: false` to disable the Stately relay. Dispatch public events through
`handle.dispatch(event, requestId)`. Reusing a request ID with the same event is safe;
reusing it with a different event returns `conflicting_command`.

`dispatch` acknowledges durable acceptance. `getCommand(requestId)` returns `pending`, `delivered`,
or `rejected`; delivery need not cause a transition. `getSnapshot()` returns the latest committed
actor tree, timers, event count, root status, and inspector URL. `info()` returns execution metadata.
After termination, new commands fail with `execution_closed`; queued commands are rejected atomically
with publication of the terminal snapshot.

The Rivet client accepts the event schema's input type. The machine and admission hook receive its
parsed output. Defaults and transforms run once at acceptance; replay uses the recorded result.
RivetKit's `createWithInput` is typed as
`unknown`; the host validates input at runtime with `inputSchema`. Events are also parsed at the RPC boundary.

## Supply application services

<!-- logic resource lifecycle and admission hook from src/rivet/actor.ts -->

Use a logic factory when the machine needs an execution-scoped client or other resource:

```ts
const approval = durableRivetActor({
  inputSchema: approvalInput,
  eventSchema: approvalEvent,
  logic: ({ executionId, signal }) => {
    const service = openApprovalService({ executionId, signal });
    return {
      logic: createApprovalMachine(service),
      dispose: () => service.close(),
    };
  },
});
```

The factory runs during workflow reconstruction. Its `dispose` callback runs when that workflow
attempt exits. Application effects belong in journaled async actors, rather than the factory.
External services still need idempotency for a crash between their commit and recording the async result.

An optional `admission({ event, commands })` hook applies application rules inside the command-acceptance
transaction, after request-ID deduplication. `commands.has(event)` checks the accepted command log,
including already delivered commands. The café uses it to reject arrivals after an accepted close command.
`terminal` configures the terminal error code and messages. These rules stay outside the shared runtime.

## Durability and inspection boundaries

<!-- replay, timer and inspection implementation from src/execution.ts and src/rivet -->

`DurableActorExecution` owns logical time, root and child delivery, effect execution, mailbox draining,
and inspection checkpoints and turn buffers.
`RivetExecution` supplies timer scheduling and async-result journaling. `durableRivetActor` owns Rivet
workflow steps, the durable inbox, receipts, publication, wake notifications, and actor lifecycle.

The host retains complete workflow history and reconstructs the actor tree by replay. Timers use recorded
logical time, with due timers winning ties against commands accepted at the same timestamp. Journal keys
and workflow step order are persistent contracts. Keep `historyVersion` unchanged for compatible moves;
use a new actor identity or storage for incompatible machine changes. This host does not migrate machines
or compact history.

Inspection belongs to the Rivet actor activation. Workflow waits reuse the connection and do not publish
historical snapshots. The adapter calls `execution.resumeInspection(inspector)` at fresh input boundaries
after replay and `execution.commitInspection(inspector)` after a fresh turn commits. The neutral
driver records the actor tree automatically. A delivery replaces the previous turn's pending events;
committing sends genuine inspection events through the SDK's `Inspector.publish` API without
deduplicating identical fresh events. The SDK binds logical actor identities in event order, even when
pure transitions reconstruct actor handles, while retaining the preceding snapshot for each real event.
Resume uses the SDK's explicit checkpoint API and discards pending replay events, so process restart
publishes current state without invented transitions or replacement actor registrations.
Relay reconnects receive the SDK's current checkpoint.
Completion, sleep, and destruction release inspection resources.

The adapter drains async jobs serially. It supports the bounded async-actor pattern used by these examples;
it does not provide overlapping long-running jobs, independently distributed child actors, or exactly-once
arbitrary custom actions. Custom actions execute again during replay.

### SDK release prerequisite

<!-- SDK inspection resume requirement from src/execution.ts and package.json -->

This branch requires the SDK's new `Inspector.resume` and `Inspector.publish` APIs from
[Viz PR #973](https://github.com/statelyai/viz/pull/973). They are not available in the pinned registry
release `0.40.0`. Keep this consumer change unmerged until the SDK APIs are published, then update
`@statelyai/sdk` in this package and `examples/cafe/package.json` and regenerate the café workspace lockfile.
For local verification before publication, build and pack the SDK change, then temporarily install its
tarball in a dedicated worktree. Do not commit local tarball paths or publish this private package.

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

## Verify reuse

<!-- unrelated consumer and native tests from ../../examples/cafe/src/fixtures/approval-worker.ts and ../../examples/cafe/src/rivet/host.test.ts -->

From the repository root, run:

```sh
pnpm --dir examples/cafe install
pnpm --dir examples/cafe exec vitest run src/rivet/host.test.ts
pnpm test:cafe
pnpm typecheck:cafe
```

The unrelated document-approval consumer imports only the public Rivet export. Native-engine tests exercise
event payloads, request deduplication, journaled validation, a preserved deadline across runner restart,
terminal rejection, and an ordinary Rivet actor in the same registry. Café tests retain the payment,
timer-ordering, crash-recovery, saved-history, and inspector lifecycle regressions.
