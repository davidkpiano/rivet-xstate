# Durable café

<!-- dependencies and engines from package.json -->

A Node café simulation using XState `6.0.0-alpha.70`, SQLite or RivetKit `2.3.31`, and `@statelyai/sdk` `0.40.0`.
Requires Node 24.17 or newer. Payments are simulated. No provider account or API key is required.

## Run and inspect the actors

<!-- installation and scripts from package.json and ../../package.json -->

From the repository root:

```sh
pnpm --dir examples/cafe install
pnpm cafe --open
```

The process prints its Stately inspector link. `--open` also opens it in your browser.
The inspector shows the live café actor tree, each machine's statechart, events, and context.
Inspect customers, orders, baristas, equipment, payments, and invoked payment actors.
The SDK receives actual XState inspection events through `createDurable`.
The current inspector may show unresolved targets for v6 transitions implemented as callbacks.
Their runtime behavior is covered by the simulation tests.

The inspector uses the hosted Stately relay. The simulation and recovery run locally.
Use `--no-inspect` to run entirely offline. Inspector availability does not affect café execution.

```sh
pnpm cafe --customers 10 --baristas 3 --tick-ms 300 --open
pnpm cafe --db .cafe/second.sqlite --customers 4 --baristas 2
```

The initial input is saved with the run. On resume, omit the input flags or supply the exact original input.
Use a different `--db` path for another run.

## Operate the café

<!-- commands parsed by src/main.ts and src/model.ts -->

Customers arrive at staggered times. Orders alternate between espresso and latte.
Baristas share a grinder, espresso machine, and frother. The terminal serializes payments.
Customers can lose patience; their orders handle cancellation and payment compensation.
Breaking equipment pauses its operator. Repair restarts the full interrupted grinding, extraction,
or frothing step. This models redoing interrupted work. Process recovery instead preserves the deadline
of a step that was already running when the process stopped.

Enter these commands in the running process:

```text
status
arrive latte
arrive espresso
cancel order-3
break grinder
repair grinder
break terminal
repair terminal
close
crash
quit
```

`close` stops new arrivals and lets existing orders settle. `quit` stops the process cleanly.
`crash` sends SIGKILL to the current Node process. Restart with the same `--db` to recover.
For example, crash while a barista is brewing, then run `pnpm cafe --open` again.
The same inspection room is reused, and pending timers retain their original deadlines.
Overdue simulated work catches up on restart.

Commands also accept JSON:

```json
{ "type": "arrival", "drink": "latte", "patienceTicks": 4 }
```

Use `--json` for machine-readable status output, `--run-for 5000` for a timed run, or `--help` for all flags.

## How durability works

<!-- local host implementation from src/local-host.ts and src/store.ts -->

One café is one durable execution containing many co-located actors.
The domain machines import XState and receive ordinary async payment services.
They contain no SQLite or Stately SDK calls.

<!-- shared execution mechanics from ../../packages/durable/src/execution.ts and both provider hosts -->

Both hosts use `DurableActorExecution` from `@demo/xstate-durable` for actor creation, logical time, mailbox settling, inspection,
and delivery routing. Providers supply timer storage and async execution/journaling hooks.
SQLite and Rivet keep their existing journal keys, completion ordering, and persisted histories.

The local host records external events and timer firings before delivery.
It rebuilds the actor tree by replaying that history with a stable execution identity.
Local actor starts, sends, and stops run again during replay.
Completed async actors reuse their journaled results through `runLogic`.

SQLite stores timer deadlines and fake payment receipts separately from the actor tree.
Charges and refunds use order-based idempotency keys. If the process dies after a charge commits
but before the async result is journaled, retrying returns the same receipt.
This proves idempotency of the local simulated provider. Real payment providers need their own
idempotency and reconciliation contracts.

The host drains generated messages and payment actors before accepting another external event.
These simulated payment bodies finish immediately. This bounded host does not implement arbitrary
long-running async work, distributed delivery, checkpoint compaction, or machine migrations.
Change the machine version and start a new database when changing replay semantics.
Only one process may own a database at a time.

## Run on Rivet

<!-- native workflow host from ../../packages/durable/src/rivet and CLI from src/rivet -->

```sh
pnpm cafe:rivet --open --customers 6 --baristas 2
```

This starts the real Rivet engine locally and registers café and payment-ledger actors.
The same domain machines run through `createDurable`. The Stately inspector shows their complete actor tree.
Each café is a Rivet actor containing a durable workflow; its payment ledger is another Rivet actor.
Customers and equipment are XState actors within that café, rather than separate Rivet actors.

<!-- registration and application service boundary from src/rivet/registry.ts and src/rivet/services.ts -->

The café registers its machine with the reusable `durableRivetActor` factory:

```ts
const cafe = durableRivetActor({
  logic: (context) => openCafeLogic({ ...context, endpoint }),
  inputSchema: cafeInputSchema,
  eventSchema: cafeCommandSchema,
  admission: admitCafeCommand,
  historyVersion: 'rivet-cafe-1',
  terminal: { code: 'cafe_closed', message: 'Café is closed.' },
});
```

Payment-client creation and closing-time admission remain application code in `services.ts`.
The shared host owns all workflow, SQL inbox, receipt, timer, replay, and inspection mechanics.
See the [adapter API and unrelated-machine example](../../packages/durable/README.md).

The engine listens on port 6480; the runner uses 6481. Use `--port 6500` to select another pair.
Storage defaults to `examples/cafe/.cafe/rivet`. The engine remains running when the runner exits.
Restart with the same `--key` and `--storage` to resume. The original input and inspection room are retained.
Use a different key for another café. Commands match the local CLI, including `crash`.
`--no-inspect` disables the hosted Stately relay.

<!-- inspector ownership and replay behavior from ../../packages/durable/src/rivet/actor.ts and ../../packages/durable/src/execution.ts -->

The inspector connection belongs to the running Rivet actor. Workflow queue waits reuse it.
Only newly committed deliveries stream inspection events; replay does not rewind the graph or register
existing actors again. After an actor activation or runner restart, inspection receives the current actor
tree once, then live updates. Closing the café, actor sleep, destruction, and runner shutdown release the connection.
The shared execution driver records checkpoints and fresh turns. The SDK resumes logical actor identities
directly; the adapter no longer rewrites actor references or fabricates hydration transitions.
This change requires the SDK release described in [Durable XState hosts](../../packages/durable/README.md#sdk-release-prerequisite).

```sh
pnpm cafe:rivet --key morning --storage .cafe/morning --port 6500 --open
```

For an existing Rivet engine, `--endpoint http://127.0.0.1:6480` starts only the runner.
This example verifies the native engine on this computer. It does not provision a hosted Rivet deployment.

Rivet workflow steps journal accepted input deliveries and async results. Its queue provides wake notifications.
Commands commit to the actor's SQLite inbox before sending a wake notification, so a crash between those
operations does not lose an accepted command. Repeating a command request ID with the same payload is safe.
The acknowledgement means the command entered the durable inbox. Use `receipt REQUEST_ID` in the CLI
or `cafe.getCommand(requestId)` to read `pending`, `delivered`, or `rejected`.
Delivery does not imply a state change; for example, cancelling an unknown order has no effect.
When the café finishes closing, its final snapshot and rejection of remaining queued commands commit
together. Those receipts report `rejected` with reason `cafe_closed`; retrying their IDs returns that error.
Repairs and cancellations can still enter the inbox while existing orders finish closing.
Timer deadlines derive from the recorded execution clock, so replay preserves their original due times.
The ledger commits charges and refunds using unique order/kind keys before returning receipts.

RivetKit 2.3.31 can return `core/internal_error` while routing expires after a runner crash.
The adapter and CLI use bounded retries at idempotent request boundaries. Exhausted transport errors
remain workflow failures eligible for Rivet retry; business payment failures become journaled actor errors.
The domain machines do not import Rivet.

This demonstration keeps the complete workflow history and replays the actor tree from its start.
It avoids Rivet's compacting loop, which would discard the history needed to rebuild that tree.
Idle cafés poll their inbox at most once per second, adding history entries. Close bounded demonstrations
with `close`; do not treat this as an indefinitely running production service. Async payment work is drained
serially between input deliveries. This adapter does not implement arbitrary overlapping async work or machine migrations.
Use a new café key when changing model semantics; bump the adapter metadata version for incompatible changes.

Cloudflare, Restate, and Temporal adapters remain follow-up work.

## Verify

<!-- verification scripts from package.json -->

```sh
pnpm --dir examples/cafe test
pnpm --dir examples/cafe typecheck
```

The test suite includes real-engine Rivet tests and kills runner processes at charge/refund responses,
command-ingress, and brewing boundaries. It checks one effective charge/refund, a recovered inbox command,
and the original brewing deadline. It also covers terminal command rejection and timer-before-command
ordering at an identical timestamp. Local tests cover automatic breakdown/repair across restarts and a
saved pre-refactor history fixture. Tests use isolated temporary engine storage and stop their own engine processes.
To run just the provider tests, use `pnpm --dir examples/cafe test:rivet`.
The suite runs one test file at a time because its recovery fixtures launch and kill real processes.
