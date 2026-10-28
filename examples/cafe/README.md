# Durable café

<!-- dependencies and engines from package.json -->

A Node café simulation using XState `6.0.0-alpha.70`, SQLite, and `@statelyai/sdk` `0.40.0`.
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

Rivet, Cloudflare, Restate, and Temporal adapters are follow-up work.
This example establishes the domain model and local recovery behavior they must preserve.

## Verify

<!-- verification scripts from package.json -->

```sh
pnpm --dir examples/cafe test
pnpm --dir examples/cafe typecheck
```
