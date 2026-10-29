# XState v6 alpha × RivetKit

<!-- durable cafe example entrypoint and scripts from examples/cafe/package.json -->

For the newer multi-actor durable café demo, see [examples/cafe](examples/cafe/README.md).
It runs in Node with SQLite recovery or native Rivet workflows, and the Stately inspector.
Run `pnpm --dir examples/cafe install`, then `pnpm cafe --open` or `pnpm cafe:rivet --open`.

<!-- reusable durable host package from packages/durable/package.json -->

The café uses the reusable [Rivet durable adapter and actor host](packages/durable/README.md).
Its registration supplies the machine and schemas; the adapter handles persistence, timers, replay,
command receipts, and Stately inspection. A document-approval machine tests the same public API independently.

A traffic-light state machine running inside a Rivet Actor. XState supplies pure statechart logic; Rivet is the runtime.

<!-- runtime boundary implemented by src/server/xstateActor.ts -->

```text
XState: initialTransition() / transition() → [snapshot, actions]
Rivet:  persisted state + event delivery + action execution + durable schedules
```

There is no `createActor()`. `createXStateActor()` accepts an XState machine, persists its snapshot in `c.state`, and executes every returned action through Rivet. Delayed `after:` transitions become `c.schedule.after(...)` calls; cancellation becomes `c.schedule.cancel(...)`.

## Run

<!-- scripts and local URLs derived from package.json and vite.config.ts -->

```sh
pnpm install
pnpm dev
```

Open <http://localhost:5173>. Rivet's local inspector is at <http://localhost:6420>.

## Demo

<!-- machine states and events from src/machine/trafficLight.ts -->

The traffic machine cycles through `green → yellow → red.walk → red.flashing → red.stop → green`. Every phase has an `after:` transition. Buttons expose only the domain events currently available (`NEXT`, `POWER_OUTAGE`, `RESTORE`, `RESET`). The React view shows the current nested state value, context, and valid next events.

## Structure

<!-- primary implementation files in src -->

- `src/machine/trafficLight.ts` — standalone XState v6 alpha machine.
- `src/server/xstateActor.ts` — generic pure-transition adapter.
- `src/server/registry.ts` — Rivet actor registration and runtime action executor.
- `src/App.tsx` — realtime React client using `@rivetkit/react`.

## Verify

<!-- verification scripts from package.json -->

```sh
pnpm test
pnpm typecheck
pnpm build
```
