# XState v6 alpha × RivetKit

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
