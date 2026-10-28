import { randomUUID } from 'node:crypto';
import { setImmediate } from 'node:timers/promises';
import {
  deliverEvent,
  type AnyActorLogic,
  type InspectionEvent,
  type Snapshot,
} from 'xstate';
import { createDurable, type DurableExecution } from 'xstate/durable';
import { CafeStore, type Delivery } from './store.ts';

import { serializable, recordActor, type ActorView } from './inspection.ts';

/** One bounded execution. Every nondeterministic delivery is durable before application. */
export class LocalDurableHost {
  readonly execution: DurableExecution<AnyActorLogic>;
  readonly actors = new Map<string, ActorView>();
  private snapshot: Snapshot<unknown> | undefined;
  private logicalNow: number;
  private turn = 0;
  private operation = 0;
  private timers = new Map<string, { address: string; key: string }>();
  private pendingLogic = new Set<Promise<unknown>>();
  private rootMailbox: Array<{ type: string }> = [];
  private serial = Promise.resolve();
  private failure: unknown;
  private closed = false;

  constructor(
    readonly options: {
      logic: AnyActorLogic;
      input: unknown;
      store: CafeStore;
      inspect?: (event: InspectionEvent) => void;
      now?: () => number;
      onTurn?: (host: LocalDurableHost) => void;
    },
  ) {
    const stored = options.store.run;
    const executionId = stored?.executionId ?? randomUUID();
    this.logicalNow = stored?.startedAt ?? this.now();
    this.execution = createDurable(
      options.logic,
      {
        executionId,
        now: () => this.logicalNow,
        executeAction: (action, _metadata, runtime) => action.exec(runtime),
        enqueueRootEvent: (_source, event) => {
          this.rootMailbox.push(event);
        },
        waitForEvent: () => this.rootMailbox.shift() ?? { type: 'HOST.IDLE' },
        scheduleTimer: (source, timer, delay) => {
          const identity = `${source.address}:${JSON.stringify(timer)}`;
          const key = `${this.turn}:${this.operation++}:${identity}`;
          this.timers.set(identity, { address: source.address, key });
          options.store.schedule(
            key,
            source.address,
            timer,
            this.logicalNow + delay,
          );
        },
        cancelTimer: (source, timer) => {
          const identity = `${source.address}:${JSON.stringify(timer)}`;
          const scheduled = this.timers.get(identity);
          if (scheduled) options.store.cancel(scheduled.key);
          this.timers.delete(identity);
        },
        cancelAllTimers: (source) => {
          for (const [identity, scheduled] of this.timers) {
            if (scheduled.address === source.address) {
              options.store.cancel(scheduled.key);
              this.timers.delete(identity);
            }
          }
        },
        runLogic: (actor, exec) => {
          const key = `${this.turn}:${this.operation++}:${actor.address}`;
          const input = JSON.stringify(serializable(actor.getSnapshot()));
          const memoized = options.store.result(key, input);
          const pending = memoized
            ? memoized.status === 'done'
              ? Promise.resolve(memoized.value)
              : Promise.reject(
                  Object.assign(new Error(memoized.error.message), {
                    name: memoized.error.name,
                  }),
                )
            : Promise.resolve()
                .then(exec)
                .then(
                  (output) => {
                    options.store.saveResult(key, input, {
                      status: 'done',
                      value: output,
                    });
                    return output;
                  },
                  (error: unknown) => {
                    options.store.saveResult(key, input, {
                      status: 'error',
                      error:
                        error instanceof Error
                          ? { name: error.name, message: error.message }
                          : { name: 'Error', message: String(error) },
                    });
                    throw error;
                  },
                );
          this.pendingLogic.add(pending);
          void pending.then(
            () => {
              this.pendingLogic.delete(pending);
            },
            () => {
              this.pendingLogic.delete(pending);
            },
          );
          return pending;
        },
        deadLetter: (_source, target, event, reason) => {
          if (reason !== 'stopped')
            throw new Error(
              `Rejected ${event.type} for ${target?.address}: ${reason}`,
            );
        },
      },
      {
        inspect: (event) => {
          recordActor(this.actors, event);
          options.inspect?.(event);
        },
      },
    );
    const version = this.execution.machineVersion;
    if (!version) throw new Error('A durable machine must declare a version.');
    if (stored) {
      if (
        stored.machineId !== this.execution.machineId ||
        stored.machineVersion !== version
      ) {
        throw new Error(
          'Machine version differs from this database. Start a new database or migrate its history.',
        );
      }
      if (JSON.stringify(stored.input) !== JSON.stringify(options.input))
        throw new Error('Café input differs from the saved run.');
    } else {
      options.store.setMetadata('run', {
        executionId,
        machineId: this.execution.machineId,
        machineVersion: version,
        input: options.input,
        startedAt: this.logicalNow,
        roomId: randomUUID(),
        producerId: randomUUID(),
      });
    }
  }

  private now(): number {
    return this.options.now?.() ?? Date.now();
  }

  async start(): Promise<void> {
    const [snapshot, effects] = this.execution.initialTransition(
      this.options.input,
    );
    this.snapshot = snapshot;
    await this.execution.executeEffects(effects);
    await this.drain();
    for (const delivery of this.options.store.history())
      await this.apply(delivery);
    this.options.onTurn?.(this);
  }

  private async drain(): Promise<void> {
    let idlePasses = 0;
    for (;;) {
      if (this.pendingLogic.size) {
        await Promise.allSettled([...this.pendingLogic]);
        await this.execution.executeEffects([]);
      }
      const event = await this.execution.waitForEvent();
      if (event.type === 'HOST.IDLE') {
        // Async completion handlers publish through the actor mailbox after their body settles.
        await this.execution.executeEffects([]);
        await setImmediate();
        if (this.pendingLogic.size || this.rootMailbox.length) continue;
        if (++idlePasses >= 2) break;
        continue;
      }
      idlePasses = 0;
      if (!this.snapshot) throw new Error('Execution has not started.');
      const [snapshot, effects] = this.execution.transition(
        this.snapshot,
        event,
      );
      this.snapshot = snapshot;
      await this.execution.executeEffects(effects);
    }
    if (this.snapshot?.status === 'error') throw this.snapshot.error;
  }

  private async apply(delivery: Delivery): Promise<void> {
    if (!this.snapshot) throw new Error('Execution has not started.');
    this.turn = delivery.seq;
    this.operation = 0;
    this.logicalNow = delivery.at;
    if (delivery.event.type === 'xstate.timer') {
      const { type: _type, ...timer } = delivery.event;
      this.timers.delete(`${delivery.address}:${JSON.stringify(timer)}`);
    }
    if (delivery.address === this.execution.rootAddress) {
      const [snapshot, effects] = this.execution.transition(
        this.snapshot,
        delivery.event,
      );
      this.snapshot = snapshot;
      await this.execution.executeEffects(effects);
    } else {
      const target = this.execution.getActorRef(
        this.snapshot,
        delivery.address,
      );
      if (target)
        deliverEvent(
          delivery.event.type === 'xstate.timer' ? target : undefined,
          target,
          delivery.event,
        );
      await this.execution.executeEffects([]);
    }
    await this.drain();
    this.options.onTurn?.(this);
  }

  private serialized<T>(body: () => Promise<T>): Promise<T> {
    const next = this.serial.then(async () => {
      if (this.closed) throw new Error('Host is closed.');
      if (this.failure) throw this.failure;
      try {
        return await body();
      } catch (error) {
        this.failure = error;
        throw error;
      }
    });
    this.serial = next.then(
      () => {},
      () => {},
    );
    return next;
  }

  send(event: unknown): Promise<void> {
    return this.serialized(async () => {
      const acceptedAt = this.now();
      await this.tickDue(acceptedAt);
      const delivery = this.options.store.append(
        this.execution.rootAddress,
        event,
        acceptedAt,
      );
      await this.apply(delivery);
    });
  }

  tick(now = this.now()): Promise<void> {
    return this.serialized(() => this.tickDue(now));
  }

  private async tickDue(now: number): Promise<void> {
    for (;;) {
      const timer = this.options.store.nextTimer(now);
      if (!timer) break;
      await this.apply(timer);
    }
  }

  view() {
    return {
      actors: [...this.actors.values()],
      payments: this.options.store.payments(),
      timers: this.options.store.timers(),
      events: this.turn,
    };
  }

  async close(): Promise<void> {
    await this.serial;
    this.closed = true;
    this.options.store.close();
  }
}
