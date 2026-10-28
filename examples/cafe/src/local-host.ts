import { randomUUID } from 'node:crypto';
import { setImmediate } from 'node:timers/promises';
import { type AnyActorLogic, type InspectionEvent } from 'xstate';
import { CafeExecution } from './execution-core.ts';
import { CafeStore, type Delivery } from './store.ts';

import { serializable } from './inspection.ts';

/** One bounded execution. Every nondeterministic delivery is durable before application. */
export class LocalDurableHost {
  private readonly core: CafeExecution;
  private turn = 0;
  private operation = 0;
  private timers = new Map<string, { address: string; key: string }>();
  private pendingLogic = new Set<Promise<unknown>>();
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
    const startedAt = stored?.startedAt ?? this.now();
    this.core = new CafeExecution({
      logic: options.logic,
      input: options.input,
      executionId,
      startedAt,
      runtime: {
        scheduleTimer: (source, timer, delay) => {
          const identity = `${source.address}:${JSON.stringify(timer)}`;
          const key = `${this.turn}:${this.operation++}:${identity}`;
          this.timers.set(identity, { address: source.address, key });
          options.store.schedule(
            key,
            source.address,
            timer,
            this.core.now + delay,
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
      },
      drainLogic: async (execution) => {
        if (this.pendingLogic.size) {
          await Promise.allSettled([...this.pendingLogic]);
          await execution.executeEffects([]);
        }
      },
      hasPendingLogic: () => this.pendingLogic.size > 0,
      yield: async () => {
        await setImmediate();
      },
      inspect: options.inspect,
    });
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
        startedAt,
        roomId: randomUUID(),
        producerId: randomUUID(),
      });
    }
  }

  private now(): number {
    return this.options.now?.() ?? Date.now();
  }

  get execution() {
    return this.core.execution;
  }

  get actors() {
    return this.core.actors;
  }

  async start(): Promise<void> {
    await this.core.start();
    for (const delivery of this.options.store.history())
      await this.apply(delivery);
    this.options.onTurn?.(this);
  }

  private async apply(delivery: Delivery): Promise<void> {
    this.turn = delivery.seq;
    this.operation = 0;
    if (delivery.event.type === 'xstate.timer') {
      const { type: _type, ...timer } = delivery.event;
      this.timers.delete(`${delivery.address}:${JSON.stringify(timer)}`);
    }
    await this.core.deliver(delivery);
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
