import {
  deliverEvent,
  type AnyActorLogic,
  type InspectionEvent,
  type Snapshot,
} from 'xstate';
import { createDurable, type DurableExecution } from 'xstate/durable';
import { recordActor, type ActorView } from '../inspection.ts';
import { isTransportFailure } from './requests.ts';

export type Delivery = {
  address: string;
  at: number;
  event: { type: string };
  commandId: string | null;
};
type LogicJob = {
  key: string;
  exec: () => PromiseLike<unknown>;
  resolve(value: unknown): void;
  reject(error: unknown): void;
};
export type LogicResult =
  | { status: 'done'; value: unknown }
  | { status: 'error'; name: string; message: string };

export class RivetExecution {
  readonly execution: DurableExecution<AnyActorLogic>;
  readonly actors = new Map<string, ActorView>();
  private snapshot: Snapshot<unknown> | undefined;
  private time: number;
  private turn = 0;
  private operation = 0;
  private readonly timers = new Map<string, Delivery>();
  private readonly jobs: LogicJob[] = [];
  private readonly rootMailbox: Array<{ type: string }> = [];

  constructor(
    readonly options: {
      logic: AnyActorLogic;
      input: unknown;
      executionId: string;
      startedAt: number;
      journal(
        key: string,
        exec: () => Promise<LogicResult>,
      ): Promise<LogicResult>;
      inspect?: (event: InspectionEvent) => void;
      signal?: AbortSignal;
    },
  ) {
    this.time = options.startedAt;
    this.execution = createDurable(
      options.logic,
      {
        executionId: options.executionId,
        now: () => this.time,
        executeAction: (action, _metadata, runtime) => action.exec(runtime),
        enqueueRootEvent: (_source, event) => {
          this.rootMailbox.push(event);
        },
        waitForEvent: () => this.rootMailbox.shift() ?? { type: 'HOST.IDLE' },
        scheduleTimer: (source, timer, delay) => {
          const key = `${source.address}:${JSON.stringify(timer)}`;
          this.timers.set(key, {
            address: source.address,
            at: this.time + delay,
            event: { type: 'xstate.timer', ...timer },
            commandId: null,
          });
        },
        cancelTimer: (source, timer) => {
          this.timers.delete(`${source.address}:${JSON.stringify(timer)}`);
        },
        cancelAllTimers: (source) => {
          for (const [key, timer] of this.timers)
            if (timer.address === source.address) this.timers.delete(key);
        },
        runLogic: (actor, exec) =>
          new Promise((resolve, reject) => {
            this.jobs.push({
              key: `logic:${this.turn}:${this.operation++}:${actor.address}`,
              exec,
              resolve,
              reject,
            });
          }),
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
  }

  async start(): Promise<void> {
    const [snapshot, effects] = this.execution.initialTransition(
      this.options.input,
    );
    this.snapshot = snapshot;
    await this.execution.executeEffects(effects);
    await this.drain();
  }

  private async drain(): Promise<void> {
    let idle = 0;
    for (;;) {
      while (this.jobs.length) {
        const job = this.jobs.shift();
        if (!job) break;
        const result = await this.options.journal(job.key, async () => {
          try {
            return { status: 'done', value: await job.exec() };
          } catch (error) {
            this.options.signal?.throwIfAborted();
            if (isTransportFailure(error)) throw error;
            return {
              status: 'error',
              name: error instanceof Error ? error.name : 'Error',
              message: error instanceof Error ? error.message : String(error),
            };
          }
        });
        if (result.status === 'done') job.resolve(result.value);
        else
          job.reject(
            Object.assign(new Error(result.message), { name: result.name }),
          );
        await this.execution.executeEffects([]);
      }
      const event = await this.execution.waitForEvent();
      if (event.type === 'HOST.IDLE') {
        await this.execution.executeEffects([]);
        await new Promise<void>((resolve) => setTimeout(resolve, 0));
        if (this.jobs.length || this.rootMailbox.length) continue;
        if (++idle >= 2) break;
        continue;
      }
      idle = 0;
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

  async deliver(delivery: Delivery): Promise<void> {
    if (!this.snapshot) throw new Error('Execution has not started.');
    this.time = delivery.at;
    this.turn++;
    this.operation = 0;
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
      const actor = this.execution.getActorRef(this.snapshot, delivery.address);
      if (actor)
        deliverEvent(
          delivery.event.type === 'xstate.timer' ? actor : undefined,
          actor,
          delivery.event,
        );
      await this.execution.executeEffects([]);
    }
    await this.drain();
  }

  get nextTimer(): Delivery | undefined {
    return [...this.timers.values()].sort((a, b) => a.at - b.at)[0];
  }

  view() {
    return {
      actors: [...this.actors.values()],
      timers: [...this.timers.values()],
      events: this.turn,
    };
  }
}
