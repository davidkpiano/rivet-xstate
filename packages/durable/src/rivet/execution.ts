import { type AnyActorLogic, type InspectionEvent } from 'xstate';
import { DurableActorExecution } from '../execution.ts';
import { isTransportFailure } from './requests.ts';

export type Delivery = {
  address: string;
  at: number;
  event: { type: string };
  commandId: string | null;
  acceptedAt?: number;
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
  private readonly core: DurableActorExecution;
  private turn = 0;
  private operation = 0;
  private readonly timers = new Map<string, Delivery>();
  private readonly jobs: LogicJob[] = [];

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
    this.core = new DurableActorExecution({
      logic: options.logic,
      input: options.input,
      executionId: options.executionId,
      startedAt: options.startedAt,
      runtime: {
        scheduleTimer: (source, timer, delay) => {
          const key = `${source.address}:${JSON.stringify(timer)}`;
          this.timers.set(key, {
            address: source.address,
            at: this.core.now + delay,
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
      },
      drainLogic: async (execution) => {
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
          await execution.executeEffects([]);
        }
      },
      hasPendingLogic: () => this.jobs.length > 0,
      yield: () => new Promise<void>((resolve) => setTimeout(resolve, 0)),
      inspect: options.inspect,
    });
  }

  get now() {
    return this.core.now;
  }

  get execution() {
    return this.core.execution;
  }

  get actors() {
    return this.core.actors;
  }

  start(): Promise<void> {
    return this.core.start();
  }

  async deliver(delivery: Delivery): Promise<void> {
    this.turn++;
    this.operation = 0;
    if (delivery.event.type === 'xstate.timer') {
      const { type: _type, ...timer } = delivery.event;
      this.timers.delete(`${delivery.address}:${JSON.stringify(timer)}`);
    }
    await this.core.deliver(delivery);
  }

  get nextTimer(): Delivery | undefined {
    return [...this.timers.values()].sort((a, b) => a.at - b.at)[0];
  }

  view() {
    return {
      actors: [...this.actors.values()],
      timers: [...this.timers.values()],
      events: this.turn,
      status: this.core.status,
    };
  }
}
