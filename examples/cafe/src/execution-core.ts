import {
  deliverEvent,
  type AnyActorLogic,
  type InspectionEvent,
  type Snapshot,
} from 'xstate';
import {
  createDurable,
  type DurableExecution,
  type DurableExecutionAdapter,
} from 'xstate/durable';
import { recordActor, type ActorView } from './inspection.ts';

type RuntimeHooks = Pick<
  DurableExecutionAdapter<AnyActorLogic>,
  'scheduleTimer' | 'cancelTimer' | 'cancelAllTimers' | 'runLogic'
>;

export class CafeExecution {
  readonly execution: DurableExecution<AnyActorLogic>;
  readonly actors = new Map<string, ActorView>();
  private snapshot: Snapshot<unknown> | undefined;
  private logicalNow: number;
  private readonly rootMailbox: Array<{ type: string }> = [];

  constructor(
    readonly options: {
      logic: AnyActorLogic;
      input: unknown;
      executionId: string;
      startedAt: number;
      runtime: RuntimeHooks;
      drainLogic(execution: DurableExecution<AnyActorLogic>): Promise<void>;
      hasPendingLogic(): boolean;
      yield(): Promise<void>;
      inspect?: (event: InspectionEvent) => void;
    },
  ) {
    this.logicalNow = options.startedAt;
    this.execution = createDurable(
      options.logic,
      {
        ...options.runtime,
        executionId: options.executionId,
        now: () => this.logicalNow,
        executeAction: (action, _metadata, runtime) => action.exec(runtime),
        enqueueRootEvent: (_source, event) => {
          this.rootMailbox.push(event);
        },
        waitForEvent: () => this.rootMailbox.shift() ?? { type: 'HOST.IDLE' },
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

  get now(): number {
    return this.logicalNow;
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
      if (this.options.hasPendingLogic())
        await this.options.drainLogic(this.execution);
      const event = await this.execution.waitForEvent();
      if (event.type === 'HOST.IDLE') {
        // Actor completion messages can follow the async operation's settlement.
        await this.execution.executeEffects([]);
        await this.options.yield();
        if (this.options.hasPendingLogic() || this.rootMailbox.length) continue;
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

  async deliver(delivery: {
    address: string;
    at: number;
    event: { type: string };
  }): Promise<void> {
    if (!this.snapshot) throw new Error('Execution has not started.');
    this.logicalNow = delivery.at;
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
}
