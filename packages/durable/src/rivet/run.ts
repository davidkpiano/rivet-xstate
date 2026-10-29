import { randomUUID } from 'node:crypto';
import { queue, type RunContext } from 'rivetkit';
import { db } from 'rivetkit/db';
import { workflow, type WorkflowStepContext } from 'rivetkit/workflow';
import { createInspector, type Transport } from '@statelyai/sdk/inspect';
import type {
  AnyActorLogic,
  InputFrom,
  EventFromLogic,
  InspectionEvent,
  ActorInspectionEvent,
} from 'xstate';
import { z } from 'zod';
import { serializable } from '../inspection-view.ts';
import { RivetExecution, type Delivery } from './execution.ts';
import { selectDelivery } from './delivery.ts';
import { checkProtocol } from './protocol.ts';
import { snapshotInput } from './input.ts';
import { DurableInspection } from './inspection.ts';

type Queues<K extends string, B> = Record<K, ReturnType<typeof queue<B>>>;
type Step<S, K extends string, B> = WorkflowStepContext<
  S,
  unknown,
  unknown,
  unknown,
  unknown,
  ReturnType<typeof db>,
  {},
  Queues<K, B>
>;
export type DurableActorView = ReturnType<RivetExecution['view']> & {
  inspectorUrl: string | null;
};
export type LogicResource<L extends AnyActorLogic> = {
  logic: L;
  dispose?: () => void | Promise<void>;
};
export type XStateOptions<
  L extends AnyActorLogic,
  S = InputFrom<L>,
  K extends string = string,
  B = EventFromLogic<L>,
> = {
  queue: K;
  historyVersion?: string;
  input: (context: Step<S, K, B>) => InputFrom<L> | Promise<InputFrom<L>>;
  mapEvent?: (body: B) => { event: EventFromLogic<L>; id?: string };
  inspect?:
    | boolean
    | {
        name?: string;
        transport?: () => Transport;
        enabled?: (context: Step<S, K, B>) => boolean;
      };
  onSnapshot?: (
    context: Step<S, K, B>,
    view: DurableActorView,
    delivery: Delivery | null,
  ) => void | Promise<void>;
  beforePublish?: (view: DurableActorView) => void | Promise<void>;
  onDelivery?: (delivery: Delivery) => void;
  now?: () => number;
};

/** A normal Rivet run handler. Actor configuration and RPC actions belong to its caller. */
export function xstate<
  L extends AnyActorLogic,
  S = InputFrom<L>,
  K extends string = string,
  B = EventFromLogic<L>,
>(
  logic:
    | L
    | ((context: {
        executionId: string;
        signal: AbortSignal;
      }) => LogicResource<L> | Promise<LogicResource<L>>),
  options: XStateOptions<L, S, K, B>,
) {
  return async (
    outer: RunContext<
      S,
      unknown,
      unknown,
      unknown,
      unknown,
      ReturnType<typeof db>,
      {},
      Queues<K, B>
    >,
  ) => {
    await checkProtocol(
      outer.db,
      `xstate-rivet-run-3:${options.historyVersion ?? '1'}`,
    );
    const completeNative = outer.queue.complete as (
      message: { id: bigint; name: K },
      response: undefined,
    ) => Promise<void>;
    let inspection: DurableInspection | undefined;
    const now = options.now ?? Date.now;
    const run = workflow<
      S,
      unknown,
      unknown,
      unknown,
      unknown,
      ReturnType<typeof db>,
      {},
      Queues<K, B>
    >(async (context) => {
      const metadata = await context.step('xstate:metadata', async (step) => ({
        input: snapshotInput(await options.input(step)),
        startedAt: now(),
        executionId: outer.actorId,
        roomId: randomUUID(),
        producerId: randomUUID(),
        inspect:
          typeof options.inspect === 'object'
            ? (options.inspect.enabled?.(step) ?? true)
            : !!options.inspect,
      }));
      if (metadata.inspect && !inspection) {
        const config =
          typeof options.inspect === 'object' ? options.inspect : {};
        const transport = config.transport?.();
        const inspector = createInspector({
          name: config.name ?? 'Rivet XState actor',
          transport,
          roomId: metadata.roomId,
          producerId: metadata.producerId,
          launch: 'none',
          bidirectional: false,
          serializeSnapshot: (snapshot) =>
            serializable({
              value: snapshot.value,
              status: snapshot.status,
              context: snapshot.context,
              tags: snapshot.tags ? [...snapshot.tags] : [],
            }),
        });
        void inspector.ready.catch(() => {});
        inspection = new DurableInspection(inspector, transport);
      }
      const resource =
        typeof logic === 'function'
          ? await logic({
              executionId: metadata.executionId,
              signal: outer.abortSignal,
            })
          : { logic };
      const events: InspectionEvent[] = [];
      const actors = new Map<
        InspectionEvent['actorRef']['sessionId'],
        ActorInspectionEvent
      >();
      const execution = new RivetExecution({
        ...metadata,
        logic: resource.logic,
        signal: outer.abortSignal,
        journal: (key, run) => context.step(key, async () => run()),
        inspect: (event) => {
          events.push(event);
          const actor = actors.get(event.actorRef.sessionId);
          if (event.type === '@xstate.actor')
            actors.set(event.actorRef.sessionId, event);
          else if (actor)
            actors.set(event.actorRef.sessionId, {
              ...actor,
              snapshot: event.snapshot,
            });
        },
      });
      const view = (): DurableActorView => ({
        ...execution.view(),
        inspectorUrl: inspection?.inspectorUrl ?? null,
      });
      const publish = async (key: string, delivery: Delivery | null) => {
        await options.beforePublish?.(view());
        await context.step(key, async (step) => {
          await options.onSnapshot?.(step, view(), delivery);
          inspection?.publish(events);
        });
      };
      const delivered = new Set<string>();
      let pending: Delivery | undefined;
      let complete: (() => Promise<void>) | undefined;
      try {
        await execution.start();
        await publish('xstate:initial', null);
        for (let turn = 0; view().status === 'active'; turn++) {
          // This step executes only at the live frontier, after historical replay.
          await context.step(`xstate:hydrate:${turn}`, async () =>
            inspection?.hydrate(actors.values()),
          );
          if (!pending) {
            const timer = execution.nextTimer;
            const batch = await context.queue.nextBatch(
              `xstate:queue:${turn}`,
              {
                names: [options.queue],
                count: 1,
                completable: true,
                timeout: timer
                  ? Math.min(1000, Math.max(1, timer.at - now()))
                  : 1000,
              },
            );
            const message = batch[0];
            let received: {
              id: string;
              body: unknown;
              createdAt: number;
              finishWorkflow?: () => Promise<void>;
            } | null = null;
            if (message) {
              const workflowMessage: { complete(): Promise<void> } = message;
              received = {
                id: String(message.id),
                body: message.body,
                createdAt: message.createdAt,
                finishWorkflow: () => workflowMessage.complete(),
              };
            } else {
              // Rivet workflow deadlines can expire before checking an already populated queue.
              received = await context.step(`xstate:peek:${turn}`, async () => {
                const available = await outer.queue.tryNext({
                  names: [options.queue],
                  completable: true,
                });
                return available
                  ? z
                      .object({
                        id: z
                          .union([z.number().int(), z.bigint()])
                          .transform(String),
                        body: z.unknown(),
                        createdAt: z.number(),
                      })
                      .parse(available)
                  : null;
              });
            }
            if (received) {
              const completion = received;
              complete = async () => {
                await context.step(`xstate:ack:${turn}`, async () =>
                  completeNative.call(
                    outer.queue,
                    { id: BigInt(completion.id), name: options.queue },
                    undefined,
                  ),
                );
                await completion.finishWorkflow?.();
              };
              const mapped = options.mapEvent
                ? options.mapEvent(received.body as B)
                : {
                    event: z
                      .object({ type: z.string() })
                      .passthrough()
                      .parse(received.body),
                  };
              pending = {
                address: execution.execution.rootAddress,
                event: mapped.event,
                commandId: 'id' in mapped ? (mapped.id ?? null) : null,
                acceptedAt: received.createdAt,
                at: Math.max(execution.now, received.createdAt),
              };
            }
          }

          const timer = execution.nextTimer;
          const selection = await context.step(
            `xstate:select:${turn}`,
            async () => selectDelivery(timer, pending, now()),
          );
          const delivery =
            selection === 'timer'
              ? timer
              : selection === 'message'
                ? pending
                : undefined;
          if (!delivery) continue;
          const consumed = selection === 'message';
          if (consumed) pending = undefined;
          if (delivery.commandId && delivered.has(delivery.commandId)) {
            await complete?.();
            complete = undefined;
            continue;
          }
          events.length = 0;
          options.onDelivery?.(delivery);
          await execution.deliver(delivery);
          if (delivery.commandId) delivered.add(delivery.commandId);
          await publish(`xstate:publish:${turn}`, delivery);
          if (consumed) {
            await complete?.();
            complete = undefined;
          }
        }
        await context.step('xstate:complete', async () =>
          inspection?.hydrate(actors.values()),
        );
        await complete?.();
      } finally {
        await resource.dispose?.();
      }
    });
    const destroy = () => {
      inspection?.destroy();
      inspection = undefined;
    };
    outer.abortSignal.addEventListener('abort', destroy, { once: true });
    try {
      await run(outer);
    } finally {
      outer.abortSignal.removeEventListener('abort', destroy);
      destroy();
    }
  };
}
