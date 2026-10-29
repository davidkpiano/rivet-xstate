import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { actor, queue, UserError } from 'rivetkit';
import { db } from 'rivetkit/db';
import { workflow } from 'rivetkit/workflow';
import { createInspector, type Transport } from '@statelyai/sdk/inspect';
import type {
  ActorInspectionEvent,
  InspectionEvent,
  AnyActorLogic,
  InputFrom,
  EventFromLogic,
} from 'xstate';
import { DurableInspection } from './inspection.ts';
import { z } from 'zod';
import { serializable } from '../inspection-view.ts';
import { RivetExecution, type Delivery } from './execution.ts';

const commandRowSchema = z.object({
  id: z.string(),
  event: z.string(),
  accepted_at: z.number(),
});
const requestIdSchema = z.string().min(1).max(200);
const receiptRowSchema = commandRowSchema.extend({
  processed: z.union([z.literal(0), z.literal(1), z.literal(2)]),
});
const receiptBase = z.object({ id: z.string(), acceptedAt: z.number() });
export const commandReceiptSchema = z.discriminatedUnion('status', [
  receiptBase.extend({ status: z.literal('pending') }),
  receiptBase.extend({ status: z.literal('delivered') }),
  receiptBase.extend({
    status: z.literal('rejected'),
    reason: z.string(),
  }),
]);
export const viewSchema = z.object({
  actors: z.array(
    z.object({
      id: z.string(),
      address: z.string(),
      status: z.string(),
      value: z.unknown().optional(),
      context: z.unknown().optional(),
    }),
  ),
  timers: z.array(
    z.object({
      address: z.string(),
      at: z.number(),
      event: z.object({ type: z.string() }).passthrough(),
      commandId: z.string().nullable(),
    }),
  ),
  events: z.number(),
  inspectorUrl: z.string().nullable(),
  status: z.string(),
});
export type DurableActorView = z.infer<typeof viewSchema>;

export type LogicResource<L extends AnyActorLogic> = {
  logic: L;
  dispose?: () => void | Promise<void>;
};
export type DurableRivetActorOptions<
  L extends AnyActorLogic,
  S extends z.ZodType<EventFromLogic<L>> = z.ZodType<EventFromLogic<L>>,
> = {
  logic:
    | L
    | ((context: {
        executionId: string;
        signal: AbortSignal;
      }) => LogicResource<L> | Promise<LogicResource<L>>);
  inputSchema: z.ZodType<NoInfer<InputFrom<L>>>;
  eventSchema: S;
  historyVersion?: string;
  name?: string;
  icon?: string;
  inspection?: { name?: string; transport?: () => Transport };
  admission?: (context: {
    event: z.output<S>;
    commands: { has(event: z.output<S>): Promise<boolean> };
  }) => void | Promise<void>;
  terminal?: { code: string; message: string; rejectedMessage?: string };
  afterCommandCommitted?: (commandId: string) => void;
  onView?: (view: DurableActorView) => void;
  onDelivery?: (delivery: Delivery) => void;
  beforePublish?: (view: DurableActorView) => Promise<void>;
  now?: () => number;
};

export function durableRivetActor<
  L extends AnyActorLogic,
  S extends z.ZodType<EventFromLogic<L>>,
>(options: DurableRivetActorOptions<L, S>) {
  const now = options.now ?? Date.now;
  const terminal = options.terminal ?? {
    code: 'execution_closed',
    message: 'Execution is closed.',
  };
  const startSchema = z.object({
    config: options.inputSchema,
    inspect: z.boolean().default(true),
  });
  const metadataSchema = z.object({
    version: z.literal(options.historyVersion ?? 'rivet-xstate-1'),
    executionId: z.string(),
    input: z.unknown(),
    inspect: z.boolean(),
    startedAt: z.number(),
    roomId: z.string(),
    producerId: z.string(),
  });
  type Metadata = Omit<z.infer<typeof metadataSchema>, 'input'> & {
    input: InputFrom<L>;
  };
  return actor({
    options: {
      name: options.name ?? 'Durable XState actor',
      ...(options.icon ? { icon: options.icon } : {}),
    },
    createState: (
      _context,
      input: { config: InputFrom<L>; inspect?: boolean },
    ): Metadata => {
      const parsed = startSchema.parse(input);
      return {
        version: options.historyVersion ?? 'rivet-xstate-1',
        executionId: randomUUID(),
        input: parsed.config,
        inspect: parsed.inspect,
        startedAt: now(),
        roomId: randomUUID(),
        producerId: randomUUID(),
      };
    },
    createVars: (context) => {
      const metadata = context.state;
      metadataSchema.parse(metadata);
      const transport = metadata.inspect
        ? options.inspection?.transport?.()
        : undefined;
      const inspector = metadata.inspect
        ? createInspector({
            name:
              options.inspection?.name ?? options.name ?? 'Rivet durable actor',
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
          })
        : undefined;
      void inspector?.ready.catch(() => {});
      return {
        inspection: inspector
          ? new DurableInspection(inspector, transport)
          : undefined,
      };
    },
    onSleep: (context) => context.vars.inspection?.destroy(),
    onDestroy: (context) => context.vars.inspection?.destroy(),
    db: db({
      onMigrate: async (database) => {
        await database.execute(
          'CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, event TEXT NOT NULL, accepted_at INTEGER NOT NULL, processed INTEGER NOT NULL DEFAULT 0)',
        );
        await database.execute(
          'CREATE TABLE IF NOT EXISTS published (id INTEGER PRIMARY KEY CHECK (id=1), view TEXT NOT NULL)',
        );
      },
    }),
    queues: { wake: queue<{}>() },
    run: workflow(async (context) => {
      const metadata = await context.step(
        'metadata',
        async (step) => step.state,
      );
      metadataSchema.parse(metadata);
      const inspectorUrl = await context.step(
        'inspector-url',
        async (step) => step.vars.inspection?.inspectorUrl ?? null,
      );
      const resource =
        typeof options.logic === 'function'
          ? await options.logic({
              executionId: metadata.executionId,
              signal: context.abortSignal,
            })
          : { logic: options.logic };
      const inspectionEvents: InspectionEvent[] = [];
      const inspectionActors = new Map<
        InspectionEvent['actorRef']['sessionId'],
        ActorInspectionEvent
      >();
      const execution = new RivetExecution({
        logic: resource.logic,
        input: metadata.input,
        executionId: metadata.executionId,
        startedAt: metadata.startedAt,
        journal: (key, run) => context.step(key, async () => run()),
        inspect: (event) => {
          inspectionEvents.push(event);
          const registration = inspectionActors.get(event.actorRef.sessionId);
          if (event.type === '@xstate.actor')
            inspectionActors.set(event.actorRef.sessionId, event);
          else if (registration)
            inspectionActors.set(event.actorRef.sessionId, {
              ...registration,
              snapshot: event.snapshot,
            });
        },
        signal: context.abortSignal,
      });
      const view = (): DurableActorView =>
        viewSchema.parse({
          ...execution.view(),
          inspectorUrl,
        });
      try {
        await execution.start();
        await options.beforePublish?.(view());
        await context.step('initial-view', async (step) => {
          await step.db.transaction(
            async (transaction) => {
              await transaction.execute(
                'INSERT OR REPLACE INTO published VALUES (1, ?)',
                JSON.stringify(view()),
              );
              if (view().status !== 'active')
                await transaction.execute(
                  'UPDATE commands SET processed=2 WHERE processed=0',
                );
            },
            { name: 'publish' },
          );
          step.vars.inspection?.publish(inspectionEvents);
          options.onView?.(view());
        });
        // A plain loop replays the full actor tree. Rivet's compacting loop would skip earlier turns.
        for (let turn = 0; view().status === 'active'; turn++) {
          const delivery: Delivery | null = await context.step(
            `input:${turn}`,
            async (step) => {
              step.vars.inspection?.hydrate(inspectionActors.values());
              const rows = await step.db.execute(
                'SELECT id, event, accepted_at FROM commands WHERE processed=0 ORDER BY accepted_at, rowid LIMIT 1',
              );
              const command = rows[0]
                ? commandRowSchema.parse(rows[0])
                : undefined;
              const timer = execution.nextTimer;
              const sampledAt = now();
              if (
                timer &&
                timer.at <= sampledAt &&
                (!command || timer.at <= command.accepted_at)
              )
                return timer;
              if (command)
                return {
                  address: execution.execution.rootAddress,
                  event: z
                    .object({ type: z.string() })
                    .passthrough()
                    .parse(JSON.parse(command.event)),
                  at: command.accepted_at,
                  commandId: command.id,
                };
              return null;
            },
          );
          inspectionEvents.length = 0;
          if (delivery) {
            options.onDelivery?.(delivery);
            await execution.deliver(delivery);
            await options.beforePublish?.(view());
            await context.step(`publish:${turn}`, async (step) => {
              await step.db.transaction(
                async (transaction) => {
                  await transaction.execute(
                    'INSERT OR REPLACE INTO published VALUES (1, ?)',
                    JSON.stringify(view()),
                  );
                  if (delivery.commandId)
                    await transaction.execute(
                      'UPDATE commands SET processed=1 WHERE id=?',
                      delivery.commandId,
                    );
                  if (view().status !== 'active')
                    await transaction.execute(
                      'UPDATE commands SET processed=2 WHERE processed=0',
                    );
                },
                { name: 'publish' },
              );
              step.vars.inspection?.publish(inspectionEvents);
              options.onView?.(view());
            });
          } else {
            const timer = execution.nextTimer;
            await context.queue.nextBatch(`wake:${turn}`, {
              names: ['wake'],
              count: 32,
              timeout: timer
                ? Math.min(1000, Math.max(1, timer.at - now()))
                : 1000,
            });
          }
        }
        await context.step('inspection-complete', async (step) => {
          step.vars.inspection?.destroy();
        });
      } finally {
        await resource.dispose?.();
      }
    }),
    actions: {
      dispatch: async (context, raw: z.input<S>, requestId = randomUUID()) => {
        const event = z.parse(options.eventSchema, raw);
        const id = requestIdSchema.parse(requestId);
        const payload = JSON.stringify(event);
        const acceptedAt = await context.db.transaction(
          async (transaction) => {
            const rows = await transaction.execute(
              'SELECT id, event, accepted_at, processed FROM commands WHERE id=?',
              id,
            );
            const existing = rows[0]
              ? receiptRowSchema.parse(rows[0])
              : undefined;
            if (existing) {
              if (existing.event !== payload)
                throw new UserError(
                  'Request ID already belongs to another command.',
                  { code: 'conflicting_command' },
                );
              if (existing.processed === 2)
                throw new UserError(
                  terminal.rejectedMessage ?? terminal.message,
                  { code: terminal.code },
                );
              return existing.accepted_at;
            }
            await options.admission?.({
              event,
              commands: {
                has: async (acceptedEvent) => {
                  const normalized = JSON.parse(JSON.stringify(acceptedEvent));
                  const rows = await transaction.execute(
                    'SELECT event FROM commands',
                  );
                  return z
                    .array(z.object({ event: z.string() }))
                    .parse(rows)
                    .some((row) =>
                      isDeepStrictEqual(JSON.parse(row.event), normalized),
                    );
                },
              },
            });
            const published = await transaction.execute(
              'SELECT view FROM published WHERE id=1',
            );
            if (published[0]) {
              const row = z.object({ view: z.string() }).parse(published[0]);
              if (viewSchema.parse(JSON.parse(row.view)).status !== 'active')
                throw new UserError(terminal.message, { code: terminal.code });
            }
            const at = now();
            await transaction.execute(
              'INSERT INTO commands (id,event,accepted_at) VALUES (?,?,?)',
              id,
              payload,
              at,
            );
            return at;
          },
          { name: 'accept-command' },
        );
        options.afterCommandCommitted?.(id);
        await context.queue.send('wake', {});
        return { id, acceptedAt };
      },
      getSnapshot: async (context): Promise<DurableActorView | null> => {
        const rows = await context.db.execute(
          'SELECT view FROM published WHERE id=1',
        );
        return rows[0]
          ? viewSchema.parse(
              JSON.parse(z.object({ view: z.string() }).parse(rows[0]).view),
            )
          : null;
      },
      getCommand: async (
        context,
        requestId: string,
      ): Promise<z.infer<typeof commandReceiptSchema> | null> => {
        const rows = await context.db.execute(
          'SELECT id, event, accepted_at, processed FROM commands WHERE id=?',
          requestIdSchema.parse(requestId),
        );
        if (!rows[0]) return null;
        const row = receiptRowSchema.parse(rows[0]);
        return commandReceiptSchema.parse({
          id: row.id,
          acceptedAt: row.accepted_at,
          status:
            row.processed === 0
              ? 'pending'
              : row.processed === 1
                ? 'delivered'
                : 'rejected',
          ...(row.processed === 2 ? { reason: terminal.code } : {}),
        });
      },
      info: (context) => {
        metadataSchema.parse(context.state);
        return context.state;
      },
    },
  });
}
