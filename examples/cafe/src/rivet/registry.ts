import { randomUUID } from 'node:crypto';
import { actor, queue, setup, UserError, type Registry } from 'rivetkit';
import { createClient } from 'rivetkit/client';
import { db } from 'rivetkit/db';
import { workflow } from 'rivetkit/workflow';
import { createInspector } from '@statelyai/sdk/inspect';
import { z } from 'zod';
import {
  cafeInputSchema,
  cafeCommandSchema,
  createCafeMachine,
} from '../model.ts';
import { serializable } from '../inspection.ts';
import { RivetExecution, type Delivery } from './execution.ts';
import { paymentLedger } from './payments.ts';
import { retryIdempotentRivetRequest } from './requests.ts';

const startSchema = z.object({
  config: cafeInputSchema,
  inspect: z.boolean().default(true),
});
const metadataSchema = z.object({
  version: z.literal('rivet-cafe-1'),
  executionId: z.string(),
  input: cafeInputSchema,
  inspect: z.boolean(),
  startedAt: z.number(),
  roomId: z.string(),
  producerId: z.string(),
});
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
    reason: z.literal('cafe_closed'),
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
export type RivetCafeView = z.infer<typeof viewSchema>;

export function createCafeRegistry(
  options: {
    endpoint?: string;
    enginePort?: number;
    httpPort?: number;
    afterCommandCommitted?: (commandId: string) => void;
    afterPaymentResponse?: (kind: 'charge' | 'refund', orderId: string) => void;
    onView?: (view: RivetCafeView) => void;
    onDelivery?: (delivery: Delivery) => void;
    beforePublish?: (view: RivetCafeView) => Promise<void>;
    now?: () => number;
  } = {},
) {
  const now = options.now ?? Date.now;
  const endpoint =
    options.endpoint ?? `http://127.0.0.1:${options.enginePort ?? 6480}`;
  const cafe = actor({
    options: { name: 'Durable XState café', icon: 'coffee' },
    createState: (_context, input: unknown) => {
      const parsed = startSchema.parse(input);
      return metadataSchema.parse({
        version: 'rivet-cafe-1',
        executionId: randomUUID(),
        input: parsed.config,
        inspect: parsed.inspect,
        startedAt: now(),
        roomId: randomUUID(),
        producerId: randomUUID(),
      });
    },
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
      const metadata = metadataSchema.parse(
        await context.step('metadata', async (step) => step.state),
      );
      const client =
        createClient<Registry<{ payments: typeof paymentLedger }>>(endpoint);
      const ledger = client.payments.getOrCreate([metadata.executionId]);
      const services = {
        charge: async (request: { orderId: string; amount: number }) => {
          const result = await retryIdempotentRivetRequest(
            () => ledger.charge(request),
            context.abortSignal,
          );
          options.afterPaymentResponse?.('charge', request.orderId);
          return result;
        },
        refund: async (request: { orderId: string; amount: number }) => {
          const result = await retryIdempotentRivetRequest(
            () => ledger.refund(request),
            context.abortSignal,
          );
          options.afterPaymentResponse?.('refund', request.orderId);
          return result;
        },
      };
      const inspector = metadata.inspect
        ? createInspector({
            name: 'Rivet durable café',
            roomId: metadata.roomId,
            producerId: metadata.producerId,
            launch: 'none',
            bidirectional: false,
            sessionIdReuse: 'always',
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
      const disposeInspector = () => inspector?.destroy();
      context.abortSignal.addEventListener('abort', disposeInspector, {
        once: true,
      });
      const execution = new RivetExecution({
        logic: createCafeMachine(services),
        input: metadata.input,
        executionId: metadata.executionId,
        startedAt: metadata.startedAt,
        journal: (key, run) => context.step(key, async () => run()),
        inspect: (event) => inspector?.inspect(event),
        signal: context.abortSignal,
      });
      const view = (): RivetCafeView =>
        viewSchema.parse({
          ...execution.view(),
          inspectorUrl: inspector?.inspectorUrl ?? null,
          status:
            execution.view().actors.find((entry) => entry.id === 'cafe')
              ?.status ?? 'created',
        });
      try {
        await execution.start();
        await options.beforePublish?.(view());
        await context.step('initial-view', async (step) => {
          await step.db.execute(
            'INSERT OR REPLACE INTO published VALUES (1, ?)',
            JSON.stringify(view()),
          );
          options.onView?.(view());
        });
        // A plain loop replays the full actor tree. Rivet's compacting loop would skip earlier turns.
        for (let turn = 0; view().status === 'active'; turn++) {
          const delivery: Delivery | null = await context.step(
            `input:${turn}`,
            async (step) => {
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
                  event: cafeCommandSchema.parse(JSON.parse(command.event)),
                  at: command.accepted_at,
                  commandId: command.id,
                };
              return null;
            },
          );
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
      } finally {
        context.abortSignal.removeEventListener('abort', disposeInspector);
        inspector?.destroy();
        await client.dispose();
      }
    }),
    actions: {
      dispatch: async (context, raw: unknown, requestId = randomUUID()) => {
        const event = cafeCommandSchema.parse(raw);
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
                  'Command rejected because the café closed.',
                  { code: 'cafe_closed' },
                );
              return existing.accepted_at;
            }
            if (event.type === 'arrival') {
              const closing = await transaction.execute(
                'SELECT id FROM commands WHERE event=? LIMIT 1',
                JSON.stringify({ type: 'close' }),
              );
              if (closing.length)
                throw new UserError('Café is closing.', {
                  code: 'cafe_closing',
                });
            }
            const published = await transaction.execute(
              'SELECT view FROM published WHERE id=1',
            );
            if (published[0]) {
              const row = z.object({ view: z.string() }).parse(published[0]);
              if (viewSchema.parse(JSON.parse(row.view)).status !== 'active')
                throw new UserError('Café is closed.', { code: 'cafe_closed' });
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
      getSnapshot: async (context): Promise<RivetCafeView | null> => {
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
          ...(row.processed === 2 ? { reason: 'cafe_closed' } : {}),
        });
      },
      info: (context) => metadataSchema.parse(context.state),
    },
  });
  return setup({
    use: { cafe, payments: paymentLedger },
    ...(options.endpoint
      ? { endpoint, startEngine: false }
      : { startEngine: true, enginePort: options.enginePort ?? 6480 }),
    httpPort: options.httpPort ?? 6481,
    noWelcome: true,
    startServices: false,
  });
}
