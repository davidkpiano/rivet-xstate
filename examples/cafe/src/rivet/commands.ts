import { randomUUID } from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { UserError, queue, type ActionContext } from 'rivetkit';
import { db, type RawAccess } from 'rivetkit/db';
import { z } from 'zod';
import type { Delivery } from '@demo/xstate-durable/rivet';

const commandRowSchema = z.object({
  id: z.string(),
  event: z.string(),
  submitted_at: z.number(),
  accepted_at: z.number().nullable(),
});
const requestIdSchema = z.string().min(1).max(200);
const receiptRowSchema = commandRowSchema.extend({
  processed: z.union([z.literal(-1), z.literal(0), z.literal(1), z.literal(2)]),
});
const receiptBase = z.object({ id: z.string(), submittedAt: z.number() });
export const commandReceiptSchema = z.discriminatedUnion('status', [
  receiptBase.extend({ status: z.literal('submitting'), acceptedAt: z.null() }),
  receiptBase.extend({ status: z.literal('pending'), acceptedAt: z.number() }),
  receiptBase.extend({
    status: z.literal('delivered'),
    acceptedAt: z.number(),
  }),
  receiptBase.extend({
    status: z.literal('rejected'),
    acceptedAt: z.number().nullable(),
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
export type Command<E> = { id: string; event: E };

export type Metadata<I> = {
  input: I;
  executionId: string;
  inspect: boolean;
  startedAt: number;
};
type CommandContext<I, E> = ActionContext<
  Metadata<I>,
  unknown,
  unknown,
  unknown,
  unknown,
  ReturnType<typeof db>,
  {},
  { events: ReturnType<typeof queue<Command<E>>> }
>;
export function commandInbox<I, S extends z.ZodType>(options: {
  eventSchema: S;
  admission?: (context: {
    event: z.output<S>;
    commands: { has(event: z.output<S>): Promise<boolean> };
  }) => void | Promise<void>;
  terminal?: { code: string; message: string; rejectedMessage?: string };
  afterCommandCommitted?: (id: string) => void;
  beforeEnqueue?: (id: string) => void;
  now?: () => number;
}) {
  const now = options.now ?? Date.now;
  const terminal = options.terminal ?? {
    code: 'execution_closed',
    message: 'Execution is closed.',
  };
  return {
    database: db({
      onMigrate: async (database) => {
        await database.execute(
          'CREATE TABLE IF NOT EXISTS commands (id TEXT PRIMARY KEY, event TEXT NOT NULL, submitted_at INTEGER NOT NULL, accepted_at INTEGER, processed INTEGER NOT NULL DEFAULT -1)',
        );
        await database.execute(
          'CREATE TABLE IF NOT EXISTS published (id INTEGER PRIMARY KEY CHECK (id=1), view TEXT NOT NULL)',
        );
      },
    }),
    publish: async (
      context: { db: RawAccess },
      view: DurableActorView,
      delivery: Delivery | null,
    ) => {
      await context.db.transaction(
        async (transaction) => {
          await transaction.execute(
            'INSERT OR REPLACE INTO published VALUES (1, ?)',
            JSON.stringify(view),
          );
          if (delivery?.commandId)
            await transaction.execute(
              'UPDATE commands SET processed=1, accepted_at=COALESCE(accepted_at,?) WHERE id=?',
              delivery.acceptedAt ?? delivery.at,
              delivery.commandId,
            );
          if (view.status !== 'active')
            await transaction.execute(
              'UPDATE commands SET processed=2 WHERE processed IN (-1,0)',
            );
        },
        { name: 'publish' },
      );
    },
    actions: {
      dispatch: async (
        context: CommandContext<I, z.output<S>>,
        raw: z.input<S>,
        requestId: string = randomUUID(),
      ) => {
        const event = z.parse(options.eventSchema, raw);
        const id = requestIdSchema.parse(requestId);
        const payload = JSON.stringify(event);
        const existingAcceptance = await context.db.transaction(
          async (transaction) => {
            const rows = await transaction.execute(
              'SELECT id, event, submitted_at, accepted_at, processed FROM commands WHERE id=?',
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
              return existing.processed === 1 ? existing.accepted_at : null;
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
              'INSERT INTO commands (id,event,submitted_at) VALUES (?,?,?)',
              id,
              payload,
              at,
            );
            return null;
          },
          { name: 'accept-command' },
        );
        if (existingAcceptance !== null)
          return { id, acceptedAt: existingAcceptance };
        options.beforeEnqueue?.(id);
        const message = z
          .object({ createdAt: z.number() })
          .parse(await context.queue.send('events', { id, event }));
        await context.db.execute(
          'UPDATE commands SET processed=CASE WHEN processed=-1 THEN 0 ELSE processed END, accepted_at=COALESCE(accepted_at,?) WHERE id=?',
          message.createdAt,
          id,
        );
        options.afterCommandCommitted?.(id);
        const receipt = z
          .object({ accepted_at: z.number() })
          .parse(
            (
              await context.db.execute(
                'SELECT accepted_at FROM commands WHERE id=?',
                id,
              )
            )[0],
          );
        return { id, acceptedAt: receipt.accepted_at };
      },
      getSnapshot: async (
        context: CommandContext<I, z.output<S>>,
      ): Promise<DurableActorView | null> => {
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
        context: CommandContext<I, z.output<S>>,
        requestId: string,
      ): Promise<z.infer<typeof commandReceiptSchema> | null> => {
        const rows = await context.db.execute(
          'SELECT id, event, submitted_at, accepted_at, processed FROM commands WHERE id=?',
          requestIdSchema.parse(requestId),
        );
        if (!rows[0]) return null;
        const row = receiptRowSchema.parse(rows[0]);
        return commandReceiptSchema.parse({
          id: row.id,
          submittedAt: row.submitted_at,
          acceptedAt: row.accepted_at,
          status:
            row.processed === -1
              ? 'submitting'
              : row.processed === 0
                ? 'pending'
                : row.processed === 1
                  ? 'delivered'
                  : 'rejected',
          ...(row.processed === 2 ? { reason: terminal.code } : {}),
        });
      },
      info: (context: CommandContext<I, z.output<S>>) => {
        return context.state;
      },
    },
  };
}
