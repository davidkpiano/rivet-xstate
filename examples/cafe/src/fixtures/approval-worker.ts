import { appendFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { actor, event, queue, setup, UserError } from 'rivetkit';
import {
  createAsyncLogic,
  setup as setupMachine,
  types,
  type EventFromLogic,
} from 'xstate';
import { z } from 'zod';
import { xstate, type DurableActorView } from '@demo/xstate-durable/rivet';
import { db } from 'rivetkit/db';
import { workflow } from 'rivetkit/workflow';
import {
  commandInbox,
  type Command,
  type Metadata,
} from '../rivet/commands.ts';

export const approvalInput = z.object({
  document: z.string().transform((value) => `${value} [checked]`),
  expiresIn: z.number().int().positive(),
  finishOnValidation: z.boolean().default(false),
});
export const approvalEvent = z.discriminatedUnion('type', [
  z.object({
    type: z.literal('note'),
    text: z.string().transform((value) => `[${value}]`),
    id: z.string().default('doc-note'),
    sessionId: z.string().default('review-session'),
  }),
  z.object({
    type: z.literal('approve'),
    reviewer: z.number().transform((value) => `reviewer-${value}`),
  }),
]);
type Input = z.infer<typeof approvalInput>;

function approvalMachine(validate: () => Promise<boolean>) {
  return setupMachine({
    schemas: {
      input: types<Input>(),
      context: types<Input & { notes: number; lastNote: string }>(),
      events: {
        note: types<{ text: string }>(),
        approve: types<{ reviewer: string }>(),
      },
    },
    actors: { validate: createAsyncLogic({ run: validate }) },
    delays: { expiry: ({ context }) => context.expiresIn },
  }).createMachine({
    id: 'document-approval',
    context: ({ input }) => ({ ...input, notes: 0, lastNote: '' }),
    initial: 'validating',
    states: {
      validating: {
        invoke: {
          id: 'validation',
          src: 'validate',
          onDone: ({ context }) => ({
            target: context.finishOnValidation ? 'approved' : 'waiting',
          }),
        },
      },
      waiting: {
        after: { expiry: { target: 'expired' } },
        on: {
          note: ({ context, event }) => ({
            context: { notes: context.notes + 1, lastNote: event.text },
          }),
          approve: { target: 'approved' },
        },
      },
      approved: { type: 'final' },
      expired: { type: 'final' },
    },
  });
}

export function createApprovalRegistry(
  storage: string,
  port: number,
  gateInitial?: () => Promise<void>,
  mode = 'normal',
) {
  let gated = false;
  const commands = commandInbox<Input, typeof approvalEvent>({
    eventSchema: approvalEvent,
    admission: async ({ event, commands }) => {
      if (event.type === 'note') {
        const { type, ...data } = event;
        if (await commands.has({ ...data, type }))
          throw new UserError('Note already accepted.', {
            code: 'duplicate_note',
          });
      }
    },
  });
  const approvalRun = xstate(
    ({ executionId }) => {
      appendFileSync(join(storage, 'factories.log'), `${executionId}\n`);
      return {
        logic: approvalMachine(async () => {
          appendFileSync(join(storage, 'validations.log'), `${executionId}\n`);
          return true;
        }),
      };
    },
    {
      historyVersion: mode === 'version-two' ? '2' : '1',
      queue: 'events',
      input: async (c: { state: Metadata<Input> }) => {
        if (mode === 'late-bootstrap') await gateInitial?.();
        return c.state.input;
      },
      mapEvent: (body: Command<z.output<typeof approvalEvent>>) => ({
        event: body.event,
        id: body.id,
      }),
      onDelivery: (delivery) =>
        process.send?.({ type: 'delivery', data: delivery }),
      beforePublish: async (view) => {
        if (
          !gated &&
          gateInitial &&
          (view.status === 'done' || mode === 'receive-gap')
        ) {
          gated = true;
          await gateInitial();
        }
      },
      onSnapshot: commands.publish,
    },
  );
  const legacyRun: typeof approvalRun = workflow(async (context) => {
    await context.step('metadata', async () => ({
      version: 'legacy-whole-actor-host',
    }));
    process.send?.({ type: 'legacy.history' });
    await context.queue.nextBatch('wait', { names: ['events'], timeout: 1000 });
  });
  const runApproval: typeof approvalRun = async (c) => {
    if (mode === 'receive-gap') {
      const receive = c.queue.nextBatch.bind(c.queue);
      c.queue.nextBatch = async (options) => {
        const messages = await receive(options);
        if (messages.length) {
          process.send?.({ type: 'fault.receive' });
          process.kill(process.pid, 'SIGSTOP');
        }
        return messages;
      };
    }

    try {
      await (mode === 'legacy' ? legacyRun : approvalRun)(c);
    } catch (error) {
      process.send?.({ type: 'protocol.rejected', message: String(error) });
      throw error;
    }
  };
  const approval = actor({
    createState: (
      c,
      input: { config: z.input<typeof approvalInput>; inspect?: boolean },
    ): Metadata<Input> => ({
      input: approvalInput.parse(input.config),
      inspect: input.inspect ?? false,
      executionId: c.actorId,
      startedAt: Date.now(),
    }),
    db: commands.database,
    queues: { events: queue<Command<z.output<typeof approvalEvent>>>() },
    run: runApproval,
    actions: commands.actions,
  });
  // Ordinary actors can share the registry without opting into durable execution.
  const health = actor({ state: {}, actions: { ping: () => 'pong' } });
  type ComposedState = Input & {
    custom: number;
    preferences: { locale: string };
    view: DurableActorView | null;
  };
  const composedLogic = approvalMachine(async () => true);
  const composedHandler = xstate(composedLogic, {
    queue: 'events',
    input: (c: { state: ComposedState }) => c.state,
    onSnapshot: (c, view, delivery) => {
      c.state.view = view;
      if (mode === 'ack-gap' && delivery?.event.type === 'note') {
        process.send?.({ type: 'fault.beforeComplete' });
        process.kill(process.pid, 'SIGSTOP');
      }
    },
  });
  const composed = actor({
    createState: (_c, input: z.input<typeof approvalInput>): ComposedState => ({
      ...approvalInput.parse(input),
      custom: 0,
      preferences: { locale: 'en' },
      view: null,
    }),
    createVars: () => ({ label: 'caller vars' }),
    db: db({
      onMigrate: async (database) => {
        await database.execute(
          'CREATE TABLE IF NOT EXISTS caller_values (value TEXT)',
        );
      },
    }),
    onWake: async (c) => {
      await c.db.execute('INSERT INTO caller_values VALUES (?)', 'wake');
    },
    queues: {
      events: queue<EventFromLogic<typeof composedLogic>>(),
      audit: queue<string>(),
    },
    events: { changed: event<number>() },
    run: composedHandler,
    actions: {
      submit: (c, event: EventFromLogic<typeof composedLogic>) =>
        c.queue.send('events', event),
      snapshot: (c) => c.state.view,
      editInput: (c) => {
        c.state.document = 'changed after initialization';
        c.state.preferences.locale = 'fr';
        return c.state.document;
      },
      own: async (c) => {
        c.state.custom++;
        c.broadcast('changed', c.state.custom);
        await c.queue.send('audit', 'owned');
        return {
          label: c.vars.label,
          custom: c.state.custom,
          rows: await c.db.execute('SELECT value FROM caller_values'),
        };
      },
    },
  });
  return setup({
    use: { approval, health, composed },
    startEngine: true,
    enginePort: port,
    httpPort: port + 1,
    noWelcome: true,
    startServices: false,
  });
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [storage, port, mode] = process.argv.slice(2);
  if (!storage || !port) throw new Error('Storage and port required.');
  process.env.RIVETKIT_STORAGE_PATH = storage;
  let release: (() => void) | undefined;
  const registry = createApprovalRegistry(
    storage,
    Number(port),
    mode === 'initial-done' ||
      mode === 'late-bootstrap' ||
      mode === 'receive-gap'
      ? () =>
          new Promise<void>((resolve) => {
            release = resolve;
            process.send?.({
              type:
                mode === 'late-bootstrap'
                  ? 'initial.input'
                  : mode === 'receive-gap'
                    ? 'initial.receive'
                    : 'initial.terminal',
            });
          })
      : undefined,
    mode,
  );
  process.on('message', (message) => {
    if (message === 'publish') release?.();
  });
  const enginePid = () => {
    const row = execFileSync('ps', ['-axo', 'pid=,ppid=,comm='], {
      encoding: 'utf8',
    })
      .split('\n')
      .map((line) => line.trim().split(/\s+/))
      .find(
        (parts) =>
          Number(parts[1]) === process.pid &&
          parts.slice(2).join(' ').includes('rivet-engine'),
      );
    return row ? Number(row[0]) : null;
  };
  const monitor = setInterval(() => {
    const pid = enginePid();
    if (pid) process.send?.({ type: 'engine', enginePid: pid });
  }, 500);
  await registry.startAndWait();
  clearInterval(monitor);
  process.send?.({ type: 'ready', enginePid: enginePid() });
}
