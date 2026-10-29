import { appendFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { actor, setup, UserError } from 'rivetkit';
import { createAsyncLogic, setup as setupMachine, types } from 'xstate';
import { z } from 'zod';
import { durableRivetActor } from '@demo/xstate-durable/rivet';

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
) {
  let gated = false;
  const approval = durableRivetActor({
    inputSchema: approvalInput,
    eventSchema: approvalEvent,
    onDelivery: (delivery) =>
      process.send?.({ type: 'delivery', data: delivery }),
    logic: ({ executionId }) => ({
      logic: approvalMachine(async () => {
        appendFileSync(join(storage, 'validations.log'), `${executionId}\n`);
        return true;
      }),
    }),
    beforePublish: async (view) => {
      if (!gated && gateInitial && view.status === 'done') {
        gated = true;
        await gateInitial();
      }
    },
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
  // Ordinary actors can share the registry without opting into durable execution.
  const health = actor({ state: {}, actions: { ping: () => 'pong' } });
  return setup({
    use: { approval, health },
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
    mode === 'initial-done'
      ? () =>
          new Promise<void>((resolve) => {
            release = resolve;
            process.send?.({ type: 'initial.terminal' });
          })
      : undefined,
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
