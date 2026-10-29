import { fork, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { cpSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { createClient } from 'rivetkit/client';
import { afterEach, expect, test } from 'vitest';
import { z } from 'zod';
import { cafeInputSchema } from '../model.ts';
import { createCafeRegistry, viewSchema } from './registry.ts';
import { retryIdempotentRivetRequest } from './requests.ts';

const resources: {
  directory: string;
  workers: ChildProcess[];
  enginePid: number | null;
}[] = [];
afterEach(async (context) => {
  for (const resource of resources.splice(0)) {
    for (const child of resource.workers) {
      if (child.exitCode === null && child.signalCode === null) {
        const exited = new Promise<void>((resolve) =>
          child.once('exit', () => resolve()),
        );
        child.kill('SIGKILL');
        await exited;
      }
    }
    if (resource.enginePid) {
      try {
        process.kill(resource.enginePid, 'SIGTERM');
      } catch {}
    }
    if (resource.enginePid) {
      let deadline = Date.now() + 5000;
      let forced = false;
      for (;;) {
        try {
          process.kill(resource.enginePid, 0);
        } catch (error) {
          if (
            error instanceof Error &&
            'code' in error &&
            error.code === 'ESRCH'
          )
            break;
          throw error;
        }
        if (Date.now() >= deadline) {
          if (forced)
            throw new Error(
              `Rivet engine ${resource.enginePid} did not exit; preserving its storage.`,
            );
          process.kill(resource.enginePid, 'SIGKILL');
          forced = true;
          deadline = Date.now() + 2000;
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
    const logs = join(resource.directory, '.rivetkit/var/logs');
    if (context.task.result?.state === 'fail' && existsSync(logs)) {
      const target = fileURLToPath(
        new URL(
          `../../../../.audit/rivet-failure-${resource.directory.split('/').at(-1)}`,
          import.meta.url,
        ),
      );
      cpSync(logs, target, { recursive: true });
      process.stderr.write(`Rivet failure logs: ${target}\n`);
    }
    rmSync(resource.directory, { recursive: true, force: true });
  }
});

async function freePort(): Promise<number> {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string')
    throw new Error('No TCP address.');
  const port = address.port;
  await new Promise<void>((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
  return port;
}

async function start(
  resource: (typeof resources)[number],
  port: number,
  mode = 'normal',
) {
  const messages: unknown[] = [];
  let stderr = '';
  const worker = fork(
    fileURLToPath(new URL('../fixtures/rivet-worker.ts', import.meta.url)),
    [resource.directory, String(port), mode],
    {
      execArgv: ['--import', 'tsx'],
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
      env: { ...process.env, RIVET_LOG_LEVEL: 'error', RUST_LOG: 'error' },
    },
  );
  resource.workers.push(worker);
  worker.on('message', (message) => {
    messages.push(message);
    const engine = z
      .object({ type: z.literal('engine'), enginePid: z.number() })
      .safeParse(message);
    if (engine.success) resource.enginePid = engine.data.enginePid;
  });
  worker.stderr?.on('data', (chunk) => {
    stderr += String(chunk);
  });
  const wait = async (type: string, timeout = 45000): Promise<unknown> => {
    const start = Date.now();
    for (;;) {
      const match = messages.find(
        (message) =>
          z.object({ type: z.string() }).parse(message).type === type,
      );
      if (match) return match;
      if (worker.exitCode !== null || worker.signalCode !== null)
        throw new Error(`Rivet worker exited: ${stderr}`);
      if (Date.now() - start > timeout)
        throw new Error(`No ${type}: ${stderr}`);
      await new Promise((resolve) => setTimeout(resolve, 30));
    }
  };
  const ready = z
    .object({ enginePid: z.number().nullable() })
    .parse(await wait('ready'));
  if (ready.enginePid) resource.enginePid = ready.enginePid;
  return { worker, wait, messages, stderr: () => stderr };
}

async function fixture(mode: string, customers = 1, tickMs = 50) {
  const resource = {
    directory: mkdtempSync(join(tmpdir(), 'rivet-cafe-')),
    workers: [] as ChildProcess[],
    enginePid: null as number | null,
  };
  resources.push(resource);
  const port = await freePort();
  const server = await start(resource, port, mode);
  const registry = createCafeRegistry({ endpoint: `http://127.0.0.1:${port}` });
  const client = createClient<typeof registry>(`http://127.0.0.1:${port}`);
  const key = randomUUID();
  const config = cafeInputSchema.parse({
    customers,
    baristas: 2,
    tickMs,
    patienceTicks: 200,
  });
  const handle = client.cafe.getOrCreate([key], {
    createWithInput: { config, inspect: mode === 'inspection' },
  });
  return { ...server, resource, port, client, handle, config, key };
}

async function eventually<T>(
  read: () => Promise<T>,
  accept: (value: T) => boolean,
  timeout = 25000,
): Promise<T> {
  const began = Date.now();
  let last: unknown;
  while (Date.now() - began < timeout) {
    try {
      const value = await read();
      last = value;
      if (accept(value)) return value;
    } catch (error) {
      last = String(error);
    }
    await new Promise((resolve) => setTimeout(resolve, 75));
  }
  throw new Error(`Rivet result did not converge: ${JSON.stringify(last)}`);
}
function counts(view: unknown) {
  const parsed = viewSchema.nullable().parse(view);
  const root = parsed?.actors.find((entry) => entry.id === 'cafe');
  return root
    ? z
        .object({
          served: z.number(),
          cancelled: z.number(),
          refunded: z.number(),
          failed: z.number(),
        })
        .parse(root.context)
    : undefined;
}
async function kill(child: ChildProcess) {
  const exited = new Promise<void>((resolve) =>
    child.once('exit', () => resolve()),
  );
  child.kill('SIGKILL');
  await exited;
}

test('real Rivet workflow serves coffee and processes idempotent commands, refunds and repair', async () => {
  const cafe = await fixture('normal', 3, 20);
  try {
    const info = await cafe.handle.info();
    await eventually(
      () => cafe.handle.getSnapshot(),
      (view) => counts(view)?.served === 3,
    );
    await cafe.handle.dispatch(
      { type: 'equipment.break', equipment: 'grinder' },
      'break-grinder',
    );
    await cafe.handle.dispatch(
      { type: 'arrival', drink: 'latte' },
      'same-arrival',
    );
    await cafe.handle.dispatch(
      { type: 'arrival', drink: 'latte' },
      'same-arrival',
    );
    await eventually(
      () => cafe.handle.getSnapshot(),
      (view) =>
        view?.actors.some(
          (entry) =>
            entry.id === 'barista-1' &&
            JSON.stringify(entry.value).includes('waitingForGrinder'),
        ) === true ||
        view?.actors.some(
          (entry) =>
            entry.id === 'barista-2' &&
            JSON.stringify(entry.value).includes('waitingForGrinder'),
        ) === true,
    );
    await cafe.handle.dispatch(
      { type: 'cancel', orderId: 'order-4' },
      'cancel-four',
    );
    await cafe.handle.dispatch(
      { type: 'equipment.repair', equipment: 'grinder' },
      'repair-grinder',
    );
    const view = await eventually(
      () => cafe.handle.getSnapshot(),
      (view) => counts(view)?.refunded === 1,
    );
    expect(counts(view)).toMatchObject({
      served: 3,
      cancelled: 1,
      refunded: 1,
      failed: 0,
    });
    expect(
      view?.actors.filter((entry) => entry.id.startsWith('customer-')),
    ).toHaveLength(4);
    const receipts = await cafe.client.payments
      .getOrCreate([info.executionId])
      .receipts();
    expect(receipts).toHaveLength(5);
    const ledger = cafe.client.payments.getOrCreate([
      `ledger-test-${cafe.key}`,
    ]);
    const duplicates = await Promise.all(
      Array.from({ length: 8 }, () =>
        ledger.charge({ orderId: 'same', amount: 450 }),
      ),
    );
    expect(new Set(duplicates.map((receipt) => receipt.receiptId)).size).toBe(
      1,
    );
    await expect(
      ledger.charge({ orderId: 'same', amount: 500 }),
    ).rejects.toMatchObject({ group: 'user', code: 'conflicting_payment' });
    await expect(
      ledger.refund({ orderId: 'missing', amount: 450 }),
    ).rejects.toMatchObject({ group: 'user', code: 'missing_matching_charge' });
    await ledger.refund({ orderId: 'same', amount: 450 });
    await ledger.refund({ orderId: 'same', amount: 450 });
    expect(await ledger.receipts()).toHaveLength(2);
    await cafe.handle.dispatch({ type: 'close' }, 'close-cafe');
    await expect(
      cafe.handle.dispatch({ type: 'arrival', drink: 'espresso' }, 'too-late'),
    ).rejects.toMatchObject({ group: 'user', code: 'cafe_closing' });
    await expect(
      cafe.handle.dispatch({ type: 'close' }, 'close-cafe'),
    ).resolves.toMatchObject({ id: 'close-cafe' });
    await expect(
      cafe.handle.dispatch({ type: 'close' }, 'same-arrival'),
    ).rejects.toMatchObject({ group: 'user', code: 'conflicting_command' });
  } finally {
    await cafe.client.dispose();
  }
}, 60000);

test('commands admitted during the final transition receive a durable rejection', async () => {
  const cafe = await fixture('closing', 0);
  try {
    await cafe.handle.info();
    await cafe.handle.dispatch({ type: 'close' }, 'closing');
    await cafe.wait('fault.beforePublish');
    const accepted = await cafe.handle.dispatch(
      { type: 'cancel', orderId: 'order-1' },
      'late-cancel',
    );
    expect(await cafe.handle.getCommand('late-cancel')).toMatchObject({
      ...accepted,
      status: 'pending',
    });
    cafe.worker.send('publish');
    await eventually(
      () => cafe.handle.getSnapshot(),
      (view) => view?.status === 'done',
    );
    expect(await cafe.handle.getCommand('late-cancel')).toEqual({
      ...accepted,
      status: 'rejected',
      reason: 'cafe_closed',
    });
    expect(await cafe.handle.getCommand('closing')).toMatchObject({
      status: 'delivered',
    });
    await expect(
      cafe.handle.dispatch(
        { type: 'cancel', orderId: 'order-1' },
        'late-cancel',
      ),
    ).rejects.toMatchObject({ code: 'cafe_closed' });
    await expect(
      cafe.handle.dispatch(
        { type: 'cancel', orderId: 'order-1' },
        'after-closed',
      ),
    ).rejects.toMatchObject({ code: 'cafe_closed' });
    expect(await cafe.handle.getCommand('after-closed')).toBeNull();
    expect(await cafe.handle.getCommand('late-cancel')).toMatchObject({
      acceptedAt: accepted.acceptedAt,
    });
  } finally {
    await cafe.client.dispose();
  }
}, 60000);

test('SIGKILL after payment response recovers through native workflow replay without a second charge', async () => {
  const cafe = await fixture('payment');
  try {
    const info = await cafe.handle.info();
    await cafe.wait('fault.payment');
    await kill(cafe.worker);
    await start(cafe.resource, cafe.port);
    await eventually(
      () => cafe.handle.getSnapshot(),
      (view) => counts(view)?.served === 1,
      35000,
    );
    expect(
      await cafe.client.payments.getOrCreate([info.executionId]).receipts(),
    ).toHaveLength(1);
  } finally {
    await cafe.client.dispose();
  }
}, 80000);

test('SIGKILL after refund response recovers one effective compensation', async () => {
  const cafe = await fixture('refund', 0);
  try {
    const info = await cafe.handle.info();
    await cafe.handle.dispatch(
      { type: 'equipment.break', equipment: 'grinder' },
      'block-brew',
    );
    await cafe.handle.dispatch(
      { type: 'arrival', drink: 'espresso', patienceTicks: 2000 },
      'paid-order',
    );
    await eventually(
      () => cafe.handle.getSnapshot(),
      (view) =>
        view?.actors.some((a) =>
          JSON.stringify(a.value).includes('waitingForGrinder'),
        ) === true,
    );
    await cafe.handle.dispatch(
      { type: 'cancel', orderId: 'order-1' },
      'cancel-paid-order',
    );
    await cafe.wait('fault.payment');
    await kill(cafe.worker);
    await start(cafe.resource, cafe.port);
    const view = await eventually(
      () => cafe.handle.getSnapshot(),
      (view) => counts(view)?.refunded === 1,
      35000,
    );
    expect(counts(view)).toMatchObject({
      refunded: 1,
      cancelled: 1,
      served: 0,
      failed: 0,
    });
    const receipts = await cafe.client.payments
      .getOrCreate([info.executionId])
      .receipts();
    expect(receipts.map((r) => r.kind).sort()).toEqual(['charge', 'refund']);
  } finally {
    await cafe.client.dispose();
  }
}, 80000);

test('a due timer precedes a command accepted at the identical timestamp', async () => {
  const cafe = await fixture('tie');
  try {
    await cafe.handle.info();
    const initial = z
      .object({ data: viewSchema })
      .parse(await cafe.wait('fault.beforePublish')).data;
    const timer = [...initial.timers].sort((a, b) => a.at - b.at)[0];
    expect(timer).toBeDefined();
    if (!timer) throw new Error('No initial timer.');
    cafe.worker.send({ type: 'clock', at: timer.at });
    await cafe.wait('clock.set');
    const command = await cafe.handle.dispatch(
      { type: 'cancel', orderId: 'order-1' },
      'same-deadline',
    );
    expect(command.acceptedAt).toBe(timer.at);
    cafe.worker.send('publish');
    await eventually(
      () => cafe.handle.getCommand('same-deadline'),
      (result) => result?.status === 'delivered',
    );
    const deliveries = cafe.messages.flatMap((message) => {
      const parsed = z
        .object({ type: z.string(), data: z.unknown().optional() })
        .parse(message);
      return parsed.type === 'delivery' ? [parsed.data] : [];
    });
    expect(deliveries.slice(0, 2)).toEqual([
      timer,
      {
        address: 'cafe',
        at: timer.at,
        event: { type: 'cancel', orderId: 'order-1' },
        commandId: 'same-deadline',
      },
    ]);
  } finally {
    await cafe.client.dispose();
  }
}, 60000);

test('SIGKILL during extraction replays the original timer deadline', async () => {
  const cafe = await fixture('brew', 1, 200);
  try {
    await cafe.handle.info();
    const fault = z
      .object({ data: viewSchema })
      .parse(await cafe.wait('fault.brew'));
    const brewing = fault.data.actors.find(
      (entry) => JSON.stringify(entry.value) === '{"working":"extracting"}',
    );
    const timer = fault.data.timers.find(
      (entry) => entry.address === brewing?.address,
    );
    expect(timer).toBeDefined();
    if (!timer) throw new Error('No extraction timer.');
    await kill(cafe.worker);
    await new Promise((resolve) =>
      setTimeout(resolve, Math.max(0, timer.at - Date.now() + 100)),
    );
    const resumed = await start(cafe.resource, cafe.port);
    await eventually(
      () => cafe.handle.getSnapshot(),
      (view) => counts(view)?.served === 1,
      35000,
    );
    const deliveries = resumed.messages.filter(
      (message) =>
        z.object({ type: z.string() }).parse(message).type === 'delivery',
    );
    expect(deliveries).toContainEqual({ type: 'delivery', data: timer });
  } finally {
    await cafe.client.dispose();
  }
}, 80000);

test('a command committed before its wake notification survives SIGKILL and a retried request ID', async () => {
  const cafe = await fixture('command', 0);
  try {
    const info = await cafe.handle.info();
    const submitted = cafe.handle
      .dispatch({ type: 'arrival', drink: 'espresso' }, 'recover-this-order')
      .catch(() => undefined);
    await cafe.wait('fault.command');
    await kill(cafe.worker);
    await start(cafe.resource, cafe.port);
    await retryIdempotentRivetRequest(() =>
      cafe.handle.dispatch(
        { type: 'arrival', drink: 'espresso' },
        'recover-this-order',
      ),
    );
    const view = await eventually(
      () => cafe.handle.getSnapshot(),
      (view) => counts(view)?.served === 1,
      35000,
    );
    expect(
      view?.actors.filter((entry) => entry.id.startsWith('customer-')),
    ).toHaveLength(1);
    expect(
      await cafe.client.payments.getOrCreate([info.executionId]).receipts(),
    ).toHaveLength(1);
    await submitted;
  } finally {
    await cafe.client.dispose();
  }
}, 80000);


test('inspection survives native workflow sleeps and hydrates current state after runner restart', async () => {
  const cafe = await fixture('inspection', 0, 20);
  const inspection = (messages: unknown[]) => messages.flatMap((message) => {
    const parsed = z.object({ type: z.literal('inspection.message'), data: z.object({ type: z.string() }).passthrough() }).safeParse(message);
    return parsed.success ? [parsed.data.data] : [];
  });
  const lifecycle = (messages: unknown[], type: string) => messages.filter((message) => z.object({ type: z.string() }).parse(message).type === type).length;
  try {
    await eventually(() => cafe.handle.getSnapshot(), (view) => view?.status === 'active');
    await new Promise((resolve) => setTimeout(resolve, 3500));
    expect(lifecycle(cafe.messages, 'inspection.open')).toBe(1);
    expect(lifecycle(cafe.messages, 'inspection.close')).toBe(0);
    const before = inspection(cafe.messages).length;
    await new Promise((resolve) => setTimeout(resolve, 2200));
    expect(inspection(cafe.messages)).toHaveLength(before);
    await cafe.handle.dispatch({ type: 'arrival', drink: 'espresso' }, 'inspection-order');
    await eventually(() => cafe.handle.getSnapshot(), (view) => counts(view)?.served === 1);
    await new Promise((resolve) => setTimeout(resolve, 150));
    const messages = inspection(cafe.messages);
    const registrations = messages.filter((message) => message.type === '@statelyai.system.actorRegistered');
    expect(new Set(registrations.map((message) => message.sessionId)).size).toBe(registrations.length);
    expect(registrations.some((message) => message.replaces === true)).toBe(false);
    const snapshots = messages.filter((message) => message.type === '@statelyai.system.actorSnapshot');
    const served = snapshots.flatMap((message) => {
      const parsed = z.object({ context: z.object({ served: z.number() }) }).safeParse(message.snapshot);
      return parsed.success ? [parsed.data.context.served] : [];
    });
    expect(served.at(-1)).toBe(1);
    expect(served).toEqual([...served].sort((a, b) => a - b));
    await kill(cafe.worker);
    const restarted = await start(cafe.resource, cafe.port, 'inspection');
    await eventually(() => cafe.handle.getSnapshot(), (view) => counts(view)?.served === 1);
    await eventually(async () => inspection(restarted.messages), (messages) => messages.some((message) => {
      const parsed = z.object({ context: z.object({ served: z.literal(1) }) }).safeParse(message.snapshot);
      return parsed.success;
    }));
    const restored = inspection(restarted.messages);
    expect(lifecycle(restarted.messages, 'inspection.open')).toBe(1);
    const restoredSnapshots = restored.filter((message) => message.type === '@statelyai.system.actorSnapshot');
    expect(restoredSnapshots.every((message) => z.object({ event: z.object({ type: z.literal('xstate.hydrate') }) }).safeParse(message).success)).toBe(true);
    const restoredCount = restored.length;
    await new Promise((resolve) => setTimeout(resolve, 2200));
    expect(inspection(restarted.messages)).toHaveLength(restoredCount);
    await cafe.handle.dispatch({ type: 'close' }, 'inspection-close');
    await eventually(() => cafe.handle.getSnapshot(), (view) => view?.status === 'done');
    await eventually(async () => lifecycle(restarted.messages, 'inspection.close'), (closed) => closed === 1);
  } finally {
    await cafe.client.dispose();
  }
}, 90000);
