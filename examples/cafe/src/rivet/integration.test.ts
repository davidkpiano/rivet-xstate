import { fork, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdtempSync, rmSync } from 'node:fs';
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
afterEach(async () => {
  for (const resource of resources.splice(0)) {
    for (const child of resource.workers) {
      if (child.exitCode === null && child.signalCode === null)
        child.kill('SIGKILL');
    }
    if (resource.enginePid) {
      try {
        process.kill(resource.enginePid, 'SIGTERM');
      } catch {}
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
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
  worker.on('message', (message) => messages.push(message));
  worker.stderr?.on('data', (chunk) => {
    stderr += String(chunk);
  });
  const wait = async (type: string, timeout = 25000): Promise<unknown> => {
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
    createWithInput: { config, inspect: false },
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
