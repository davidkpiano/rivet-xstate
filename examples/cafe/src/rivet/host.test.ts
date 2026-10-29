import { fork, type ChildProcess } from 'node:child_process';
import { createServer } from 'node:net';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { afterEach, expect, test } from 'vitest';
import { createClient } from 'rivetkit/client';
import { z } from 'zod';
import {
  retryIdempotentRivetRequest,
  viewSchema,
} from '@demo/xstate-durable/rivet';
import type { createApprovalRegistry } from '../fixtures/approval-worker.ts';

const resources: {
  storage: string;
  workers: ChildProcess[];
  enginePid: number | null;
}[] = [];

async function kill(worker: ChildProcess) {
  if (worker.exitCode !== null || worker.signalCode !== null) return;
  const stopped = new Promise<void>((resolve) =>
    worker.once('exit', () => resolve()),
  );
  worker.kill('SIGKILL');
  await stopped;
}

afterEach(async () => {
  for (const resource of resources.splice(0)) {
    await Promise.all(resource.workers.map(kill));
    if (resource.enginePid) {
      try {
        process.kill(resource.enginePid, 'SIGTERM');
      } catch (error) {
        if (!(
          error instanceof Error &&
          'code' in error &&
          error.code === 'ESRCH'
        ))
          throw error;
      }
      const deadline = Date.now() + 7000;
      for (;;) {
        try {
          process.kill(resource.enginePid, 0);
        } catch {
          break;
        }
        if (Date.now() > deadline)
          throw new Error(
            `Engine ${resource.enginePid} did not stop; preserving storage.`,
          );
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
    rmSync(resource.storage, { recursive: true, force: true });
  }
});

async function freePort() {
  const server = createServer();
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  if (!address || typeof address === 'string')
    throw new Error('No TCP address.');
  await new Promise<void>((resolve) => server.close(() => resolve()));
  return address.port;
}

async function eventually<T>(
  read: () => Promise<T>,
  accept: (value: T) => boolean,
) {
  const deadline = Date.now() + 35_000;
  let last: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await read();
      if (accept(value)) return value;
      last = value;
    } catch (error) {
      last = String(error);
    }
    await new Promise((resolve) => setTimeout(resolve, 75));
  }
  throw new Error(`Approval did not converge: ${JSON.stringify(last)}`);
}

async function start(
  resource: (typeof resources)[number],
  port: number,
  mode = 'normal',
  observe?: (message: unknown) => void,
) {
  let ready = false;
  let stderr = '';
  const worker = fork(
    fileURLToPath(new URL('../fixtures/approval-worker.ts', import.meta.url)),
    [resource.storage, String(port), mode],
    {
      execArgv: ['--import', 'tsx'],
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
      env: { ...process.env, RIVET_LOG_LEVEL: 'error', RUST_LOG: 'error' },
    },
  );
  resource.workers.push(worker);
  worker.stderr?.on('data', (chunk) => {
    stderr += String(chunk);
  });
  worker.on('message', (message) => {
    observe?.(message);
    const parsed = z
      .object({ type: z.string(), enginePid: z.number().nullable() })
      .safeParse(message);
    if (!parsed.success) return;
    if (parsed.data.enginePid) resource.enginePid = parsed.data.enginePid;
    if (parsed.data.type === 'ready') ready = true;
  });
  await eventually(async () => {
    if (worker.exitCode !== null || worker.signalCode !== null)
      throw new Error(`Approval runner exited: ${stderr}`);
    return ready;
  }, Boolean);
  return worker;
}

test('public Rivet host runs an unrelated approval machine with durable commands, invokes and deadlines', async () => {
  const resource = {
    storage: mkdtempSync(join(tmpdir(), 'durable-approval-')),
    workers: [] as ChildProcess[],
    enginePid: null as number | null,
  };
  resources.push(resource);
  const port = await freePort();
  const worker = await start(resource, port);
  const client = createClient<ReturnType<typeof createApprovalRegistry>>(
    `http://127.0.0.1:${port}`,
  );
  const approval = client.approval.getOrCreate(['expires'], {
    createWithInput: {
      config: { document: 'proposal.pdf', expiresIn: 8000 },
      inspect: false,
    },
  });
  if (false) {
    // @ts-expect-error Machine events remain checked through the Rivet client.
    void approval.dispatch({ type: 'arrival', drink: 'latte' });
    // @ts-expect-error Required event payload is preserved.
    void approval.dispatch({ type: 'approve' });
    // @ts-expect-error The RPC accepts schema input, not its transformed output.
    void approval.dispatch({ type: 'approve', reviewer: 'reviewer-7' });
  }
  const root = (view: unknown) =>
    viewSchema
      .nullable()
      .parse(view)
      ?.actors.find((entry) => entry.id === 'document-approval');
  const validations = () =>
    readFileSync(join(resource.storage, 'validations.log'), 'utf8')
      .trim()
      .split('\n');
  try {
    expect(await client.health.getOrCreate(['health']).ping()).toBe('pong');
    const waiting = viewSchema.parse(
      await eventually(
        () => approval.getSnapshot(),
        (view) => root(view)?.value === 'waiting',
      ),
    );
    expect(waiting.status).toBe('active');
    expect(waiting.actors.some((entry) => entry.id === 'cafe')).toBe(false);
    expect(validations()).toHaveLength(1);
    const timer = waiting.timers[0];
    expect(timer).toBeDefined();
    await approval.dispatch({ type: 'note', text: 'Reviewed' }, 'same-note');
    await approval.dispatch({ type: 'note', text: 'Reviewed' }, 'same-note');
    await expect(
      approval.dispatch({ type: 'note', text: 'Different' }, 'same-note'),
    ).rejects.toMatchObject({ code: 'conflicting_command' });
    const noted = await eventually(
      () => approval.getSnapshot(),
      (view) =>
        z.object({ notes: z.literal(1) }).safeParse(root(view)?.context)
          .success,
    );
    expect(root(noted)?.context).toMatchObject({
      notes: 1,
      lastNote: '[Reviewed]',
    });
    expect(await approval.getCommand('same-note')).toMatchObject({
      status: 'delivered',
    });
    await new Promise((resolve) => setTimeout(resolve, 1200));
    expect(validations()).toHaveLength(1);
    await kill(worker);
    await new Promise((resolve) =>
      setTimeout(resolve, Math.max(0, (timer?.at ?? 0) - Date.now() + 100)),
    );
    const resumedDeliveries: unknown[] = [];
    await start(resource, port, 'normal', (message) => {
      const delivery = z
        .object({ type: z.literal('delivery'), data: z.unknown() })
        .safeParse(message);
      if (delivery.success) resumedDeliveries.push(delivery.data.data);
    });
    const expired = await eventually(
      () => approval.getSnapshot(),
      (view) => view?.status === 'done' && root(view)?.value === 'expired',
    );
    expect(root(expired)?.context).toMatchObject({
      notes: 1,
      document: 'proposal.pdf [checked]',
    });
    expect(resumedDeliveries).toContainEqual(timer);
    expect(validations()).toHaveLength(1);
    await expect(
      approval.dispatch({ type: 'approve', reviewer: 7 }, 'too-late'),
    ).rejects.toMatchObject({ code: 'execution_closed' });
    expect(await approval.getCommand('too-late')).toBeNull();

    const second = client.approval.getOrCreate(['approved'], {
      createWithInput: {
        config: { document: 'another.pdf', expiresIn: 30_000 },
        inspect: false,
      },
    });
    await eventually(
      () => second.getSnapshot(),
      (view) => root(view)?.value === 'waiting',
    );
    await retryIdempotentRivetRequest(() =>
      second.dispatch({ type: 'approve', reviewer: 7 }, 'approval'),
    );
    await eventually(
      () => second.getSnapshot(),
      (view) => view?.status === 'done' && root(view)?.value === 'approved',
    );
    expect(await second.getCommand('approval')).toMatchObject({
      status: 'delivered',
    });
    expect(validations()).toHaveLength(2);
  } finally {
    await client.dispose();
  }
}, 90_000);

test('initial completion rejects commands accepted before the first publication', async () => {
  const resource = {
    storage: mkdtempSync(join(tmpdir(), 'durable-initial-')),
    workers: [] as ChildProcess[],
    enginePid: null as number | null,
  };
  resources.push(resource);
  const port = await freePort();
  let terminal = false;
  const worker = await start(resource, port, 'initial-done', (message) => {
    if (
      z.object({ type: z.literal('initial.terminal') }).safeParse(message)
        .success
    )
      terminal = true;
  });
  const client = createClient<ReturnType<typeof createApprovalRegistry>>(
    `http://127.0.0.1:${port}`,
  );
  const approval = client.approval.getOrCreate(['initial-final'], {
    createWithInput: {
      config: {
        document: 'done.pdf',
        expiresIn: 10_000,
        finishOnValidation: true,
      },
      inspect: false,
    },
  });
  try {
    await approval.info();
    await eventually(async () => terminal, Boolean);
    await approval.dispatch(
      { type: 'note', text: 'During startup' },
      'startup-command',
    );
    expect(await approval.getCommand('startup-command')).toMatchObject({
      status: 'pending',
    });
    worker.send('publish');
    await eventually(
      () => approval.getSnapshot(),
      (view) => view?.status === 'done',
    );
    expect(await approval.getCommand('startup-command')).toMatchObject({
      status: 'rejected',
      reason: 'execution_closed',
    });
    await expect(
      approval.dispatch(
        { type: 'note', text: 'During startup' },
        'startup-command',
      ),
    ).rejects.toMatchObject({ code: 'execution_closed' });
  } finally {
    await client.dispose();
  }
}, 60_000);

test('admission compares normalized events independent of property order', async () => {
  const resource = {
    storage: mkdtempSync(join(tmpdir(), 'durable-admission-')),
    workers: [] as ChildProcess[],
    enginePid: null as number | null,
  };
  resources.push(resource);
  const port = await freePort();
  await start(resource, port);
  const client = createClient<ReturnType<typeof createApprovalRegistry>>(
    `http://127.0.0.1:${port}`,
  );
  const approval = client.approval.getOrCreate(['notes'], {
    createWithInput: {
      config: { document: 'notes.pdf', expiresIn: 30_000 },
      inspect: false,
    },
  });
  try {
    await eventually(
      () => approval.getSnapshot(),
      (view) =>
        view?.actors.some((entry) => entry.value === 'waiting') ?? false,
    );
    await approval.dispatch({ type: 'note', text: 'a' }, 'first');
    await eventually(
      () => approval.getCommand('first'),
      (receipt) => receipt?.status === 'delivered',
    );
    await expect(
      approval.dispatch({ type: 'note', text: 'a' }, 'second'),
    ).rejects.toMatchObject({ code: 'duplicate_note' });
    expect(await approval.getCommand('second')).toBeNull();
  } finally {
    await client.dispose();
  }
}, 60_000);
