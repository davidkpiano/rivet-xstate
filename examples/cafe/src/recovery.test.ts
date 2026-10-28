import { fork } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { afterEach, expect, test } from 'vitest';
import { z } from 'zod';
import { openCafe } from './run.ts';

const directories: string[] = [];
afterEach(() => {
  for (const path of directories.splice(0))
    rmSync(path, { recursive: true, force: true });
});
function database() {
  const directory = mkdtempSync(join(tmpdir(), 'cafe-recovery-'));
  directories.push(directory);
  return join(directory, 'cafe.sqlite');
}

async function crashAt(
  path: string,
  mode: 'charge' | 'brew',
): Promise<unknown> {
  const child = fork(
    fileURLToPath(new URL('./fixtures/crash-worker.ts', import.meta.url)),
    [path, mode],
    {
      execArgv: ['--import', 'tsx'],
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'],
    },
  );
  let stderr = '';
  child.stderr?.on('data', (chunk) => {
    stderr += String(chunk);
  });
  const exited = new Promise<{
    code: number | null;
    signal: NodeJS.Signals | null;
  }>((resolve, reject) => {
    child.once('exit', (code, signal) => resolve({ code, signal }));
    child.once('error', reject);
  });
  const message = new Promise<unknown>((resolve) =>
    child.once('message', resolve),
  );
  let timeout: NodeJS.Timeout | undefined;
  try {
    const result = await Promise.race([
      message,
      exited.then((status) => {
        throw new Error(
          `Worker exited before crash point: ${JSON.stringify(status)} ${stderr}`,
        );
      }),
      new Promise<never>((_resolve, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`Crash point timeout. ${stderr}`)),
          8000,
        );
      }),
    ]);
    expect(z.object({ type: z.string() }).parse(result).type).toBe(
      mode === 'charge' ? 'charge.committed' : 'brewing',
    );
    child.kill('SIGKILL');
    expect((await exited).signal).toBe('SIGKILL');
    return result;
  } finally {
    clearTimeout(timeout);
    child.kill('SIGKILL');
    await exited;
  }
}

test('SIGKILL after the provider commits and before async journaling recovers one effective charge', async () => {
  const path = database();
  await crashAt(path, 'charge');
  const db = new DatabaseSync(path);
  expect(db.prepare('SELECT * FROM payments').all()).toHaveLength(1);
  expect(db.prepare('SELECT * FROM results').all()).toHaveLength(0);
  db.close();
  const resumed = await openCafe({
    database: path,
    inspect: false,
    now: () => 2000,
  });
  await resumed.host.tick(2000);
  expect(
    resumed.host.view().actors.find((actor) => actor.id === 'cafe')?.context,
  ).toMatchObject({ served: 1, failed: 0 });
  expect(resumed.store.payments()).toHaveLength(1);
  await resumed.host.close();
});

test('SIGKILL during extraction preserves the equipment reservation, actor tree and timer deadline', async () => {
  const path = database();
  const before = z
    .object({
      timers: z.array(
        z.object({ address: z.string(), due_at: z.number() }).passthrough(),
      ),
    })
    .parse(await crashAt(path, 'brew'));
  const resumed = await openCafe({
    database: path,
    inspect: false,
    now: () => 1060,
  });
  expect(
    resumed.host.view().actors.find((actor) => actor.id === 'barista-1')?.value,
  ).toEqual({ working: 'extracting' });
  expect(
    resumed.host.view().actors.find((actor) => actor.id === 'espresso')
      ?.context,
  ).toMatchObject({ holder: { ownerId: 'barista-1' } });
  expect(resumed.store.timers()).toEqual(before.timers);
  await resumed.host.tick(1099);
  expect(
    resumed.host.view().actors.find((actor) => actor.id === 'barista-1')?.value,
  ).toEqual({ working: 'extracting' });
  await resumed.host.tick(2000);
  expect(
    resumed.host.view().actors.find((actor) => actor.id === 'cafe')?.context,
  ).toMatchObject({ served: 1 });
  expect(resumed.store.payments()).toHaveLength(1);
  expect(resumed.host.view().timers).toHaveLength(0);
  await resumed.host.close();
});

test('replay after cancellation preserves exactly one charge and refund', async () => {
  const path = database();
  let now = 1000;
  const first = await openCafe({
    database: path,
    inspect: false,
    now: () => now,
  });
  now = 2600;
  await first.host.tick();
  await first.host.send({ type: 'cancel', orderId: 'order-1' });
  await first.host.tick(100000);
  const before = first.host.view().actors.find((actor) => actor.id === 'cafe');
  await first.host.close();
  const second = await openCafe({
    database: path,
    inspect: false,
    now: () => 100000,
  });
  await second.host.tick(100000);
  expect(
    second.host.view().actors.find((actor) => actor.id === 'cafe'),
  ).toEqual(before);
  expect(
    second.store.payments().filter((row) => row.order_id === 'order-1'),
  ).toHaveLength(2);
  await second.host.close();
});
