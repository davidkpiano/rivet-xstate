import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, expect, test } from 'vitest';
import { createAsyncLogic, createMachine, setup, types } from 'xstate';
import { LocalDurableHost } from './local-host.ts';
import { CafeStore } from './store.ts';

const directories: string[] = [];
afterEach(() => {
  for (const path of directories.splice(0))
    rmSync(path, { recursive: true, force: true });
});
function database() {
  const directory = mkdtempSync(join(tmpdir(), 'durable-cafe-'));
  directories.push(directory);
  return join(directory, 'cafe.sqlite');
}

test('a child timer resumes against its original deadline with stable actor identities', async () => {
  const path = database();
  let now = 1000;
  const child = createMachine({
    id: 'worker',
    initial: 'brewing',
    states: {
      brewing: { after: { 500: { target: 'done' } } },
      done: { type: 'final' },
    },
  });
  const machine = setup({ actors: { worker: child } }).createMachine({
    id: 'test-cafe',
    version: '1',
    initial: 'open',
    states: {
      open: {
        invoke: { id: 'espresso', src: 'worker', onDone: { target: 'done' } },
      },
      done: { type: 'final' },
    },
  });
  const first = new LocalDurableHost({
    logic: machine,
    input: undefined,
    store: new CafeStore(path),
    now: () => now,
  });
  await first.start();
  const identities = [...first.actors.keys()];
  expect(first.view().timers).toHaveLength(1);
  expect(first.view().timers[0]).toMatchObject({ due_at: 1500 });
  await first.close();
  now = 1400;
  const second = new LocalDurableHost({
    logic: machine,
    input: undefined,
    store: new CafeStore(path),
    now: () => now,
  });
  await second.start();
  expect([...second.actors.keys()]).toEqual(identities);
  await second.tick();
  expect(
    second.view().actors.find((actor) => actor.id === 'test-cafe')?.value,
  ).toBe('open');
  now = 1500;
  await second.tick();
  expect(
    second.view().actors.find((actor) => actor.id === 'test-cafe')?.value,
  ).toBe('done');
  expect(second.view().timers).toHaveLength(0);
  await second.close();
});

test('completed async actors replay their results without rerunning external work', async () => {
  const path = database();
  let charges = 0;
  const charge = createAsyncLogic({
    id: 'charge',
    run: async () => {
      charges++;
      return { receiptId: 'receipt-1' };
    },
  });
  const machine = setup({ actors: { charge } }).createMachine({
    id: 'payment',
    version: '1',
    initial: 'paying',
    states: {
      paying: {
        invoke: { src: 'charge', id: 'charge', onDone: { target: 'paid' } },
      },
      paid: { type: 'final' },
    },
  });
  const first = new LocalDurableHost({
    logic: machine,
    input: undefined,
    store: new CafeStore(path),
  });
  await first.start();
  expect(
    first.view().actors.find((actor) => actor.id === 'payment')?.value,
  ).toBe('paid');
  await first.close();
  const second = new LocalDurableHost({
    logic: machine,
    input: undefined,
    store: new CafeStore(path),
  });
  await second.start();
  expect(
    second.view().actors.find((actor) => actor.id === 'payment')?.value,
  ).toBe('paid');
  expect(charges).toBe(1);
  await second.close();
});

test('replaying cancellation does not resurrect timers', async () => {
  const path = database();
  const machine = createMachine({
    id: 'cancel-timer',
    version: '1',
    initial: 'waiting',
    schemas: { events: { CANCEL: types<{}>() } },
    states: {
      waiting: {
        after: { 100: { target: 'late' } },
        on: { CANCEL: { target: 'cancelled' } },
      },
      cancelled: {},
      late: {},
    },
  });
  const first = new LocalDurableHost({
    logic: machine,
    input: {},
    store: new CafeStore(path),
    now: () => 1000,
  });
  await first.start();
  await first.send({ type: 'CANCEL' });
  await first.close();
  const second = new LocalDurableHost({
    logic: machine,
    input: {},
    store: new CafeStore(path),
    now: () => 5000,
  });
  await second.start();
  await second.tick();
  expect(
    second.view().actors.find((actor) => actor.id === 'cancel-timer')?.value,
  ).toBe('cancelled');
  expect(second.view().timers).toHaveLength(0);
  await second.close();
});

test('a failed async result remains failed after restart even when the service recovers', async () => {
  const path = database();
  let attempts = 0;
  const charge = createAsyncLogic({
    run: async () => {
      if (++attempts === 1) throw new Error('declined');
      return 'approved';
    },
  });
  const machine = setup({ actors: { charge } }).createMachine({
    id: 'decline',
    version: '1',
    initial: 'paying',
    states: {
      paying: {
        invoke: {
          src: 'charge',
          onDone: { target: 'paid' },
          onError: { target: 'declined' },
        },
      },
      paid: { type: 'final' },
      declined: { type: 'final' },
    },
  });
  const first = new LocalDurableHost({
    logic: machine,
    input: {},
    store: new CafeStore(path),
  });
  await first.start();
  expect(
    first.view().actors.find((actor) => actor.id === 'decline')?.value,
  ).toBe('declined');
  await first.close();
  const resumed = new LocalDurableHost({
    logic: machine,
    input: {},
    store: new CafeStore(path),
  });
  await resumed.start();
  expect(
    resumed.view().actors.find((actor) => actor.id === 'decline')?.value,
  ).toBe('declined');
  expect(attempts).toBe(1);
  await resumed.close();
});

test('the local provider deduplicates charges and rejects conflicting reuse', () => {
  const store = new CafeStore(database());
  expect(store.payment('charge', { orderId: 'order-1', amount: 400 })).toEqual(
    store.payment('charge', { orderId: 'order-1', amount: 400 }),
  );
  expect(() =>
    store.payment('charge', { orderId: 'order-1', amount: 500 }),
  ).toThrow('Conflicting');
  expect(() =>
    store.payment('refund', { orderId: 'order-2', amount: 400 }),
  ).toThrow('No matching');
  store.payment('refund', { orderId: 'order-1', amount: 400 });
  store.payment('refund', { orderId: 'order-1', amount: 400 });
  expect(store.payments()).toHaveLength(2);
  store.close();
});

test('a second live process cannot own the same database', () => {
  const path = database();
  const first = new CafeStore(path);
  expect(() => new CafeStore(path)).toThrow('already owned');
  first.close();
});

test('a command keeps its acceptance timestamp when processing crosses a timer deadline', async () => {
  const store = new CafeStore(database());
  let time = 1000;
  const machine = createMachine({
    id: 'ordering',
    version: '1',
    initial: 'waiting',
    schemas: { events: { CANCEL: types<{}>() } },
    states: {
      waiting: {
        after: { 100: { target: 'expired' } },
        on: { CANCEL: { target: 'cancelled' } },
      },
      expired: {},
      cancelled: {},
    },
  });
  const host = new LocalDurableHost({
    logic: machine,
    input: {},
    store,
    now: () => {
      const at = time;
      time += 2;
      return at;
    },
  });
  await host.start();
  time = 1099;
  await host.send({ type: 'CANCEL' });
  expect(
    store.history().map(({ at, event }) => ({ at, type: event.type })),
  ).toEqual([{ at: 1099, type: 'CANCEL' }]);
  expect(
    host.view().actors.find((actor) => actor.id === 'ordering')?.value,
  ).toBe('cancelled');
  await host.close();
});
