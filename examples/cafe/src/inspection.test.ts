import { createInspector, type Inspector } from '@statelyai/sdk/inspect';
import { DurableActorExecution } from '@demo/xstate-durable';
import { setup, type AnyActorLogic } from 'xstate';
import { expect, test } from 'vitest';
import { z } from 'zod';

const machine = setup({}).createMachine({
  id: 'document-review',
  initial: 'draft',
  states: {
    draft: { on: { submit: { target: 'reviewing' } } },
    reviewing: {
      on: { ping: {}, approve: { target: 'approved' } },
    },
    approved: {},
  },
});

function execution(logic: AnyActorLogic = machine) {
  return new DurableActorExecution({
    logic,
    input: undefined,
    executionId: 'review-42',
    startedAt: 100,
    runtime: {
      scheduleTimer: () => {},
      cancelTimer: () => {},
      cancelAllTimers: () => {},
      runLogic: () => {
        throw new Error('This machine has no async actors.');
      },
    },
    drainLogic: async () => {},
    hasPendingLogic: () => false,
    yield: async () => {},
  });
}

function inspector() {
  const messages: unknown[] = [];
  const errors: unknown[] = [];
  const instance = createInspector({
    launch: 'none',
    bidirectional: false,
    transport: {
      ready: true,
      send: (message) => {
        messages.push(message);
      },
      onMessage: () => () => {},
      onReady: (handler) => {
        queueMicrotask(handler);
        return () => {};
      },
      destroy: () => {},
    },
  });
  instance.on('error', (error) => errors.push(error));
  return { instance, messages, errors };
}

async function deliver(
  core: DurableActorExecution,
  type: string,
  live?: Inspector,
) {
  await core.deliver({
    address: core.execution.rootAddress,
    at: 101,
    event: { type },
  });
  if (live) {
    core.commitInspection(live);
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}

const messageSchema = z.object({ type: z.string() }).passthrough();
const snapshots = (messages: unknown[]) =>
  messages.map((message) => messageSchema.parse(message)).filter(
    (message) => message.type === '@statelyai.system.actorSnapshot',
  );

test('neutral execution resumes replay without history and preserves repeated identical fresh events', async () => {
  const live = inspector();
  const restarted = inspector();
  try {
    await Promise.all([live.instance.ready, restarted.instance.ready]);
    const first = execution();
    await first.start();
    first.commitInspection(live.instance);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(
      live.messages.map((message) => messageSchema.parse(message)).filter(
        (message) => message.type === '@statelyai.system.actorRegistered',
      ),
    ).toHaveLength(1);
    await deliver(first, 'submit', live.instance);
    await deliver(first, 'ping', live.instance);
    expect(live.errors).toEqual([]);

    const replay = execution();
    await replay.start();
    await deliver(replay, 'submit');
    await deliver(replay, 'ping');
    const before = live.messages.length;
    replay.resumeInspection(live.instance);
    replay.commitInspection(live.instance);
    expect(live.messages).toHaveLength(before);
    await deliver(replay, 'ping', live.instance);
    const pings = snapshots(live.messages).filter((message) =>
      z.object({ type: z.literal('ping') }).safeParse(message.event).success,
    );
    expect(pings).toHaveLength(2);
    expect(pings[0]?.snapshot).toMatchObject({ value: 'reviewing' });
    expect(pings[0]?.snapshot).toEqual(pings[1]?.snapshot);
    expect(
      live.messages.map((message) => messageSchema.parse(message)).some(
        (message) => message.replaces === true,
      ),
    ).toBe(false);

    replay.resumeInspection(restarted.instance);
    const checkpoint = restarted.messages.find(
      (message) =>
        messageSchema.parse(message).type === '@statelyai.system.init' &&
        z.object({ actors: z.array(z.unknown()).min(1) }).safeParse(message).success,
    );
    expect(checkpoint).toMatchObject({
      actors: [{ actorId: 'document-review', snapshot: { value: 'reviewing' } }],
    });
    expect(snapshots(restarted.messages)).toHaveLength(0);
    const resumedCount = restarted.messages.length;
    replay.resumeInspection(restarted.instance);
    expect(restarted.messages).toHaveLength(resumedCount);
    await deliver(replay, 'ping', restarted.instance);
    expect(snapshots(restarted.messages)).toHaveLength(1);
    expect(live.errors).toEqual([]);
    expect(restarted.errors).toEqual([]);
  } finally {
    live.instance.destroy();
    restarted.instance.destroy();
  }
});

test('a fresh commit restores the pre-turn checkpoint after unpublished replay', async () => {
  const live = inspector();
  try {
    await live.instance.ready;
    const before = live.messages.length;
    const replay = execution();
    await replay.start();
    await deliver(replay, 'submit');
    await deliver(replay, 'ping');
    expect(live.messages).toHaveLength(before);

    await deliver(replay, 'approve', live.instance);
    const messages = live.messages.map((message) => messageSchema.parse(message));
    const checkpointIndex = messages.findIndex(
      (message) =>
        message.type === '@statelyai.system.init' &&
        z.object({ actors: z.array(z.unknown()).min(1) }).safeParse(message).success,
    );
    const snapshotIndex = messages.findIndex(
      (message) => message.type === '@statelyai.system.actorSnapshot',
    );
    expect(checkpointIndex).toBeGreaterThanOrEqual(0);
    expect(checkpointIndex).toBeLessThan(snapshotIndex);
    expect(messages[checkpointIndex]).toMatchObject({
      actors: [{ actorId: 'document-review', snapshot: { value: 'reviewing' } }],
    });
    expect(snapshots(live.messages)).toEqual([
      expect.objectContaining({
        event: { type: 'approve' },
        previousSnapshot: expect.objectContaining({ value: 'reviewing' }),
        snapshot: expect.objectContaining({ value: 'approved' }),
      }),
    ]);
    expect(
      messages.filter(
        (message) => message.type === '@statelyai.system.actorRegistered',
      ),
    ).toEqual([]);
    expect(live.errors).toEqual([]);
  } finally {
    live.instance.destroy();
  }
});

test('resume discards a pending turn baseline without rewinding a later empty commit', async () => {
  const live = inspector();
  try {
    await live.instance.ready;
    const replay = execution();
    await replay.start();
    await deliver(replay, 'submit');
    replay.resumeInspection(live.instance);
    const before = live.messages.length;
    replay.commitInspection(live.instance);
    replay.resumeInspection(live.instance);
    replay.commitInspection(live.instance);
    expect(live.messages).toHaveLength(before);

    await deliver(replay, 'approve', live.instance);
    expect(snapshots(live.messages)).toEqual([
      expect.objectContaining({
        previousSnapshot: expect.objectContaining({ value: 'reviewing' }),
        snapshot: expect.objectContaining({ value: 'approved' }),
      }),
    ]);
    expect(live.errors).toEqual([]);
  } finally {
    live.instance.destroy();
  }
});

test('a committed delivery with no inspection events still restores the actor tree', async () => {
  const live = inspector();
  try {
    await live.instance.ready;
    const replay = execution();
    await replay.start();
    await deliver(replay, 'submit');
    await replay.deliver({
      address: 'missing',
      at: 102,
      event: { type: 'ignored' },
    });
    replay.commitInspection(live.instance);

    expect(live.messages).toContainEqual(
      expect.objectContaining({
        type: '@statelyai.system.init',
        actors: [
          expect.objectContaining({
            actorId: 'document-review',
            snapshot: expect.objectContaining({ value: 'reviewing' }),
          }),
        ],
      }),
    );
    expect(snapshots(live.messages)).toEqual([]);
    expect(live.errors).toEqual([]);
  } finally {
    live.instance.destroy();
  }
});

test('the pre-turn checkpoint retains stopped children and parents before registering new children', async () => {
  const child = setup({}).createMachine({
    initial: 'idle',
    states: { idle: {} },
  });
  const parent = setup({ actors: { child } }).createMachine({
    id: 'parent',
    initial: 'first',
    states: {
      first: {
        invoke: { src: 'child', id: 'first-child' },
        on: { next: { target: 'second' } },
      },
      second: {
        invoke: { src: 'child', id: 'second-child' },
        on: { next: { target: 'third' } },
      },
      third: { invoke: { src: 'child', id: 'third-child' } },
    },
  });
  const live = inspector();
  try {
    await live.instance.ready;
    const replay = execution(parent);
    await replay.start();
    await deliver(replay, 'next');
    await deliver(replay, 'next', live.instance);

    const messages = live.messages.map((message) => messageSchema.parse(message));
    const checkpoint = messages.find(
      (message) =>
        message.type === '@statelyai.system.init' &&
        z.object({ actors: z.array(z.unknown()).min(1) }).safeParse(message).success,
    );
    const actors = z
      .object({
        actors: z.array(
          z.object({
            actorId: z.string(),
            sessionId: z.string(),
            parentSessionId: z.string().nullable(),
            snapshot: z.object({ status: z.string(), value: z.string() }),
          }),
        ),
      })
      .parse(checkpoint).actors;
    expect(actors).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          actorId: 'parent',
          snapshot: expect.objectContaining({ value: 'second' }),
        }),
        expect.objectContaining({
          actorId: 'first-child',
          snapshot: expect.objectContaining({ status: 'stopped' }),
        }),
        expect.objectContaining({
          actorId: 'second-child',
          snapshot: expect.objectContaining({ status: 'active' }),
        }),
      ]),
    );
    expect(actors).toHaveLength(3);
    const root = actors.find((actor) => actor.actorId === 'parent');
    expect(root?.parentSessionId).toBeNull();
    for (const actor of actors.filter((actor) => actor.actorId !== 'parent')) {
      expect(actor.parentSessionId).toBe(root?.sessionId);
    }
    expect(
      messages.filter(
        (message) => message.type === '@statelyai.system.actorRegistered',
      ),
    ).toEqual([
      expect.objectContaining({
        actorId: 'third-child',
        parentSessionId: root?.sessionId,
      }),
    ]);
    expect(snapshots(live.messages)).toContainEqual(
      expect.objectContaining({
        previousSnapshot: expect.objectContaining({ value: 'second' }),
        snapshot: expect.objectContaining({ value: 'third' }),
      }),
    );
    expect(live.errors).toEqual([]);
  } finally {
    live.instance.destroy();
  }
});
