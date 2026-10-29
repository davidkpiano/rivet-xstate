import { createInspector, type Inspector } from '@statelyai/sdk/inspect';
import { DurableActorExecution } from '@demo/xstate-durable';
import { setup } from 'xstate';
import { expect, test } from 'vitest';
import { z } from 'zod';

const machine = setup({}).createMachine({
  id: 'document-review',
  initial: 'draft',
  states: {
    draft: { on: { submit: { target: 'reviewing' } } },
    reviewing: { on: { ping: {} } },
  },
});

function execution() {
  return new DurableActorExecution({
    logic: machine,
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
