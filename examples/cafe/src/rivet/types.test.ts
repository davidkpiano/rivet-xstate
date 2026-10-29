import { actor, queue } from 'rivetkit';
import { setup, types } from 'xstate';
import { expect, test } from 'vitest';
import { xstate } from '@demo/xstate-durable/rivet';

const machine = setup({
  schemas: {
    input: types<{ count: number }>(),
    context: types<{ count: number }>(),
    events: { increment: types<{ amount: number }>() },
  },
}).createMachine({ context: ({ input }) => input });

test('the run handler preserves machine input and queue event types', () => {
  const run = xstate(machine, { queue: 'events', input: (c) => c.state });
  const definition = actor({
    state: { count: 0 },
    queues: { events: queue<{ type: 'increment'; amount: number }>() },
    run,
  });
  expect(definition).toBeDefined();
  if (false) {
    xstate(machine, {
      queue: 'events',
      // @ts-expect-error Machine input cannot contain a string count.
      input: () => ({ count: 'wrong' }),
    });
    actor({
      state: { count: 0 },
      // @ts-expect-error The run handler's queue must carry machine events.
      queues: { events: queue<{ type: 'wrong' }>() },
      run,
    });
    actor({
      state: { count: 0 },
      // @ts-expect-error The selected queue must exist.
      queues: { other: queue<{ type: 'increment'; amount: number }>() },
      run,
    });
  }
});
