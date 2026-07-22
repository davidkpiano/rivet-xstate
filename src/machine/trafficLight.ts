import { createMachine, types } from 'xstate';

export type TrafficContext = {
  completedCycles: number;
  manualAdvances: number;
  outages: number;
  lastChange: string;
};

export type TrafficEvent =
  | { type: 'NEXT' }
  | { type: 'POWER_OUTAGE' }
  | { type: 'RESTORE' }
  | { type: 'RESET' };

type SignalChanged = {
  value: string;
};

const initialContext: TrafficContext = {
  completedCycles: 0,
  manualAdvances: 0,
  outages: 0,
  lastChange: 'initialized',
};

const manual = (target: string, lastChange: string) =>
  ({ context }: { context: TrafficContext }) => ({
    target,
    context: {
      manualAdvances: context.manualAdvances + 1,
      lastChange,
    },
  });

const announce = (value: string) => (_: unknown, enqueue: any) => {
  enqueue.emit({ type: 'signalChanged', value });
};

// Defined per running state (not on the root) so `snapshot.can()` reports
// false once the power is already out.
const powerOutage = ({ context }: { context: TrafficContext }) => ({
  target: 'outage',
  context: {
    outages: context.outages + 1,
    lastChange: 'power outage',
  },
});

export const trafficLightMachine = createMachine({
  id: 'trafficLight',
  schemas: {
    context: types<TrafficContext>(),
    events: {
      NEXT: types<Extract<TrafficEvent, { type: 'NEXT' }>>(),
      POWER_OUTAGE: types<Extract<TrafficEvent, { type: 'POWER_OUTAGE' }>>(),
      RESTORE: types<Extract<TrafficEvent, { type: 'RESTORE' }>>(),
      RESET: types<Extract<TrafficEvent, { type: 'RESET' }>>(),
    },
    emitted: {
      signalChanged: types<SignalChanged>(),
    },
  },
  context: initialContext,
  initial: 'green',
  on: {
    RESET: () => ({
      target: '.green',
      context: { ...initialContext, lastChange: 'manual reset' },
    }),
  },
  states: {
    green: {
      entry: announce('green'),
      after: {
        5_000: () => ({
          target: 'yellow',
          context: { lastChange: 'green timer elapsed' },
        }),
      },
      on: {
        NEXT: manual('yellow', 'green manually advanced'),
        POWER_OUTAGE: powerOutage,
      },
    },
    yellow: {
      entry: announce('yellow'),
      after: {
        2_000: () => ({
          target: 'red.walk',
          context: { lastChange: 'yellow timer elapsed' },
        }),
      },
      on: {
        NEXT: manual('red.walk', 'yellow manually advanced'),
        POWER_OUTAGE: powerOutage,
      },
    },
    red: {
      initial: 'walk',
      on: {
        POWER_OUTAGE: powerOutage,
      },
      states: {
        walk: {
          entry: announce('red.walk'),
          after: {
            4_000: () => ({
              target: 'flashing',
              context: { lastChange: 'walk timer elapsed' },
            }),
          },
          on: {
            NEXT: manual('flashing', 'walk manually advanced'),
          },
        },
        flashing: {
          entry: announce('red.flashing'),
          after: {
            3_000: () => ({
              target: 'stop',
              context: { lastChange: 'flashing timer elapsed' },
            }),
          },
          on: {
            NEXT: manual('stop', 'flashing manually advanced'),
          },
        },
        stop: {
          entry: announce('red.stop'),
          after: {
            2_000: ({ context }) => ({
              target: '#trafficLight.green',
              context: {
                completedCycles: context.completedCycles + 1,
                lastChange: 'red timer elapsed',
              },
            }),
          },
          on: {
            NEXT: ({ context }) => ({
              target: '#trafficLight.green',
              context: {
                completedCycles: context.completedCycles + 1,
                manualAdvances: context.manualAdvances + 1,
                lastChange: 'red manually advanced',
              },
            }),
          },
        },
      },
    },
    outage: {
      entry: announce('outage'),
      on: {
        RESTORE: () => ({
          target: 'red.stop',
          context: { lastChange: 'power restored' },
        }),
      },
    },
  },
});
