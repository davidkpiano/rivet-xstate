// JSON-safe mirror of trafficLightMachine for the Stately embed.
// The real machine uses dynamic function transitions (xstate v6 alpha),
// which can't cross postMessage — this config carries the static structure
// (states + targets) so the visualizer can render the statechart.
export const trafficLightVizConfig = {
  id: 'trafficLight',
  initial: 'green',
  on: {
    RESET: '.green',
  },
  states: {
    green: {
      after: { 5000: 'yellow' },
      on: { NEXT: 'yellow', POWER_OUTAGE: 'outage' },
    },
    yellow: {
      after: { 2000: 'red.walk' },
      on: { NEXT: 'red.walk', POWER_OUTAGE: 'outage' },
    },
    red: {
      initial: 'walk',
      on: { POWER_OUTAGE: 'outage' },
      states: {
        walk: {
          after: { 4000: 'flashing' },
          on: { NEXT: 'flashing' },
        },
        flashing: {
          after: { 3000: 'stop' },
          on: { NEXT: 'stop' },
        },
        stop: {
          after: { 2000: '#trafficLight.green' },
          on: { NEXT: '#trafficLight.green' },
        },
      },
    },
    outage: {
      on: { RESTORE: 'red.stop' },
    },
  },
};
