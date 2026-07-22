import {
  getNextTransitions,
  initialTransition,
  transition,
} from 'xstate';
import { describe, expect, it } from 'vitest';
import { trafficLightMachine } from './trafficLight';

describe('trafficLightMachine', () => {
  it('starts in green and describes its after transition as an action', () => {
    const [snapshot, actions] = initialTransition(trafficLightMachine);

    expect(snapshot.value).toBe('green');
    expect(actions.some((action) => action.type === '@xstate.raise')).toBe(true);
  });

  it('advances through nested red states without an XState actor', () => {
    let [snapshot] = initialTransition(trafficLightMachine);

    let actions;
    [snapshot, actions] = transition(trafficLightMachine, snapshot, {
      type: 'NEXT',
    });
    expect(snapshot.value).toBe('yellow');
    expect(actions.some((action) => action.type === '@xstate.cancel')).toBe(
      true,
    );
    expect(actions.some((action) => action.type === '@xstate.raise')).toBe(true);

    [snapshot] = transition(trafficLightMachine, snapshot, { type: 'NEXT' });
    expect(snapshot.value).toEqual({ red: 'walk' });

    [snapshot] = transition(trafficLightMachine, snapshot, { type: 'NEXT' });
    expect(snapshot.value).toEqual({ red: 'flashing' });

    [snapshot] = transition(trafficLightMachine, snapshot, { type: 'NEXT' });
    expect(snapshot.value).toEqual({ red: 'stop' });

    [snapshot] = transition(trafficLightMachine, snapshot, { type: 'NEXT' });
    expect(snapshot.value).toBe('green');
    expect(snapshot.context.completedCycles).toBe(1);
    expect(snapshot.context.manualAdvances).toBe(5);
  });

  it('exposes only domain events as next events', () => {
    const [snapshot] = initialTransition(trafficLightMachine);
    const eventTypes = getNextTransitions(snapshot).map(
      (candidate) => candidate.eventType,
    );

    expect(eventTypes).toContain('NEXT');
    expect(eventTypes).toContain('POWER_OUTAGE');
    expect(eventTypes).toContain('RESET');
    expect(eventTypes).toContain('xstate.after.5000.trafficLight.green');
  });
});
