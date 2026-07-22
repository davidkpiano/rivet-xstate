import { actor, event, setup } from 'rivetkit';
import { trafficLightMachine } from '../machine/trafficLight';
import { createXStateActor } from './xstateActor';

// Shared roster of running traffic lights, so every client (tab or browser)
// sees the same actor list live.
const directory = actor({
  options: { name: 'Directory', icon: 'list' },
  state: { ids: ['intersection-1'] },
  events: {
    ids: event<string[]>(),
  },
  actions: {
    list: (context) => context.state.ids,
    add: (context, id: string) => {
      if (!context.state.ids.includes(id)) {
        context.state.ids.push(id);
        context.broadcast('ids', [...context.state.ids]);
      }
      return context.state.ids;
    },
    remove: (context, id: string) => {
      context.state.ids = context.state.ids.filter(
        (existing) => existing !== id,
      );
      context.broadcast('ids', [...context.state.ids]);
      return context.state.ids;
    },
  },
});

const trafficLight = createXStateActor({
  machine: trafficLightMachine,
  options: { name: 'Traffic light', icon: 'traffic-light' },
  executeAction: (context, action) => {
    context.broadcast('effect', {
      type: action.type,
      params: action.params,
    });
  },
});

export const registry = setup({ use: { trafficLight, directory } });

registry.start();
