import { actor, event } from 'rivetkit';
import {
  getNextTransitions,
  initialTransition,
  isBuiltInExecutableAction,
  transition,
  type AnyStateMachine,
  type EventFromLogic,
  type ExecutableActionObjectFromLogic,
  type InputFrom,
  type SnapshotFrom,
} from 'xstate';

type PersistedSnapshot = Record<string, unknown>;

type RuntimeState = {
  snapshot: PersistedSnapshot;
  schedules: Record<string, string>;
};

type RuntimeContext = {
  state: RuntimeState;
  schedule: {
    after(delay: number, action: string, ...args: unknown[]): Promise<string>;
    cancel(id: string): Promise<boolean>;
  };
  broadcast(name: string, ...args: unknown[]): void;
  destroy(): void;
};

export type MachineView<M extends AnyStateMachine> = {
  value: MachineSnapshotOf<M>['value'];
  context: MachineSnapshotOf<M>['context'];
  status: MachineSnapshotOf<M>['status'];
  nextEvents: string[];
  /** For every external event type, whether the snapshot can handle it. */
  can: Record<string, boolean>;
};

type MachineSnapshotOf<M extends AnyStateMachine> = ReturnType<
  M['resolveState']
>;

export type XStateActorOptions<M extends AnyStateMachine> = {
  machine: M;
  input?: InputFrom<M>;
  options?: { name?: string; icon?: string };
  executeAction?: (
    context: RuntimeContext,
    action: ExecutableActionObjectFromLogic<M>,
  ) => void | Promise<void>;
};

function restore<M extends AnyStateMachine>(
  machine: M,
  persisted: PersistedSnapshot,
) {
  return machine.resolveState(persisted) as MachineSnapshotOf<M>;
}

function nextEvents<M extends AnyStateMachine>(snapshot: MachineSnapshotOf<M>) {
  const events = new Set<string>();

  for (const candidate of getNextTransitions(snapshot)) {
    const eventType = candidate.eventType;
    if (
      eventType &&
      !eventType.startsWith('xstate.') &&
      !eventType.startsWith('@xstate')
    ) {
      events.add(eventType);
    }
  }

  return [...events];
}

function externalEvents<M extends AnyStateMachine>(machine: M): string[] {
  return (
    (machine as unknown as { events: string[] }).events ?? []
  ).filter(
    (eventType) =>
      !eventType.startsWith('xstate.') && !eventType.startsWith('@xstate'),
  );
}

function view<M extends AnyStateMachine>(
  machine: M,
  persisted: PersistedSnapshot,
): MachineView<M> {
  const snapshot = restore(machine, persisted);
  const can: Record<string, boolean> = {};
  for (const eventType of externalEvents(machine)) {
    can[eventType] = (
      snapshot as unknown as { can(event: { type: string }): boolean }
    ).can({ type: eventType });
  }
  return {
    value: snapshot.value,
    context: snapshot.context,
    status: snapshot.status,
    nextEvents: nextEvents(snapshot),
    can,
  };
}

export function createXStateActor<M extends AnyStateMachine>(
  options: XStateActorOptions<M>,
) {
  const { machine } = options;
  const initialize = initialTransition as unknown as (
    logic: M,
    input?: InputFrom<M>,
  ) => [SnapshotFrom<M>, ExecutableActionObjectFromLogic<M>[]];
  const [initialSnapshot, initialActions] = initialize(machine, options.input);
  const initialState: RuntimeState = {
    snapshot: machine.getPersistedSnapshot(
      initialSnapshot,
    ) as PersistedSnapshot,
    schedules: {},
  };

  async function executeActions(
    context: RuntimeContext,
    actions: ExecutableActionObjectFromLogic<M>[],
  ) {
    for (const action of actions) {
      if (isBuiltInExecutableAction(action)) {
        if (action.type === '@xstate.raise') {
          if (action.delay !== undefined) {
            const scheduleId = await context.schedule.after(
              action.delay,
              'fireEvent',
              action.event,
              action.id,
            );
            if (action.id) context.state.schedules[action.id] = scheduleId;
          }
          continue;
        }

        if (action.type === '@xstate.cancel') {
          const scheduleId = context.state.schedules[action.id];
          if (scheduleId) {
            await context.schedule.cancel(scheduleId);
            delete context.state.schedules[action.id];
          }
          continue;
        }
      }

      if (!options.executeAction) {
        throw new Error(`No Rivet executor for XState action "${action.type}"`);
      }
      await options.executeAction(context, action);
    }
  }

  async function apply(
    context: RuntimeContext,
    machineEvent: EventFromLogic<M>,
  ) {
    const snapshot = restore(machine, context.state.snapshot);
    const [nextSnapshot, actions] = transition(
      machine,
      snapshot as SnapshotFrom<M>,
      machineEvent,
    );
    context.state.snapshot = machine.getPersistedSnapshot(
      nextSnapshot,
    ) as PersistedSnapshot;

    await executeActions(context, actions);

    const nextView = view(machine, context.state.snapshot);
    context.broadcast('snapshot', nextView);
    return nextView;
  }

  return actor({
    options: options.options,
    state: initialState,
    events: {
      snapshot: event<MachineView<M>>(),
      effect: event<{ type: string; params: unknown }>(),
    },
    onCreate: (context) =>
      executeActions(context as RuntimeContext, initialActions),
    actions: {
      dispatch: (context, machineEvent: EventFromLogic<M>) =>
        apply(context as RuntimeContext, machineEvent),
      fireEvent: (
        context,
        machineEvent: EventFromLogic<M>,
        logicalId?: string,
      ) => {
        if (logicalId) delete context.state.schedules[logicalId];
        return apply(context as RuntimeContext, machineEvent);
      },
      getSnapshot: (context) => view(machine, context.state.snapshot),
      stop: async (context) => {
        for (const scheduleId of Object.values(context.state.schedules)) {
          await (context as RuntimeContext).schedule.cancel(scheduleId);
        }
        context.state.schedules = {};
        (context as RuntimeContext).destroy();
      },
    },
  });
}
