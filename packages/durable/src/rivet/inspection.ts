import type { Inspector, Transport } from '@statelyai/sdk/inspect';
import type { ActorInspectionEvent, InspectionEvent } from 'xstate';

type ActorRef = InspectionEvent['actorRef'];

export class DurableInspection {
  private readonly refs = new Map<ActorRef['sessionId'], ActorRef>();
  private hydrated = false;

  constructor(
    private readonly inspector: Inspector,
    private transport?: Transport,
  ) {}

  get inspectorUrl() {
    return this.inspector.inspectorUrl;
  }

  private ref(actor: ActorRef): ActorRef {
    const existing = this.refs.get(actor.sessionId);
    if (existing) return existing;
    const stable = new Proxy(actor, {
      get: (target, key, receiver) => {
        const value = Reflect.get(target, key, receiver);
        if (key === '_parent' && value) return this.ref(value);
        return value;
      },
    });
    this.refs.set(actor.sessionId, stable);
    return stable;
  }

  publish(events: InspectionEvent[]): void {
    for (const event of events) {
      const actorRef = this.ref(event.actorRef);
      const normalized: InspectionEvent =
        event.type === '@xstate.actor'
          ? {
              ...event,
              actorRef,
              parentRef: event.parentRef && this.ref(event.parentRef),
            }
          : {
              ...event,
              actorRef,
              sourceRef: event.sourceRef && this.ref(event.sourceRef),
              targetRef: event.targetRef && this.ref(event.targetRef),
              sent: event.sent.map((sent) => ({
                ...sent,
                targetRef: this.ref(sent.targetRef),
              })),
            };
      this.inspector.inspect(normalized);
    }
    this.hydrated = true;
  }

  hydrate(actors: Iterable<ActorInspectionEvent>): void {
    if (this.hydrated) return;
    const events: InspectionEvent[] = [];
    for (const actor of actors) {
      events.push(actor, {
        type: '@xstate.transition',
        rootId: actor.rootId,
        actorRef: actor.actorRef,
        snapshot: actor.snapshot,
        eventType: 'xstate.hydrate',
        event: { type: 'xstate.hydrate' },
        sourceRef: undefined,
        targetRef: actor.actorRef,
        actions: [],
        sent: [],
        microsteps: [],
      });
    }
    this.publish(events);
  }

  destroy(): void {
    this.inspector.destroy();
    this.transport?.destroy();
    this.transport = undefined;
  }
}
