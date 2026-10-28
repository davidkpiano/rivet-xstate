import type { InspectionEvent } from 'xstate';

export type ActorView = {
  id: string;
  address: string;
  status: string;
  value: unknown;
  context: unknown;
};

export function serializable(value: unknown): unknown {
  if (value === undefined) return undefined;
  return JSON.parse(
    JSON.stringify(value, (_key, item: unknown) => {
      if (
        item !== null &&
        typeof item === 'object' &&
        'sessionId' in item &&
        'id' in item
      ) {
        return {
          actorId: item.id,
          address: 'address' in item ? item.address : undefined,
        };
      }
      if (typeof item === 'bigint') return item.toString();
      return item;
    }),
  );
}

export function recordActor(
  actors: Map<string, ActorView>,
  event: InspectionEvent,
): void {
  const snapshot = event.snapshot;
  const id =
    event.type === '@xstate.actor'
      ? event.id
      : 'id' in event.actorRef && typeof event.actorRef.id === 'string'
        ? event.actorRef.id
        : (event.actorRef.sessionId ?? 'unknown');
  actors.set(event.actorRef.sessionId ?? id, {
    id,
    address:
      'address' in event.actorRef && typeof event.actorRef.address === 'string'
        ? event.actorRef.address
        : id,
    status: snapshot?.status ?? 'created',
    value: snapshot && 'value' in snapshot ? snapshot.value : undefined,
    context:
      snapshot && 'context' in snapshot
        ? serializable(snapshot.context)
        : undefined,
  });
}
