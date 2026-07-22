import { useEffect, useRef, useState } from 'react';
import type { TrafficEvent } from './machine/trafficLight';
import type { MachineView } from './server/xstateActor';
import type { trafficLightMachine } from './machine/trafficLight';
import { trafficLightVizConfig } from './machine/trafficLightViz';
import { useActor } from './client/rivet';
import {
  createEmbeddedInspector,
  STATELY_EMBED_URL,
  type EmbeddedInspector,
} from './client/inspector';

type TrafficView = MachineView<typeof trafficLightMachine>;

const eventLabels: Record<TrafficEvent['type'], string> = {
  NEXT: 'Advance',
  POWER_OUTAGE: 'Cut power',
  RESTORE: 'Restore',
  RESET: 'Reset',
};

function nextActorId(ids: string[]) {
  let n = ids.length + 1;
  while (ids.includes(`intersection-${n}`)) n += 1;
  return `intersection-${n}`;
}

function statePath(value: TrafficView['value']) {
  if (typeof value === 'string') return value;
  const [parent, child] = Object.entries(value)[0] ?? [];
  return child ? `${parent}.${String(child)}` : String(parent);
}

function activeLamp(path: string) {
  if (path === 'green') return 'green';
  if (path === 'yellow') return 'yellow';
  if (path.startsWith('red') || path === 'outage') return 'red';
  return 'off';
}

function pedState(path: string) {
  if (path === 'red.walk') return 'walk';
  if (path === 'red.flashing') return 'flashing';
  if (path === 'outage' || path === 'connecting') return 'off';
  return 'stop';
}

function PedSignal({ state }: { state: string }) {
  return (
    <div className={`ped-signal ped-signal--${state}`} aria-label={`Pedestrian signal: ${state}`}>
      <svg className="ped-signal__hand" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M8.5 11.5V5.25a1.25 1.25 0 0 1 2.5 0V10h1V3.25a1.25 1.25 0 0 1 2.5 0V10h1V4.25a1.25 1.25 0 0 1 2.5 0V11h1V7.25a1.25 1.25 0 0 1 2.5 0v6.9c0 3.8-2.7 6.85-6.6 6.85-3 0-4.7-1.4-6.1-3.7l-2.4-3.8a1.45 1.45 0 0 1 2.4-1.6l1.7 2.6z" />
      </svg>
      <svg className="ped-signal__walker" viewBox="0 0 24 24" aria-hidden="true">
        <path d="M13.5 5.5c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zM9.8 8.9 7 23h2.1l1.8-8 2.1 2v6h2v-7.5l-2.1-2 .6-3C14.8 12 16.8 13 19 13v-2c-1.9 0-3.5-1-4.3-2.4l-1-1.6c-.4-.6-1-1-1.7-1-.3 0-.5.1-.8.1L6 8.3V13h2V9.6l1.8-.7" />
      </svg>
    </div>
  );
}

function toVizSnapshot(snapshot: TrafficView) {
  return {
    value: snapshot.value,
    status: snapshot.status,
    context: snapshot.context,
  };
}

function ActorCard({
  id,
  selected,
  embedded,
  onSelect,
  onStopped,
}: {
  id: string;
  selected: boolean;
  embedded: EmbeddedInspector | null;
  onSelect: (id: string) => void;
  onStopped: (id: string) => void;
}) {
  const actor = useActor({ name: 'trafficLight', key: [id] });
  const [snapshot, setSnapshot] = useState<TrafficView | null>(null);
  const [pendingEvent, setPendingEvent] = useState<string | null>(null);
  const [stopping, setStopping] = useState(false);
  const [menuOpen, setMenuOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement | null>(null);
  const registeredWith = useRef<EmbeddedInspector | null>(null);

  useEffect(() => {
    if (!menuOpen) return;
    const close = (mouseEvent: MouseEvent) => {
      if (!menuRef.current?.contains(mouseEvent.target as Node)) {
        setMenuOpen(false);
      }
    };
    document.addEventListener('mousedown', close);
    return () => document.removeEventListener('mousedown', close);
  }, [menuOpen]);

  actor.useEvent('snapshot', setSnapshot);

  useEffect(() => {
    if (!actor.connection) return;
    let active = true;

    actor.connection.getSnapshot().then((nextSnapshot) => {
      if (active) setSnapshot(nextSnapshot);
    });

    return () => {
      active = false;
    };
  }, [actor.connection, actor.hash]);

  useEffect(() => {
    if (!embedded || !snapshot) return;
    if (registeredWith.current !== embedded) {
      registeredWith.current = embedded;
      embedded.inspector.actor(id, {
        machine: trafficLightVizConfig,
        snapshot: toVizSnapshot(snapshot),
      });
    } else {
      embedded.inspector.snapshot(id, toVizSnapshot(snapshot));
    }
  }, [embedded, snapshot, id]);

  const send = async (type: TrafficEvent['type']) => {
    if (!actor.connection) return;
    setPendingEvent(type);
    try {
      const nextSnapshot = await actor.connection.dispatch({ type });
      setSnapshot(nextSnapshot);
    } finally {
      setPendingEvent(null);
    }
  };

  const stop = async () => {
    if (!actor.connection) return;
    setStopping(true);
    try {
      await actor.connection.stop();
    } finally {
      onStopped(id);
    }
  };

  const path = snapshot ? statePath(snapshot.value) : 'connecting';
  const lamp = activeLamp(path);
  const outage = path === 'outage';
  const context = snapshot?.context;
  const busy = !actor.isConnected || pendingEvent !== null || stopping;

  return (
    <article
      className={`actor-card ${selected ? 'actor-card--selected' : ''}`}
      onClick={() => onSelect(id)}
      aria-live="polite"
    >
      <div className="signal-stack">
        <div className="signal" aria-label={`Traffic signal: ${lamp}`}>
          {(['red', 'yellow', 'green'] as const).map((color) => (
            <div
              className={`lamp lamp--${color} ${lamp === color ? 'lamp--active' : ''} ${
                outage && color === 'red' ? 'lamp--blink' : ''
              }`}
              key={color}
            />
          ))}
        </div>
        <PedSignal state={pedState(path)} />
      </div>

      <div className="actor-card__menu" ref={menuRef}>
        <button
          aria-haspopup="menu"
          aria-label={`Options for ${id}`}
          className="kebab-button"
          onClick={() => setMenuOpen((open) => !open)}
          type="button"
        >
          ⋮
        </button>
        {menuOpen && (
          <div className="menu" role="menu">
            <button
              className="menu__stop"
              disabled={stopping || !actor.isConnected}
              onClick={() => {
                setMenuOpen(false);
                stop();
              }}
              role="menuitem"
              type="button"
            >
              {stopping ? 'Stopping…' : 'Stop actor'}
            </button>
          </div>
        )}
      </div>

      <div className="actor-card__body">
        <div className="actor-card__heading">
          <span className="actor-card__id">{id}</span>
        </div>

        <dl className="actor-card__context">
          <div>
            <dt>Cycles</dt>
            <dd>{context?.completedCycles ?? '—'}</dd>
          </div>
          <div>
            <dt>Manual</dt>
            <dd>{context?.manualAdvances ?? '—'}</dd>
          </div>
          <div>
            <dt>Outages</dt>
            <dd>{context?.outages ?? '—'}</dd>
          </div>
          <div className="actor-card__last-change">
            <dt>Last change</dt>
            <dd>{context?.lastChange ?? '—'}</dd>
          </div>
        </dl>

        <div className="actor-card__actions">
          {(Object.keys(eventLabels) as TrafficEvent['type'][]).map(
            (eventType) => (
              <button
                className="event-button"
                disabled={busy || !snapshot?.can[eventType]}
                key={eventType}
                onClick={() => send(eventType)}
                type="button"
              >
                {eventLabels[eventType]}
              </button>
            ),
          )}
        </div>
      </div>
    </article>
  );
}

export function App() {
  const directory = useActor({ name: 'directory', key: ['main'] });
  const [actorIds, setActorIds] = useState<string[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const iframeRef = useRef<HTMLIFrameElement | null>(null);
  const [embedded, setEmbedded] = useState<EmbeddedInspector | null>(null);

  directory.useEvent('ids', setActorIds);

  useEffect(() => {
    if (!directory.connection) return;
    let active = true;

    directory.connection.list().then((ids) => {
      if (active) setActorIds(ids);
    });

    return () => {
      active = false;
    };
  }, [directory.connection, directory.hash]);

  useEffect(() => {
    const iframe = iframeRef.current;
    if (!iframe) return;
    const nextEmbedded = createEmbeddedInspector(iframe);
    setEmbedded(nextEmbedded);
    return () => {
      nextEmbedded.destroy();
      setEmbedded(null);
    };
  }, []);

  useEffect(() => {
    if (selectedId && actorIds.includes(selectedId)) return;
    setSelectedId(actorIds[0] ?? null);
  }, [actorIds, selectedId]);

  // Mirror roster removals (from any tab) into the embed's actor tree.
  const prevIdsRef = useRef<string[]>([]);
  useEffect(() => {
    if (embedded) {
      for (const id of prevIdsRef.current) {
        if (!actorIds.includes(id)) embedded.inspector.stop(id);
      }
    }
    prevIdsRef.current = actorIds;
  }, [actorIds, embedded]);

  useEffect(() => {
    if (!embedded || !selectedId) return;
    embedded.select(selectedId);
    // Late registrations (page load, spawns) re-init the embed and would
    // steal focus — re-assert the selection each time an actor joins.
    return embedded.inspector.on('actor', () => embedded.select(selectedId));
  }, [embedded, selectedId]);

  const spawn = () => {
    if (!directory.connection) return;
    directory.connection.add(nextActorId(actorIds)).then(setActorIds);
  };
  const remove = (id: string) => {
    if (!directory.connection) return;
    directory.connection.remove(id).then(setActorIds);
  };

  return (
    <main>
      <div className="layout">
        <div className="actor-pane">
          <section className="actor-list">
            {actorIds.map((id) => (
              <ActorCard
                embedded={embedded}
                id={id}
                key={id}
                onSelect={setSelectedId}
                onStopped={remove}
                selected={id === selectedId}
              />
            ))}
            {actorIds.length === 0 && (
              <p className="empty-note">No actors running.</p>
            )}
          </section>

          <button className="spawn-button" onClick={spawn} type="button">
            + Spawn actor
          </button>
        </div>

        <div className="inspector-pane">
          <iframe
            ref={iframeRef}
            src={`${STATELY_EMBED_URL}/embed`}
            title="Stately inspector"
          />
        </div>
      </div>
    </main>
  );
}
