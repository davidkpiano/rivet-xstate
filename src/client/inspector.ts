import {
  createInspector,
  createPostMessageTransport,
  type Inspector,
} from '@statelyai/sdk';

export const STATELY_EMBED_URL: string =
  import.meta.env.VITE_STATELY_EMBED_URL ?? 'http://localhost:3000';

export type EmbeddedInspector = {
  inspector: Inspector;
  /** Focus the given actor in the embed. */
  select(actorId: string): void;
  destroy(): void;
};

export function createEmbeddedInspector(
  iframe: HTMLIFrameElement,
): EmbeddedInspector {
  const transport = createPostMessageTransport({ iframe, targetOrigin: '*' });
  const inspector = createInspector({
    transport,
    autoOpen: false,
    theme: 'dark',
    // Panels start closed (still openable from the embed's toolbar).
    panels: { activePanels: [] },
  });

  let pendingSelect: string | null = null;

  function sendSelect(actorId: string) {
    transport.send({ type: '@statelyai.system.selectActor', actorId });
  }

  // Registered after createInspector so the inspector's own queued init
  // flushes first; selection then lands on a populated store.
  transport.onReady(() => {
    if (pendingSelect) sendSelect(pendingSelect);
  });

  return {
    inspector,
    select(actorId) {
      pendingSelect = actorId;
      if (transport.ready) sendSelect(actorId);
    },
    destroy() {
      inspector.destroy();
      transport.destroy();
    },
  };
}
