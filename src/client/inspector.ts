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
    launch: 'none',
    theme: 'dark',
    // Panels start closed (still openable from the embed's toolbar).
    panels: [{ id: 'inspection', initiallyOpen: false }],
  });

  return {
    inspector,
    select(actorId) {
      inspector.selectActor(`${inspector.producerId}:${actorId}`);
    },
    destroy() {
      inspector.destroy();
      transport.destroy();
    },
  };
}
