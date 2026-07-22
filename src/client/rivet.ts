import { createRivetKit } from '@rivetkit/react';
import type { registry } from '../server/registry';

export const { useActor } = createRivetKit<typeof registry>(
  import.meta.env.VITE_RIVET_ENDPOINT ?? 'http://localhost:6420',
);
