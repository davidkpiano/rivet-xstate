import { execFileSync } from 'node:child_process';
import { z } from 'zod';

const [storage, portText, mode] = process.argv.slice(2);
if (!storage || !portText) throw new Error('Storage path and port required.');
process.env.RIVETKIT_STORAGE_PATH = storage;
const { createCafeRegistry } = await import('../rivet/registry.ts');
const freeze = (type: string, data: unknown) => {
  process.send?.({ type, data });
  process.kill(process.pid, 'SIGSTOP');
};
const port = Number(portText);
let virtualNow = Date.now();
let gated = false;
let publish: (() => void) | undefined;
let reconnectInspection: (() => void) | undefined;
const registry = createCafeRegistry({
  enginePort: port,
  ...(mode === 'inspection'
    ? {
        inspectionTransport: () => {
          process.send?.({ type: 'inspection.open' });
          return {
            ready: true,
            send: (message: unknown) =>
              process.send?.({ type: 'inspection.message', data: message }),
            onMessage: () => () => {},
            onReady: (handler) => {
              reconnectInspection = handler;
              queueMicrotask(handler);
              return () => {
                reconnectInspection = undefined;
              };
            },
            destroy: () => {
              process.send?.({ type: 'inspection.close' });
            },
          };
        },
      }
    : {}),
  httpPort: port + 1,
  ...(mode === 'backlog' ? { now: () => virtualNow } : {}),
  beforePublish: async (view) => {
    if (
      !gated &&
      ((mode === 'closing' && view.status === 'done') || mode === 'backlog')
    ) {
      gated = true;
      await new Promise<void>((resolve) => {
        publish = resolve;
        process.send?.({ type: 'fault.beforePublish', data: view });
      });
    }
  },
  beforeEnqueue: (id) => {
    if (mode === 'submission') freeze('fault.submission', id);
  },
  afterCommandCommitted: (id) => {
    if (mode === 'command') freeze('fault.command', id);
  },
  afterPaymentResponse: (kind, orderId) => {
    if (
      (mode === 'payment' && kind === 'charge') ||
      (mode === 'refund' && kind === 'refund')
    )
      freeze('fault.payment', orderId);
  },
  onView: (view) => {
    process.send?.({ type: 'view', data: view });
    if (
      mode === 'brew' &&
      view.actors.some(
        (entry) => JSON.stringify(entry.value) === '{"working":"extracting"}',
      )
    )
      freeze('fault.brew', view);
  },
  onDelivery: (delivery) => {
    process.send?.({ type: 'delivery', data: delivery });
  },
});
function enginePid(): number | null {
  const children = execFileSync('ps', ['-axo', 'pid=,ppid=,comm='], {
    encoding: 'utf8',
  })
    .trim()
    .split('\n');
  const engine = children
    .map((line) => line.trim().split(/\s+/))
    .find(
      (parts) =>
        Number(parts[1]) === process.pid &&
        parts.slice(2).join(' ').includes('rivet-engine'),
    );
  return engine ? Number(engine[0]) : null;
}
const startupMonitor = setInterval(() => {
  const pid = enginePid();
  if (pid) {
    process.send?.({ type: 'engine', enginePid: pid });
    clearInterval(startupMonitor);
  }
}, 1000);
try {
  await registry.startAndWait();
} finally {
  clearInterval(startupMonitor);
}
process.send?.({
  type: 'ready',
  pid: process.pid,
  enginePid: enginePid(),
});
process.on('message', (message) => {
  if (message === 'publish') publish?.();
  if (message === 'inspection.reconnect') reconnectInspection?.();
  if (
    message &&
    typeof message === 'object' &&
    'type' in message &&
    message.type === 'clock'
  ) {
    virtualNow = z.object({ at: z.number() }).parse(message).at;
    process.send?.({ type: 'clock.set', at: virtualNow });
  }
  if (message === 'stop')
    void registry.shutdown().finally(() => process.exit(0));
});
