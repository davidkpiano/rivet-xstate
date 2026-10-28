import { execFileSync } from 'node:child_process';

const [storage, portText, mode] = process.argv.slice(2);
if (!storage || !portText) throw new Error('Storage path and port required.');
process.env.RIVETKIT_STORAGE_PATH = storage;
const { createCafeRegistry } = await import('../rivet/registry.ts');
const freeze = (type: string, data: unknown) => {
  process.send?.({ type, data });
  process.kill(process.pid, 'SIGSTOP');
};
const port = Number(portText);
const registry = createCafeRegistry({
  enginePort: port,
  httpPort: port + 1,
  afterCommandCommitted: (id) => {
    if (mode === 'command') freeze('fault.command', id);
  },
  afterPaymentResponse: (kind, orderId) => {
    if (mode === 'payment' && kind === 'charge')
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
await registry.startAndWait();
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
process.send?.({
  type: 'ready',
  pid: process.pid,
  enginePid: engine ? Number(engine[0]) : null,
});
process.on('message', (message) => {
  if (message === 'stop')
    void registry.shutdown().finally(() => process.exit(0));
});
