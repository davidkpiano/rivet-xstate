import { parseArgs } from 'node:util';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { createClient } from 'rivetkit/client';
import { cafeCommandSchema, cafeInputSchema } from '../model.ts';
import { viewSchema } from './registry.ts';
import { retryIdempotentRivetRequest } from './requests.ts';

const { values } = parseArgs({
  options: {
    key: { type: 'string', default: 'demo' },
    port: { type: 'string', default: '6480' },
    storage: { type: 'string', default: '.cafe/rivet' },
    endpoint: { type: 'string' },
    customers: { type: 'string', default: '6' },
    baristas: { type: 'string', default: '2' },
    'tick-ms': { type: 'string', default: '750' },
    open: { type: 'boolean', default: false },
    'no-inspect': { type: 'boolean', default: false },
    help: { type: 'boolean', default: false },
  },
});

if (values.help) {
  process.stdout.write(`Rivet durable café
pnpm cafe:rivet --open --customers 6 --baristas 2
Options: --key NAME --port PORT --storage PATH --endpoint URL
         --customers N --baristas N --tick-ms N --open --no-inspect
Commands: status | receipt REQUEST_ID | arrive latte | arrive espresso | cancel order-1
          break espresso | repair espresso | close | crash | quit
Restart with the same key and storage to resume. Use another key for a new café.
`);
} else {
  await main();
}

async function main() {
  process.env.RIVETKIT_STORAGE_PATH = resolve(values.storage);
  const port = Number(values.port);
  if (!Number.isInteger(port) || port < 1024 || port >= 65535)
    throw new Error('--port must be between 1024 and 65534.');
  const config = cafeInputSchema.parse({
    customers: Number(values.customers),
    baristas: Number(values.baristas),
    tickMs: Number(values['tick-ms']),
  });
  const endpoint = values.endpoint ?? `http://127.0.0.1:${port}`;
  const { createCafeRegistry } = await import('./registry.ts');
  const registry = createCafeRegistry({
    endpoint: values.endpoint,
    enginePort: port,
    httpPort: port + 1,
  });
  await registry.startAndWait();
  const client = createClient<typeof registry>(endpoint);
  const handle = client.cafe.getOrCreate([values.key], {
    createWithInput: { config, inspect: !values['no-inspect'] },
  });
  const info = await retryIdempotentRivetRequest(() => handle.info());
  process.stdout.write(
    `Rivet café '${values.key}' · ${endpoint}\nStorage ${resolve(values.storage)}\n`,
  );
  process.stdout.write(`Saved input ${JSON.stringify(info.input)}\n`);
  const repl = createInterface({ input: process.stdin, terminal: false });
  let last = '';
  let shownInspector = false;
  let polling = false;
  let stopping = false;
  const poller = setInterval(() => {
    void poll();
  }, 500);
  async function poll() {
    if (polling || stopping) return;
    polling = true;
    try {
      const view = await handle.getSnapshot();
      if (!view) return;
      if (view.inspectorUrl && !shownInspector) {
        shownInspector = true;
        process.stdout.write(`Inspector ${view.inspectorUrl}\n`);
        if (values.open) {
          const { spawn } = await import('node:child_process');
          if (process.platform === 'darwin')
            spawn('open', [view.inspectorUrl], { stdio: 'ignore' }).unref();
          else if (process.platform === 'linux')
            spawn('xdg-open', [view.inspectorUrl], { stdio: 'ignore' }).unref();
        }
      }
      const root = view.actors.find((entry) => entry.id === 'cafe');
      const context = root?.context;
      const counts =
        context && typeof context === 'object'
          ? Object.entries(context).filter(([key]) =>
              ['served', 'cancelled', 'refunded', 'left', 'failed'].includes(
                key,
              ),
            )
          : [];
      const staff = view.actors
        .filter((entry) => entry.id.startsWith('barista-'))
        .map((entry) => `${entry.id}=${JSON.stringify(entry.value)}`);
      const signature = JSON.stringify([counts, staff]);
      if (signature !== last) {
        last = signature;
        process.stdout.write(
          `${counts.map(([key, value]) => `${key} ${value}`).join(' · ')} | ${staff.join(' · ')}\n`,
        );
      }
    } catch (error) {
      process.stderr.write(`${String(error)}\n`);
    } finally {
      polling = false;
    }
  }
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    clearInterval(poller);
    repl.close();
    await client.dispose();
    await registry.shutdown();
    process.exit(0);
  };
  repl.on('line', (line) => {
    void (async () => {
      const [command, argument] = line.trim().split(/\s+/);
      if (!command) return;
      if (command === 'crash') {
        process.kill(process.pid, 'SIGKILL');
        return;
      }
      if (command === 'quit') {
        await shutdown();
        return;
      }
      if (command === 'status') {
        process.stdout.write(
          `${JSON.stringify(viewSchema.nullable().parse(await handle.getSnapshot()))}\n`,
        );
        return;
      }
      if (command === 'receipt') {
        process.stdout.write(
          `${JSON.stringify(await handle.getCommand(argument ?? ''))}\n`,
        );
        return;
      }
      const raw: unknown = line.trim().startsWith('{')
        ? JSON.parse(line)
        : command === 'arrive'
          ? { type: 'arrival', drink: argument ?? 'latte' }
          : command === 'cancel'
            ? { type: 'cancel', orderId: argument }
            : command === 'break' || command === 'repair'
              ? { type: `equipment.${command}`, equipment: argument }
              : { type: command };
      const event = cafeCommandSchema.parse(raw);
      const requestId = randomUUID();
      process.stdout.write(
        `${JSON.stringify(await retryIdempotentRivetRequest(() => handle.dispatch(event, requestId)))}\n`,
      );
    })().catch((error: unknown) =>
      process.stderr.write(`Command: ${String(error)}\n`),
    );
  });
  process.once('SIGINT', () => {
    void shutdown();
  });
  process.once('SIGTERM', () => {
    void shutdown();
  });
  await poll();
  process.stdout.write(
    'Commands: status, receipt REQUEST_ID, arrive latte, break espresso, repair espresso, crash, quit.\n',
  );
}
