import { parseArgs } from 'node:util';
import { resolve } from 'node:path';
import { createInterface } from 'node:readline';
import { cafeCommandSchema, cafeInputSchema } from './model.ts';
import { openCafe } from './run.ts';

const { values } = parseArgs({
  options: {
    db: { type: 'string', default: '.cafe/cafe.sqlite' },
    customers: { type: 'string' },
    baristas: { type: 'string' },
    'tick-ms': { type: 'string' },
    'patience-ticks': { type: 'string' },
    'no-inspect': { type: 'boolean', default: false },
    open: { type: 'boolean', default: false },
    json: { type: 'boolean', default: false },
    'run-for': { type: 'string' },
    help: { type: 'boolean', default: false },
  },
});

const help = `Durable café · XState alpha.70

pnpm start --open --customers 6 --baristas 2
pnpm start --db .cafe/another.sqlite --tick-ms 250

Commands: status | arrive espresso | arrive latte | cancel order-1
          break grinder | repair grinder | close | crash | quit
Events may also be entered as JSON. Reuse --db to resume after a crash.
Options: --db PATH --customers N --baristas N --tick-ms N --patience-ticks N
         --open --no-inspect --json --run-for MILLISECONDS --help
`;

if (values.help) {
  process.stdout.write(help);
} else {
  await main();
}

async function main() {
  const inputProvided =
    values.customers !== undefined ||
    values.baristas !== undefined ||
    values['tick-ms'] !== undefined ||
    values['patience-ticks'] !== undefined;
  const input = inputProvided
    ? cafeInputSchema.parse({
        customers: Number(values.customers ?? 6),
        baristas: Number(values.baristas ?? 2),
        tickMs: Number(values['tick-ms'] ?? 750),
        patienceTicks: Number(values['patience-ticks'] ?? 40),
      })
    : undefined;
  let lastState = '';
  let started = false;
  const emit = (value: unknown) =>
    process.stdout.write(`${JSON.stringify(value)}\n`);
  const cafe = await openCafe({
    database: resolve(values.db),
    input,
    inspect: !values['no-inspect'],
    openInspector: values.open,
    onTurn: (host) => {
      if (!started) return;
      const view = host.view();
      const summary = view.actors.find((actor) => actor.id === 'cafe');
      const context = summary?.context;
      const counts =
        context && typeof context === 'object'
          ? Object.entries(context).filter(([key]) =>
              ['served', 'cancelled', 'refunded', 'left', 'failed'].includes(
                key,
              ),
            )
          : [];
      const staff = view.actors
        .filter((actor) => actor.id.startsWith('barista-'))
        .map((actor) => `${actor.id}=${JSON.stringify(actor.value)}`);
      const signature = values.json
        ? JSON.stringify(summary?.context)
        : JSON.stringify([counts, staff]);
      if (lastState === signature) return;
      lastState = signature;
      if (values.json) emit({ type: 'state', ...view });
      else
        process.stdout.write(
          `${counts.map(([key, value]) => `${key} ${value}`).join(' · ')} | ${staff.join(' · ')}\n`,
        );
    },
  });
  started = true;
  if (values.json)
    emit({
      type: 'ready',
      pid: process.pid,
      inspectorUrl: cafe.inspector?.inspectorUrl,
      input: cafe.input,
      ...cafe.host.view(),
    });
  else {
    process.stdout.write(`Café running. Database ${resolve(values.db)}\n`);
    if (cafe.inspector?.inspectorUrl)
      process.stdout.write(`Inspector ${cafe.inspector.inspectorUrl}\n`);
    process.stdout.write(
      'Type status, arrive latte, break espresso, repair espresso, crash, or quit.\n',
    );
  }
  cafe.inspector?.on('error', ({ message }) =>
    process.stderr.write(`Inspector: ${message}\n`),
  );
  cafe.inspector?.on('connected', () => {
    if (values.json) emit({ type: 'inspector.connected' });
  });
  const repl = createInterface({ input: process.stdin, terminal: false });
  let stopping = false;
  let runTimeout: NodeJS.Timeout | undefined;
  const shutdown = async () => {
    if (stopping) return;
    stopping = true;
    clearInterval(poller);
    clearTimeout(runTimeout);
    repl.close();
    await cafe.host.close();
    cafe.inspector?.destroy();
  };
  const fail = (error: unknown) => {
    process.stderr.write(
      `${error instanceof Error ? error.message : String(error)}\n`,
    );
    process.exitCode = 1;
    void shutdown();
  };
  const poller = setInterval(
    () => {
      void cafe.host.tick().catch(fail);
    },
    Math.min(cafe.input.tickMs, 100),
  );
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
      if (command === 'help') {
        process.stdout.write(help);
        return;
      }
      if (command === 'status') {
        emit({ type: 'status', ...cafe.host.view() });
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
      const parsed = cafeCommandSchema.parse(raw);
      await cafe.host.send(parsed);
    })().catch((error: unknown) => {
      process.stderr.write(
        `Command: ${error instanceof Error ? error.message : String(error)}\n`,
      );
    });
  });
  process.once('SIGINT', () => {
    void shutdown();
  });
  process.once('SIGTERM', () => {
    void shutdown();
  });
  if (values['run-for']) {
    const milliseconds = Number(values['run-for']);
    if (!Number.isFinite(milliseconds) || milliseconds <= 0)
      fail(new Error('--run-for must be a positive number.'));
    else
      runTimeout = setTimeout(() => {
        void shutdown();
      }, milliseconds);
  }
}
