import { openCafe } from '../run.ts';
import { cafeInputSchema } from '../model.ts';

const [database, mode] = process.argv.slice(2);
if (!database) throw new Error('Database path required.');
const cafe = await openCafe({
  database,
  input: cafeInputSchema.parse({ customers: 1, baristas: 1, tickMs: 10 }),
  inspect: false,
  now: () => 1000,
  onPaymentCommitted: (kind) => {
    if (mode === 'charge' && kind === 'charge') {
      process.send?.({ type: 'charge.committed' });
      process.kill(process.pid, 'SIGSTOP');
    }
  },
  onTurn: (host) => {
    if (
      mode === 'brew' &&
      host
        .view()
        .actors.some(
          (actor) => JSON.stringify(actor.value) === '{"working":"extracting"}',
        )
    ) {
      process.send?.({ type: 'brewing', timers: host.view().timers });
      process.kill(process.pid, 'SIGSTOP');
    }
  },
});
await cafe.host.tick(10000);
process.send?.({ type: 'unexpected.completion' });
await cafe.host.close();
