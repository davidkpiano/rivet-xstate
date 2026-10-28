import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { expect, test } from 'vitest';
import { z } from 'zod';
import { LocalDurableHost } from './local-host.ts';
import { cafeInputSchema, createCafeMachine } from './model.ts';
import { CafeStore } from './store.ts';

const fixtureSchema = z.object({
  sourceRevision: z.literal('3e5482714377dd28d58ff0c861aa5f0ea08633f5'),
  input: cafeInputSchema,
  tables: z.record(
    z.enum(['metadata', 'events', 'timers', 'results', 'payments']),
    z.array(z.record(z.string(), z.union([z.string(), z.number(), z.null()]))),
  ),
  expectedRoot: z.unknown(),
});

test('saved pre-refactor history resumes with unchanged identities, memo keys and timer deadlines', async () => {
  const fixture = fixtureSchema.parse(
    JSON.parse(
      readFileSync(
        new URL('./fixtures/pre-refactor-local.json', import.meta.url),
        'utf8',
      ),
    ),
  );
  const directory = mkdtempSync(join(tmpdir(), 'cafe-compatibility-'));
  const store = new CafeStore(join(directory, 'cafe.sqlite'));
  try {
    for (const [table, rows] of Object.entries(fixture.tables)) {
      for (const row of rows) {
        const columns = Object.keys(row);
        store.db
          .prepare(
            `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${columns.map(() => '?').join(', ')})`,
          )
          .run(...Object.values(row));
      }
    }
    const originalResults = store.db
      .prepare('SELECT * FROM results ORDER BY key')
      .all();
    const originalRun = store.run;
    let serviceCalls = 0;
    const logic = createCafeMachine({
      charge: async (request) => {
        serviceCalls++;
        return store.payment('charge', request);
      },
      refund: async (request) => {
        serviceCalls++;
        return store.payment('refund', request);
      },
    });
    const host = new LocalDurableHost({
      logic,
      input: fixture.input,
      store,
      now: () => 10_000,
    });
    await host.start();
    expect(store.run).toEqual(originalRun);
    expect(
      store.db.prepare('SELECT * FROM results ORDER BY key').all(),
    ).toEqual(originalResults);
    expect(serviceCalls).toBe(0);
    const baristaTimer = host
      .view()
      .timers.find((timer) => timer.address === 'cafe/barista-1');
    expect(baristaTimer).toMatchObject({ due_at: 1100 });
    await host.tick();
    const context = host
      .view()
      .actors.find((actor) => actor.id === 'cafe')?.context;
    const normalizedContext: unknown = JSON.parse(
      JSON.stringify(context, (_key, value: unknown) => {
        if (
          value !== null &&
          typeof value === 'object' &&
          'actorId' in value &&
          'address' in value
        )
          return { $actor: value.address };
        return value;
      }),
    );
    expect(normalizedContext).toEqual(fixture.expectedRoot);
    expect(store.payments()).toHaveLength(3);
    expect(serviceCalls).toBe(0);
  } finally {
    store.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
