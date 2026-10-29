import { z } from 'zod';
import type { RawAccess } from 'rivetkit/db';

export async function checkProtocol(
  database: RawAccess,
  version: string,
): Promise<void> {
  await database.transaction(
    async (transaction) => {
      await transaction.execute(
        'CREATE TABLE IF NOT EXISTS _xstate_rivet_protocol (id INTEGER PRIMARY KEY CHECK(id=1), version TEXT NOT NULL)',
      );
      const rows = await transaction.execute(
        'SELECT version FROM _xstate_rivet_protocol WHERE id=1',
      );
      if (rows[0]) {
        if (
          z.object({ version: z.string() }).parse(rows[0]).version !== version
        )
          throw new Error(
            'Incompatible XState Rivet history. Use a new actor key.',
          );
        return;
      }
      const tables = await transaction.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='_rivet_wf_kv'",
      );
      if (
        tables.length &&
        (await transaction.execute('SELECT key FROM _rivet_wf_kv LIMIT 1'))
          .length
      )
        throw new Error(
          'Existing Rivet workflow history has no XState protocol marker. Use a new actor key.',
        );
      await transaction.execute(
        'INSERT INTO _xstate_rivet_protocol VALUES(1,?)',
        version,
      );
    },
    { name: 'xstate-protocol' },
  );
}
