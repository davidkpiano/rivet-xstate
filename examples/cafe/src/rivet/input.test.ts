import { expect, test } from 'vitest';
import { snapshotInput } from '../../../../packages/durable/src/rivet/input.ts';

test('journal input detaches nested actor-state proxies and preserves CBOR values and domain identifiers', () => {
  const input = {
    nested: { count: 1 },
    at: new Date('2026-10-29T00:00:00Z'),
    large: 9007199254740995n,
    id: 'document',
    sessionId: 'domain-session',
  };
  const snapshot = snapshotInput(new Proxy(input, {}));
  input.nested.count = 2;
  input.at.setUTCFullYear(2030);
  expect(snapshot).toEqual({
    nested: { count: 1 },
    at: new Date('2026-10-29T00:00:00Z'),
    large: 9007199254740995n,
    id: 'document',
    sessionId: 'domain-session',
  });
});
