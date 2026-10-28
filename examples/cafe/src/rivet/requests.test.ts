import { RivetError, UserError } from 'rivetkit/client';
import { expect, test, vi } from 'vitest';
import { retryIdempotentRivetRequest } from './requests.ts';
import { isTransportFailure } from './requests.ts';

test('retries a stale gateway route using the same operation', async () => {
  const request = vi
    .fn()
    .mockRejectedValueOnce(
      new RivetError('core', 'internal_error', 'Stale route', {
        statusCode: 500,
      }),
    )
    .mockResolvedValue({ receiptId: 'charge:order-1' });
  await expect(retryIdempotentRivetRequest(request)).resolves.toEqual({
    receiptId: 'charge:order-1',
  });
  expect(request).toHaveBeenCalledTimes(2);
});

test('payment rejection is returned without retry', async () => {
  const error = new UserError('Conflicting charge', {
    code: 'conflicting_payment',
  });
  const request = vi.fn().mockRejectedValue(error);
  await expect(retryIdempotentRivetRequest(request)).rejects.toBe(error);
  expect(request).toHaveBeenCalledTimes(1);
});

test('SDK transport failures remain infrastructure failures', () => {
  expect(
    isTransportFailure(
      new RivetError('rivetkit', 'internal_error', 'Connection lost', {
        statusCode: 500,
      }),
    ),
  ).toBe(true);
  expect(
    isTransportFailure(
      new RivetError('auth', 'forbidden', 'Access denied', { statusCode: 403 }),
    ),
  ).toBe(true);
  expect(isTransportFailure(new UserError('Payment declined'))).toBe(false);
});
