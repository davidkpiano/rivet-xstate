import { createClient } from 'rivetkit/client';
import type { Registry } from 'rivetkit';
import { retryIdempotentRivetRequest } from './requests.ts';
import { createCafeMachine, type CafeCommand } from '../model.ts';
import { UserError } from 'rivetkit';
import { paymentLedger } from './payments.ts';

export function openCafeLogic(options: {
  endpoint: string;
  executionId: string;
  signal: AbortSignal;
  afterPaymentResponse?: (kind: 'charge' | 'refund', orderId: string) => void;
}) {
  const client = createClient<Registry<{ payments: typeof paymentLedger }>>(
    options.endpoint,
  );
  const ledger = client.payments.getOrCreate([options.executionId]);
  const pay = async (
    kind: 'charge' | 'refund',
    request: { orderId: string; amount: number },
  ) => {
    const result = await retryIdempotentRivetRequest(
      () => ledger[kind](request),
      options.signal,
    );
    options.afterPaymentResponse?.(kind, request.orderId);
    return result;
  };
  return {
    logic: createCafeMachine({
      charge: (request) => pay('charge', request),
      refund: (request) => pay('refund', request),
    }),
    dispose: () => client.dispose(),
  };
}

export async function admitCafeCommand({
  event,
  commands,
}: {
  event: CafeCommand;
  commands: { has(event: CafeCommand): Promise<boolean> };
}) {
  if (event.type === 'arrival' && (await commands.has({ type: 'close' })))
    throw new UserError('Café is closing.', { code: 'cafe_closing' });
}
