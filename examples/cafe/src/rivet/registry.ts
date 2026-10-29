import { setup } from 'rivetkit';
import {
  durableRivetActor,
  type DurableRivetActorOptions,
  type DurableActorView,
} from '@demo/xstate-durable/rivet';
import {
  cafeInputSchema,
  cafeCommandSchema,
  createCafeMachine,
} from '../model.ts';
import { paymentLedger } from './payments.ts';
import { openCafeLogic, admitCafeCommand } from './services.ts';

export { commandReceiptSchema, viewSchema } from '@demo/xstate-durable/rivet';
export type RivetCafeView = DurableActorView;

type CafeHostOptions = Pick<
  DurableRivetActorOptions<ReturnType<typeof createCafeMachine>>,
  'afterCommandCommitted' | 'onView' | 'onDelivery' | 'beforePublish' | 'now'
>;
export function createCafeRegistry(
  options: CafeHostOptions & {
    endpoint?: string;
    inspectionTransport?: NonNullable<
      DurableRivetActorOptions<
        ReturnType<typeof createCafeMachine>
      >['inspection']
    >['transport'];
    enginePort?: number;
    httpPort?: number;
    afterPaymentResponse?: (kind: 'charge' | 'refund', orderId: string) => void;
  } = {},
) {
  const endpoint =
    options.endpoint ?? `http://127.0.0.1:${options.enginePort ?? 6480}`;
  const cafe = durableRivetActor({
    ...options,
    name: 'Durable XState café',
    icon: 'coffee',
    historyVersion: 'rivet-cafe-1',
    logic: (context) =>
      openCafeLogic({
        ...context,
        endpoint,
        afterPaymentResponse: options.afterPaymentResponse,
      }),
    inputSchema: cafeInputSchema,
    eventSchema: cafeCommandSchema,
    admission: admitCafeCommand,
    terminal: {
      code: 'cafe_closed',
      message: 'Café is closed.',
      rejectedMessage: 'Command rejected because the café closed.',
    },
    inspection: {
      name: 'Rivet durable café',
      transport: options.inspectionTransport,
    },
  });
  return setup({
    use: { cafe, payments: paymentLedger },
    ...(options.endpoint
      ? { endpoint, startEngine: false }
      : { startEngine: true, enginePort: options.enginePort ?? 6480 }),
    httpPort: options.httpPort ?? 6481,
    noWelcome: true,
    startServices: false,
  });
}
