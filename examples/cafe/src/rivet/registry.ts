import { actor, queue, setup } from 'rivetkit';
import {
  xstate,
  type DurableActorView,
  type Delivery,
} from '@demo/xstate-durable/rivet';
import type { Transport } from '@statelyai/sdk/inspect';
import { z } from 'zod';
import {
  cafeInputSchema,
  cafeCommandSchema,
  type CafeCommand,
} from '../model.ts';
import { paymentLedger } from './payments.ts';
import { openCafeLogic, admitCafeCommand } from './services.ts';
import { commandInbox, type Command, type Metadata } from './commands.ts';
export { commandReceiptSchema, viewSchema } from './commands.ts';
export type RivetCafeView = DurableActorView;
type Input = z.infer<typeof cafeInputSchema>;
type CafeHostOptions = {
  afterCommandCommitted?: (id: string) => void;
  beforeEnqueue?: (id: string) => void;
  onView?: (view: DurableActorView) => void;
  onDelivery?: (delivery: Delivery) => void;
  beforePublish?: (view: DurableActorView) => Promise<void>;
  now?: () => number;
};
export function createCafeRegistry(
  options: CafeHostOptions & {
    endpoint?: string;
    inspectionTransport?: () => Transport;
    enginePort?: number;
    httpPort?: number;
    afterPaymentResponse?: (kind: 'charge' | 'refund', orderId: string) => void;
  } = {},
) {
  const endpoint =
    options.endpoint ?? `http://127.0.0.1:${options.enginePort ?? 6480}`;
  const commands = commandInbox<Input, typeof cafeCommandSchema>({
    ...options,
    eventSchema: cafeCommandSchema,
    admission: admitCafeCommand,
    terminal: {
      code: 'cafe_closed',
      message: 'Café is closed.',
      rejectedMessage: 'Command rejected because the café closed.',
    },
  });
  const cafeRun = xstate(
    (context) =>
      openCafeLogic({
        ...context,
        endpoint,
        afterPaymentResponse: options.afterPaymentResponse,
      }),
    {
      queue: 'events',
      input: (c: { state: Metadata<Input> }) => c.state.input,
      mapEvent: (body: Command<CafeCommand>) => ({
        event: body.event,
        id: body.id,
      }),
      inspect: {
        name: 'Rivet durable café',
        transport: options.inspectionTransport,
        enabled: (c) => c.state.inspect,
      },
      beforePublish: options.beforePublish,
      onDelivery: options.onDelivery,
      now: options.now,
      onSnapshot: async (c, view, delivery) => {
        await commands.publish(c, view, delivery);
        options.onView?.(view);
      },
    },
  );
  const cafe = actor({
    options: { name: 'Durable XState café', icon: 'coffee' },
    createState: (
      _c,
      input: { config: z.input<typeof cafeInputSchema>; inspect?: boolean },
    ): Metadata<Input> => ({
      input: cafeInputSchema.parse(input.config),
      inspect: input.inspect ?? true,
      executionId: _c.actorId,
      startedAt: (options.now ?? Date.now)(),
    }),
    db: commands.database,
    queues: { events: queue<Command<CafeCommand>>() },
    run: cafeRun,
    actions: commands.actions,
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
