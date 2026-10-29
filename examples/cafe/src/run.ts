import { createInspector, type Inspector } from '@statelyai/sdk/inspect';
import { createCafeMachine, cafeInputSchema, type CafeInput } from './model.ts';
import { LocalDurableHost } from './local-host.ts';
import { serializable } from '@demo/xstate-durable';
import { CafeStore } from './store.ts';

export async function openCafe(options: {
  database: string;
  input?: CafeInput;
  inspect?: boolean;
  openInspector?: boolean;
  now?: () => number;
  onTurn?: (host: LocalDurableHost) => void;
  onPaymentCommitted?: (kind: 'charge' | 'refund', orderId: string) => void;
}) {
  const store = new CafeStore(options.database);
  let inspector: Inspector | undefined;
  try {
    const input = cafeInputSchema.parse(
      options.input ??
        store.run?.input ?? { customers: 6, baristas: 2, tickMs: 750 },
    );
    const pay = async (
      kind: 'charge' | 'refund',
      request: { orderId: string; amount: number },
    ) => {
      const receipt = store.payment(kind, request);
      options.onPaymentCommitted?.(kind, request.orderId);
      return receipt;
    };
    const machine = createCafeMachine({
      charge: (request) => pay('charge', request),
      refund: (request) => pay('refund', request),
    });
    const host = new LocalDurableHost({
      logic: machine,
      input,
      store,
      now: options.now,
      onTurn: options.onTurn,
      inspect: (event) => inspector?.inspect(event),
    });
    const run = store.run;
    if (!run) throw new Error('Run metadata was not saved.');
    if (options.inspect !== false) {
      inspector = createInspector({
        name: 'Durable café',
        roomId: run.roomId,
        producerId: run.producerId,
        launch: options.openInspector ? 'browser' : 'none',
        bidirectional: false,
        sessionIdReuse: 'always',
        serializeSnapshot: (snapshot) =>
          serializable({
            value: snapshot.value,
            status: snapshot.status,
            context: snapshot.context,
            tags: snapshot.tags ? [...snapshot.tags] : [],
          }),
      });
      void inspector.ready.catch(() => {});
    }
    await host.start();
    return { host, store, inspector, input };
  } catch (error) {
    inspector?.destroy();
    store.close();
    throw error;
  }
}
