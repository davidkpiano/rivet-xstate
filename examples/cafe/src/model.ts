import { createAsyncLogic, setup, types } from 'xstate';
import { z } from 'zod';

const equipmentSchema = z.enum(['grinder', 'espresso', 'frother', 'terminal']);
const drinkSchema = z.enum(['espresso', 'latte']);
export const cafeInputSchema = z.object({
  customers: z.number().int().min(0).max(1000),
  baristas: z.number().int().min(1).max(100),
  tickMs: z.number().int().positive(),
  arrivalTicks: z.number().int().positive().default(2),
  patienceTicks: z.number().int().positive().default(40),
  breakAfterTicks: z.number().int().nonnegative().default(0),
  repairTicks: z.number().int().positive().default(8)
});
export type CafeInput = z.infer<typeof cafeInputSchema>;
export const cafeCommandSchema = z.discriminatedUnion('type', [
  z.object({ type: z.literal('arrival'), drink: drinkSchema, patienceTicks: z.number().int().positive().optional() }),
  z.object({ type: z.literal('cancel'), orderId: z.string().min(1) }),
  z.object({ type: z.literal('equipment.break'), equipment: equipmentSchema }),
  z.object({ type: z.literal('equipment.repair'), equipment: equipmentSchema }),
  z.object({ type: z.literal('close') })
]);
export type CafeCommand = z.infer<typeof cafeCommandSchema>;
export interface CafeServices {
  charge(request: { orderId: string; amount: number }): Promise<{ receiptId: string }>;
  refund(request: { orderId: string; amount: number }): Promise<{ receiptId: string }>;
}
type Equipment = z.infer<typeof equipmentSchema>;
type Drink = z.infer<typeof drinkSchema>;
type Mailbox<T> = { send(event: T): void };
type ResourceReply = { type: 'resource.granted' } | { type: 'resource.paused' };
type Reservation = { ownerId: string; replyTo: Mailbox<ResourceReply> };
type ResourceEvent = ({ type: 'request' } & Reservation) | { type: 'release'; ownerId: string } | { type: 'break' } | { type: 'repair' };
type ResourceContext = { equipment: Equipment; holder: Reservation | null; queue: Reservation[] };

const equipmentMachine = setup({
  schemas: {
    input: types<{ equipment: Equipment }>(),
    context: types<ResourceContext>(),
    events: {
      request: types<Reservation>(), release: types<{ ownerId: string }>(),
      break: types<{}>(), repair: types<{}>()
    }
  }
}).createMachine({
  id: 'equipment',
  context: ({ input }) => ({ ...input, holder: null, queue: [] }),
  initial: 'available',
  on: {
    break: {}, repair: {},
    request: ({ context, event }) => ({ context: {
      queue: context.holder?.ownerId === event.ownerId || context.queue.some((entry) => entry.ownerId === event.ownerId)
        ? context.queue : [...context.queue, { ownerId: event.ownerId, replyTo: event.replyTo }]
    } })
  },
  states: {
    available: {
      always: ({ context }, enq) => {
        const [next, ...queue] = context.queue;
        if (!next) return;
        enq.sendTo(next.replyTo, { type: 'resource.granted' });
        return { target: 'reserved', context: { holder: next, queue } };
      },
      on: { break: { target: 'broken' }, release: ({ context, event }) => ({ context: { queue: context.queue.filter((entry) => entry.ownerId !== event.ownerId) } }) }
    },
    reserved: {
      on: {
        release: ({ context, event }) => event.ownerId === context.holder?.ownerId
          ? { target: 'available', context: { holder: null } }
          : { context: { queue: context.queue.filter((entry) => entry.ownerId !== event.ownerId) } },
        break: ({ context }, enq) => {
          enq.sendTo(context.holder?.replyTo, { type: 'resource.paused' });
          return { target: 'broken' };
        }
      }
    },
    broken: {
      on: {
        release: ({ context, event }) => ({ context: {
          holder: event.ownerId === context.holder?.ownerId ? null : context.holder,
          queue: context.queue.filter((entry) => entry.ownerId !== event.ownerId)
        } }),
        repair: ({ context }, enq) => {
          if (context.holder) {
            enq.sendTo(context.holder.replyTo, { type: 'resource.granted' });
            return { target: 'reserved' };
          }
          return { target: 'available' };
        }
      }
    }
  }
});

type PaymentInput = { orderId: string; amount: number; operation: 'charge' | 'refund'; terminal: Mailbox<ResourceEvent>; tickMs: number };
function createPaymentMachine(services: CafeServices) {
  const charge = createAsyncLogic({
    schemas: { input: types<{ orderId: string; amount: number }>() },
    run: ({ input }) => services.charge(input)
  });
  const refund = createAsyncLogic({
    schemas: { input: types<{ orderId: string; amount: number }>() },
    run: ({ input }) => services.refund(input)
  });
  return setup({
    schemas: {
      input: types<PaymentInput>(),
      context: types<PaymentInput & { receiptId: string | null; succeeded: boolean }>(),
      output: types<{ receiptId: string | null; succeeded: boolean }>(),
      events: { 'resource.granted': types<{}>(), 'resource.paused': types<{}>() }
    },
    actors: { charge, refund },
    delays: { tap: ({ context }) => context.tickMs * 2 }
  }).createMachine({
    id: 'payment',
    context: ({ input }) => ({ ...input, receiptId: null, succeeded: false }),
    initial: 'waitingForTerminal',
    entry: ({ context, self }, enq) => enq.sendTo(context.terminal, { type: 'request', ownerId: `${context.orderId}-${context.operation}`, replyTo: self }),
    exit: ({ context }, enq) => enq.sendTo(context.terminal, { type: 'release', ownerId: `${context.orderId}-${context.operation}` }),
    output: ({ context }) => ({ receiptId: context.receiptId, succeeded: context.succeeded }),
    states: {
      waitingForTerminal: { on: { 'resource.granted': { target: 'tapping' } } },
      tapping: {
        after: { tap: ({ context }) => ({ target: context.operation === 'charge' ? 'charging' : 'refunding' }) },
        on: { 'resource.paused': { target: 'waitingForTerminal' } }
      },
      charging: {
        invoke: { id: 'charge', src: 'charge', input: ({ context }) => ({ orderId: context.orderId, amount: context.amount }),
          onDone: ({ event }) => ({ target: 'complete', context: { receiptId: event.output.receiptId, succeeded: true } }),
          onError: { target: 'complete' }
        }
      },
      refunding: {
        invoke: { id: 'refund', src: 'refund', input: ({ context }) => ({ orderId: context.orderId, amount: context.amount }),
          onDone: ({ event }) => ({ target: 'complete', context: { receiptId: event.output.receiptId, succeeded: true } }),
          onError: { target: 'complete' }
        }
      },
      complete: { type: 'final' }
    }
  });
}

type Job = { orderId: string; drink: Drink };
type BaristaEvent = { type: 'job'; job: Job } | { type: 'cancel'; orderId: string } | ResourceReply;
type BaristaInput = { baristaId: string; tickMs: number; equipment: Record<Equipment, Mailbox<ResourceEvent>> };
type BaristaContext = BaristaInput & { job: Job | null };
const baristaMachine = setup({
  schemas: {
    input: types<BaristaInput>(), context: types<BaristaContext>(),
    events: { job: types<{ job: Job }>(), cancel: types<{ orderId: string }>(), 'resource.granted': types<{}>(), 'resource.paused': types<{}>() }
  },
  delays: { grind: ({ context }) => 3 * context.tickMs, extract: ({ context }) => 5 * context.tickMs, froth: ({ context }) => 4 * context.tickMs, handoff: ({ context }) => context.tickMs }
}).createMachine({
  id: 'barista',
  context: ({ input }) => ({ ...input, job: null }),
  initial: 'idle',
  states: {
    idle: { on: { job: ({ event }) => ({ target: 'working', context: { job: event.job } }) } },
    working: {
      initial: 'waitingForGrinder',
      on: { cancel: ({ context, event }, enq) => {
        if (event.orderId !== context.job?.orderId) return;
        for (const resource of Object.values(context.equipment)) enq.sendTo(resource, { type: 'release', ownerId: context.baristaId });
        return { target: 'available', context: { job: null } };
      } },
      states: {
        waitingForGrinder: {
          entry: ({ context, self }, enq) => enq.sendTo(context.equipment.grinder, { type: 'request', ownerId: context.baristaId, replyTo: self }),
          on: { 'resource.granted': { target: 'grinding' } }
        },
        grinding: {
          after: { grind: { target: 'waitingForEspresso' } },
          exit: ({ context, event }, enq) => { if (event.type !== 'resource.paused') enq.sendTo(context.equipment.grinder, { type: 'release', ownerId: context.baristaId }); },
          on: { 'resource.paused': { target: 'grinderPaused' } }
        },
        grinderPaused: { on: { 'resource.granted': { target: 'grinding' } } },
        waitingForEspresso: {
          entry: ({ context, self }, enq) => enq.sendTo(context.equipment.espresso, { type: 'request', ownerId: context.baristaId, replyTo: self }),
          on: { 'resource.granted': { target: 'extracting' } }
        },
        extracting: {
          after: { extract: ({ context }) => ({ target: context.job?.drink === 'latte' ? 'waitingForFrother' : 'handingOff' }) },
          exit: ({ context, event }, enq) => { if (event.type !== 'resource.paused') enq.sendTo(context.equipment.espresso, { type: 'release', ownerId: context.baristaId }); },
          on: { 'resource.paused': { target: 'espressoPaused' } }
        },
        espressoPaused: { on: { 'resource.granted': { target: 'extracting' } } },
        waitingForFrother: {
          entry: ({ context, self }, enq) => enq.sendTo(context.equipment.frother, { type: 'request', ownerId: context.baristaId, replyTo: self }),
          on: { 'resource.granted': { target: 'frothing' } }
        },
        frothing: {
          after: { froth: { target: 'handingOff' } },
          exit: ({ context, event }, enq) => { if (event.type !== 'resource.paused') enq.sendTo(context.equipment.frother, { type: 'release', ownerId: context.baristaId }); },
          on: { 'resource.paused': { target: 'frotherPaused' } }
        },
        frotherPaused: { on: { 'resource.granted': { target: 'frothing' } } },
        handingOff: { after: { handoff: { target: '#barista.finished' } } }
      }
    },
    finished: {
      entry: ({ context, parent }, enq) => { if (context.job) enq.sendTo(parent, { type: 'barista.finished', baristaId: context.baristaId, orderId: context.job.orderId }); },
      always: { target: 'available', context: { job: null } }
    },
    available: {
      entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'barista.available', baristaId: context.baristaId }),
      always: { target: 'idle' }
    }
  }
});

type OrderInput = { orderId: string; customerId: string; drink: Drink; tickMs: number; terminal: Mailbox<ResourceEvent> };
type OrderEvent = { type: 'cancel' } | { type: 'assigned'; baristaId: string } | { type: 'prepared' };
export type OrderStatus = 'charging' | 'queued' | 'preparing' | 'refunding' | 'served' | 'cancelled' | 'paymentFailed' | 'refundFailed';
export type OrderRecord = { customerId: string; drink: Drink; amount: number; status: OrderStatus; paid: boolean; baristaId: string | null; receiptId: string | null; refundReceiptId: string | null };
function createOrderMachine(services: CafeServices) {
  return setup({
    schemas: {
      input: types<OrderInput>(),
      context: types<OrderInput & { amount: number; cancelRequested: boolean; receiptId: string | null; refundReceiptId: string | null; baristaId: string | null }>(),
      events: { cancel: types<{}>(), assigned: types<{ baristaId: string }>(), prepared: types<{}>() }
    },
    actors: { payment: createPaymentMachine(services) }
  }).createMachine({
    id: 'order',
    context: ({ input }) => ({ ...input, amount: input.drink === 'latte' ? 550 : 350, cancelRequested: false, receiptId: null, refundReceiptId: null, baristaId: null }),
    initial: 'charging',
    states: {
      charging: {
        on: { cancel: { context: { cancelRequested: true } } },
        invoke: { id: 'payment-charge', src: 'payment', input: ({ context }) => ({ ...context, operation: 'charge' }),
          onDone: ({ event }) => !event.output.succeeded ? { target: 'paymentFailed' } : { target: 'paid', context: { receiptId: event.output.receiptId } }
        }
      },
      paid: {
        entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'order.paid', orderId: context.orderId, receiptId: context.receiptId }),
        always: ({ context }) => ({ target: context.cancelRequested ? 'refunding' : 'queued' })
      },
      queued: {
        entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'order.queued', orderId: context.orderId }),
        on: { assigned: ({ event }) => ({ target: 'preparing', context: { baristaId: event.baristaId } }), cancel: { target: 'refunding' } }
      },
      preparing: { on: { prepared: { target: 'served' }, cancel: { target: 'refunding' } } },
      refunding: {
        entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'order.refunding', orderId: context.orderId }),
        invoke: { id: 'payment-refund', src: 'payment', input: ({ context }) => ({ ...context, operation: 'refund' }),
          onDone: ({ event }) => event.output.succeeded ? { target: 'cancelled', context: { refundReceiptId: event.output.receiptId } } : { target: 'refundFailed' }
        }
      },
      served: { type: 'final', entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'order.settled', orderId: context.orderId, status: 'served', refundReceiptId: null }) },
      cancelled: { type: 'final', entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'order.settled', orderId: context.orderId, status: 'cancelled', refundReceiptId: context.refundReceiptId }) },
      paymentFailed: { type: 'final', entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'order.settled', orderId: context.orderId, status: 'paymentFailed', refundReceiptId: null }) },
      refundFailed: { type: 'final', entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'order.settled', orderId: context.orderId, status: 'refundFailed', refundReceiptId: null }) }
    }
  });
}

const customerMachine = setup({
  schemas: {
    input: types<{ customerId: string; orderId: string; patienceMs: number }>(),
    context: types<{ customerId: string; orderId: string; patienceMs: number }>(),
    events: { served: types<{}>(), cancelled: types<{}>() }
  },
  delays: { patience: ({ context }) => context.patienceMs }
}).createMachine({
  id: 'customer', context: ({ input }) => input, initial: 'waiting',
  states: {
    waiting: {
      after: { patience: { target: 'left' } },
      on: { served: { target: 'enjoyingCoffee' }, cancelled: { target: 'disappointed' } }
    },
    left: { type: 'final', entry: ({ context, parent }, enq) => enq.sendTo(parent, { type: 'customer.left', orderId: context.orderId }) },
    enjoyingCoffee: { type: 'final' }, disappointed: { type: 'final' }
  }
});

type CafeContext = {
  config: CafeInput; nextCustomer: number; automaticArrivals: number; closing: boolean;
  orders: Record<string, OrderRecord>; orderRefs: Record<string, Mailbox<OrderEvent>>;
  customerRefs: Record<string, Mailbox<{ type: 'served' } | { type: 'cancelled' }>>;
  baristaRefs: Record<string, Mailbox<BaristaEvent>>; equipment: Record<Equipment, Mailbox<ResourceEvent>>;
  queue: string[]; availableBaristas: string[]; assignments: Record<string, string>;
  served: number; cancelled: number; refunded: number; left: number; failed: number;
  departedCustomers: string[];
};

export function createCafeMachine(services: CafeServices) {
  return setup({
    schemas: {
      input: cafeInputSchema, context: types<CafeContext>(),
      events: {
        arrival: types<{ drink: Drink; patienceTicks?: number }>(), cancel: types<{ orderId: string }>(),
        'equipment.break': types<{ equipment: Equipment }>(), 'equipment.repair': types<{ equipment: Equipment }>(), close: types<{}>(),
        'automatic.arrival': types<{}>(), dispatch: types<{}>(),
        'order.paid': types<{ orderId: string; receiptId: string | null }>(), 'order.queued': types<{ orderId: string }>(),
        'order.refunding': types<{ orderId: string }>(),
        'order.settled': types<{ orderId: string; status: 'served' | 'cancelled' | 'paymentFailed' | 'refundFailed'; refundReceiptId: string | null }>(),
        'barista.finished': types<{ baristaId: string; orderId: string }>(), 'barista.available': types<{ baristaId: string }>(),
        'customer.left': types<{ orderId: string }>()
      }
    },
    actors: { equipment: equipmentMachine, barista: baristaMachine, customer: customerMachine, order: createOrderMachine(services) }
  }).createMachine({
    id: 'cafe', version: '1',
    context: ({ input, spawn, actors }) => {
      const equipment = {
        grinder: spawn(actors.equipment, { id: 'grinder', input: { equipment: 'grinder' } }),
        espresso: spawn(actors.equipment, { id: 'espresso', input: { equipment: 'espresso' } }),
        frother: spawn(actors.equipment, { id: 'frother', input: { equipment: 'frother' } }),
        terminal: spawn(actors.equipment, { id: 'terminal', input: { equipment: 'terminal' } })
      };
      const baristaRefs: CafeContext['baristaRefs'] = {};
      for (let i = 1; i <= input.baristas; i++) {
        const baristaId = `barista-${i}`;
        baristaRefs[baristaId] = spawn(actors.barista, { id: baristaId, input: { baristaId, tickMs: input.tickMs, equipment } });
      }
      return { config: input, nextCustomer: 1, automaticArrivals: 0, closing: false, orders: {}, orderRefs: {}, customerRefs: {}, equipment, baristaRefs, queue: [], availableBaristas: Object.keys(baristaRefs), assignments: {}, served: 0, cancelled: 0, refunded: 0, left: 0, failed: 0, departedCustomers: [] };
    },
    initial: 'open',
    entry: ({ context }, enq) => {
      if (context.config.customers > 0) enq.raise({ type: 'automatic.arrival' });
      if (context.config.breakAfterTicks > 0) {
        enq.raise({ type: 'equipment.break', equipment: 'espresso' }, { delay: context.config.breakAfterTicks * context.config.tickMs });
        enq.raise({ type: 'equipment.repair', equipment: 'espresso' }, { delay: (context.config.breakAfterTicks + context.config.repairTicks) * context.config.tickMs });
      }
    },
    on: {
      'equipment.break': ({ context, event }, enq) => enq.sendTo(context.equipment[event.equipment], { type: 'break' }),
      'equipment.repair': ({ context, event }, enq) => enq.sendTo(context.equipment[event.equipment], { type: 'repair' }),
      cancel: ({ context, event }, enq) => {
        const status = context.orders[event.orderId]?.status;
        if (status && ['charging', 'queued', 'preparing'].includes(status)) enq.sendTo(context.orderRefs[event.orderId], { type: 'cancel' });
        return {};
      },
      'customer.left': ({ context, event }, enq) => {
        enq.sendTo(context.orderRefs[event.orderId], { type: 'cancel' });
        const customerId = context.orders[event.orderId]?.customerId;
        return { context: { left: context.left + 1, departedCustomers: customerId ? [...context.departedCustomers, customerId] : context.departedCustomers } };
      },
      'order.paid': ({ context, event }) => {
        const record = context.orders[event.orderId];
        if (!record) return;
        return { context: { orders: { ...context.orders, [event.orderId]: { ...record, paid: true, receiptId: event.receiptId } } } };
      },
      'order.queued': ({ context, event }, enq) => {
        const record = context.orders[event.orderId];
        if (!record) return;
        enq.raise({ type: 'dispatch' });
        return { context: { queue: [...context.queue, event.orderId], orders: { ...context.orders, [event.orderId]: { ...record, status: 'queued' } } } };
      },
      dispatch: ({ context }, enq) => {
        const queue = [...context.queue];
        const availableBaristas = [...context.availableBaristas];
        const orders = { ...context.orders };
        const assignments = { ...context.assignments };
        while (queue.length && availableBaristas.length) {
          const orderId = queue.shift(); const baristaId = availableBaristas.shift();
          if (!orderId || !baristaId) break;
          const record = orders[orderId];
          if (!record) continue;
          assignments[baristaId] = orderId;
          orders[orderId] = { ...record, status: 'preparing', baristaId };
          enq.sendTo(context.orderRefs[orderId], { type: 'assigned', baristaId });
          enq.sendTo(context.baristaRefs[baristaId], { type: 'job', job: { orderId, drink: record.drink } });
        }
        return { context: { queue, availableBaristas, orders, assignments } };
      },
      'barista.finished': ({ context, event }, enq) => {
        enq.sendTo(context.orderRefs[event.orderId], { type: 'prepared' });
        const assignments = { ...context.assignments }; delete assignments[event.baristaId];
        return { context: { assignments } };
      },
      'barista.available': ({ context, event }, enq) => {
        enq.raise({ type: 'dispatch' });
        return { context: { availableBaristas: context.availableBaristas.includes(event.baristaId) ? context.availableBaristas : [...context.availableBaristas, event.baristaId] } };
      },
      'order.refunding': ({ context, event }, enq) => {
        const record = context.orders[event.orderId];
        if (!record) return;
        if (record.baristaId) enq.sendTo(context.baristaRefs[record.baristaId], { type: 'cancel', orderId: event.orderId });
        const assignments = { ...context.assignments };
        if (record.baristaId) delete assignments[record.baristaId];
        return { context: { assignments, queue: context.queue.filter((id) => id !== event.orderId), orders: { ...context.orders, [event.orderId]: { ...record, status: 'refunding' } } } };
      },
      'order.settled': ({ context, event }, enq) => {
        const record = context.orders[event.orderId];
        if (!record) return;
        if (!context.departedCustomers.includes(record.customerId)) enq.sendTo(context.customerRefs[record.customerId], { type: event.status === 'served' ? 'served' : 'cancelled' });
        return { context: {
          orders: { ...context.orders, [event.orderId]: { ...record, status: event.status, refundReceiptId: event.refundReceiptId } },
          served: context.served + Number(event.status === 'served'), cancelled: context.cancelled + Number(event.status === 'cancelled'),
          refunded: context.refunded + Number(event.refundReceiptId !== null), failed: context.failed + Number(event.status === 'paymentFailed' || event.status === 'refundFailed')
        } };
      }
    },
    states: {
      open: {
        on: {
          'automatic.arrival': ({ context }, enq) => {
            const automaticArrivals = context.automaticArrivals + 1;
            enq.raise({ type: 'arrival', drink: automaticArrivals % 2 === 0 ? 'espresso' : 'latte' });
            if (automaticArrivals < context.config.customers) enq.raise({ type: 'automatic.arrival' }, { delay: context.config.arrivalTicks * context.config.tickMs, id: 'next-arrival' });
            return { context: { automaticArrivals } };
          },
          arrival: ({ context, event }, enq) => {
            const customerId = `customer-${context.nextCustomer}`; const orderId = `order-${context.nextCustomer}`;
            const customer = enq.spawn('customer', { id: customerId, input: { customerId, orderId, patienceMs: (event.patienceTicks ?? context.config.patienceTicks) * context.config.tickMs } });
            const order = enq.spawn('order', { id: orderId, input: { orderId, customerId, drink: event.drink, tickMs: context.config.tickMs, terminal: context.equipment.terminal } });
            const record: OrderRecord = { customerId, drink: event.drink, amount: event.drink === 'latte' ? 550 : 350, status: 'charging', paid: false, baristaId: null, receiptId: null, refundReceiptId: null };
            return { context: { nextCustomer: context.nextCustomer + 1, orders: { ...context.orders, [orderId]: record }, orderRefs: { ...context.orderRefs, [orderId]: order }, customerRefs: { ...context.customerRefs, [customerId]: customer } } };
          },
          close: (_, enq) => { enq.cancel('next-arrival'); return { target: 'closing', context: { closing: true } }; }
        }
      },
      closing: { always: ({ context }) => {
        if (Object.values(context.orders).every((order) => ['served', 'cancelled', 'paymentFailed', 'refundFailed'].includes(order.status))) return { target: 'closed' };
      } },
      closed: { type: 'final' }
    }
  });
}
