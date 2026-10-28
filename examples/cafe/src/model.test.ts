import { createActor, SimulatedClock } from 'xstate';
import { describe, expect, it } from 'vitest';
import { cafeCommandSchema, cafeInputSchema, createCafeMachine, type CafeServices } from './model.ts';

function cafe(options: { customers?: number; baristas?: number; patienceTicks?: number; services?: CafeServices } = {}) {
  const clock = new SimulatedClock();
  const payments: string[] = [];
  const refunds: string[] = [];
  const machine = createCafeMachine(options.services ?? {
    charge: async ({ orderId }) => { payments.push(orderId); return { receiptId: `charge-${orderId}` }; },
    refund: async ({ orderId }) => { refunds.push(orderId); return { receiptId: `refund-${orderId}` }; }
  });
  const errors: unknown[] = [];
  const actor = createActor(machine, {
    clock,
    input: cafeInputSchema.parse({ customers: options.customers ?? 0, baristas: options.baristas ?? 2, tickMs: 10, patienceTicks: options.patienceTicks ?? 1000 })
  });
  actor.subscribe({ error: (error) => errors.push(error) });
  actor.start();
  async function advance(ms: number, check?: () => void) {
    for (let elapsed = 0; elapsed < ms; elapsed++) {
      clock.increment(1);
      for (let flush = 0; flush < 12; flush++) await Promise.resolve();
      check?.();
      expect(errors).toEqual([]);
    }
  }
  return { actor, clock, advance, payments, refunds };
}

describe('cafe model', () => {
  it('serves staggered latte and espresso orders while baristas share exclusive equipment', async () => {
    const { actor, advance, payments } = cafe({ customers: 6, baristas: 3 });
    let contended = false;
    await advance(800, () => {
      const children = actor.getSnapshot().children;
      for (const equipment of ['grinder', 'espresso', 'frother']) {
        const resource = children[equipment]?.getSnapshot();
        if (resource?.context.queue.length > 0) contended = true;
        const activity = { grinder: 'grinding', espresso: 'extracting', frother: 'frothing' }[equipment];
        const active = Object.entries(children).filter(([id, ref]) => id.startsWith('barista-') && ref?.getSnapshot().value?.working === activity);
        expect(active.length).toBeLessThanOrEqual(1);
        if (active.length) expect(resource.context.holder.ownerId).toBe(active[0]?.[0]);
      }
    });
    expect(contended).toBe(true);
    expect(actor.getSnapshot().context.served).toBe(6);
    expect(payments).toEqual(['order-1', 'order-2', 'order-3', 'order-4', 'order-5', 'order-6']);
    expect(Object.values(actor.getSnapshot().context.orders).map((order) => order.drink)).toEqual(['latte', 'espresso', 'latte', 'espresso', 'latte', 'espresso']);
    actor.send({ type: 'close' });
    expect(actor.getSnapshot().status).toBe('done');
  });

  it('cancels work, releases reservations, refunds once, and lets the next order finish', async () => {
    const { actor, advance, payments, refunds } = cafe({ baristas: 1 });
    actor.send({ type: 'arrival', drink: 'latte' });
    actor.send({ type: 'arrival', drink: 'espresso' });
    await advance(35);
    expect(actor.getSnapshot().context.orders['order-1']?.status).toBe('preparing');
    actor.send({ type: 'cancel', orderId: 'order-1' });
    actor.send({ type: 'cancel', orderId: 'order-1' });
    await advance(300);
    expect(payments).toEqual(['order-1', 'order-2']);
    expect(refunds).toEqual(['order-1']);
    expect(actor.getSnapshot().context).toMatchObject({ served: 1, cancelled: 1, refunded: 1 });
    expect(actor.getSnapshot().context.orders['order-1']).toMatchObject({ status: 'cancelled', refundReceiptId: 'refund-order-1' });
    for (const name of ['grinder', 'espresso', 'frother', 'terminal']) expect(actor.getSnapshot().children[name]?.getSnapshot().context).toMatchObject({ holder: null, queue: [] });
    actor.stop();
  });

  it('keeps an order alive after its impatient customer leaves and compensates payment', async () => {
    const { actor, advance, refunds } = cafe({ customers: 1, patienceTicks: 4 });
    await advance(45);
    expect(actor.getSnapshot().context.left).toBe(1);
    expect(actor.getSnapshot().context.orders['order-1']?.status).toBe('refunding');
    expect(actor.getSnapshot().children['order-1']?.getSnapshot().status).toBe('active');
    await advance(100);
    expect(refunds).toEqual(['order-1']);
    expect(actor.getSnapshot().context).toMatchObject({ served: 0, cancelled: 1, refunded: 1 });
    actor.stop();
  });

  it('waits for an in-flight charge before refunding a cancelled order', async () => {
    let finishCharge: ((result: { receiptId: string }) => void) | undefined;
    const refunds: string[] = [];
    const { actor, advance } = cafe({ services: {
      charge: () => new Promise((resolve) => { finishCharge = resolve; }),
      refund: async ({ orderId }) => { refunds.push(orderId); return { receiptId: 'compensation' }; }
    } });
    actor.send({ type: 'arrival', drink: 'espresso' });
    await advance(25);
    actor.send({ type: 'cancel', orderId: 'order-1' });
    await advance(25);
    expect(refunds).toEqual([]);
    expect(actor.getSnapshot().context.orders['order-1']?.status).toBe('charging');
    expect(finishCharge).toBeDefined();
    finishCharge?.({ receiptId: 'authorized' });
    await advance(60);
    expect(refunds).toEqual(['order-1']);
    expect(actor.getSnapshot().context.orders['order-1']).toMatchObject({ status: 'cancelled', paid: true, receiptId: 'authorized', refundReceiptId: 'compensation' });
    actor.stop();
  });

  it('pauses extraction during breakdown, preserves the queue, then repairs and serves both orders', async () => {
    const { actor, advance } = cafe({ customers: 2 });
    await advance(60);
    expect(actor.getSnapshot().children['espresso']?.getSnapshot().context.holder).not.toBeNull();
    actor.send({ type: 'equipment.break', equipment: 'espresso' });
    expect(actor.getSnapshot().children['espresso']?.getSnapshot().value).toBe('broken');
    await advance(200);
    expect(actor.getSnapshot().context.served).toBe(0);
    expect(actor.getSnapshot().children['espresso']?.getSnapshot().context.queue).toHaveLength(1);
    actor.send({ type: 'equipment.repair', equipment: 'espresso' });
    await advance(250);
    expect(actor.getSnapshot().context.served).toBe(2);
    actor.stop();
  });

  it('blocks terminal transactions during a breakdown and reports a declined payment', async () => {
    const { actor, advance } = cafe({ services: {
      charge: async () => { throw new Error('declined'); },
      refund: async () => ({ receiptId: 'unused' })
    } });
    actor.send({ type: 'equipment.break', equipment: 'terminal' });
    actor.send({ type: 'arrival', drink: 'espresso' });
    await advance(50);
    expect(actor.getSnapshot().context.orders['order-1']?.status).toBe('charging');
    actor.send({ type: 'equipment.repair', equipment: 'terminal' });
    await advance(50);
    expect(actor.getSnapshot().context).toMatchObject({ failed: 1, served: 0, refunded: 0 });
    expect(actor.getSnapshot().context.orders['order-1']?.status).toBe('paymentFailed');
    actor.stop();
  });

  it('removes a cancelled queued order without sending it to a barista', async () => {
    const { actor, advance, refunds } = cafe({ customers: 3, baristas: 1 });
    await advance(65);
    expect(actor.getSnapshot().context.orders['order-3']?.status).toBe('queued');
    actor.send({ type: 'cancel', orderId: 'order-3' });
    await advance(350);
    expect(refunds).toEqual(['order-3']);
    expect(actor.getSnapshot().context).toMatchObject({ served: 2, cancelled: 1, refunded: 1 });
    expect(actor.getSnapshot().context.orders['order-3']?.baristaId).toBeNull();
    actor.stop();
  });

  it('releases a paused lease when cancelled and resumes the next waiting barista after repair', async () => {
    const { actor, advance } = cafe({ customers: 2 });
    await advance(60);
    actor.send({ type: 'equipment.break', equipment: 'espresso' });
    actor.send({ type: 'cancel', orderId: 'order-1' });
    await advance(50);
    expect(actor.getSnapshot().children['espresso']?.getSnapshot().context.holder).toBeNull();
    actor.send({ type: 'equipment.repair', equipment: 'espresso' });
    await advance(200);
    expect(actor.getSnapshot().context).toMatchObject({ served: 1, cancelled: 1, refunded: 1 });
    actor.stop();
  });

  it('closes admissions immediately, drains existing orders, and surfaces failed refunds', async () => {
    const { actor, advance } = cafe({ customers: 10, baristas: 1, services: {
      charge: async () => ({ receiptId: 'charge' }),
      refund: async () => { throw new Error('refund provider unavailable'); }
    } });
    actor.send({ type: 'close' });
    await advance(30);
    actor.send({ type: 'cancel', orderId: 'order-1' });
    await advance(100);
    expect(Object.keys(actor.getSnapshot().context.orders)).toEqual(['order-1']);
    expect(actor.getSnapshot().context.orders['order-1']?.status).toBe('refundFailed');
    expect(actor.getSnapshot().context).toMatchObject({ failed: 1, refunded: 0, served: 0 });
    expect(actor.getSnapshot().status).toBe('done');
  });

  it('validates external configuration and commands', () => {
    expect(cafeInputSchema.safeParse({ customers: -1, baristas: 0, tickMs: 0 }).success).toBe(false);
    expect(cafeCommandSchema.safeParse({ type: 'equipment.break', equipment: 'oven' }).success).toBe(false);
    expect(cafeCommandSchema.parse({ type: 'arrival', drink: 'latte' })).toEqual({ type: 'arrival', drink: 'latte' });
  });
});
