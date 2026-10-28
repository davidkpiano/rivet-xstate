import { actor, UserError } from 'rivetkit';
import { db, type RawAccess } from 'rivetkit/db';
import { z } from 'zod';

const paymentRequestSchema = z.object({
  orderId: z.string().min(1),
  amount: z.number().int().positive(),
});
type PaymentRequest = z.infer<typeof paymentRequestSchema>;
type PaymentKind = 'charge' | 'refund';
const receiptSchema = paymentRequestSchema.extend({
  kind: z.enum(['charge', 'refund']),
  receiptId: z.string(),
});
type Receipt = z.infer<typeof receiptSchema>;

async function payment(
  database: RawAccess,
  kind: PaymentKind,
  request: PaymentRequest,
  afterCommit?: (kind: PaymentKind, request: PaymentRequest) => void,
): Promise<{ receiptId: string }> {
  const parsed = paymentRequestSchema.safeParse(request);
  if (!parsed.success) {
    throw new UserError(
      'Payment requires an order ID and positive integer cents.',
      { code: 'invalid_payment' },
    );
  }
  const { orderId, amount } = parsed.data;
  const result = await database.transaction(
    async (transaction) => {
      const existing = z
        .array(receiptSchema)
        .parse(
          await transaction.execute(
            'SELECT order_id AS orderId, kind, amount, receipt_id AS receiptId FROM payments WHERE order_id = ? AND kind = ?',
            orderId,
            kind,
          ),
        )
        .at(0);
      if (existing) {
        if (existing.amount !== amount) {
          throw new UserError(`Conflicting ${kind} for ${orderId}.`, {
            code: 'conflicting_payment',
          });
        }
        return { receiptId: existing.receiptId, created: false };
      }
      if (kind === 'refund') {
        const charge = z
          .array(receiptSchema)
          .parse(
            await transaction.execute(
              "SELECT order_id AS orderId, kind, amount, receipt_id AS receiptId FROM payments WHERE order_id = ? AND kind = 'charge'",
              orderId,
            ),
          )
          .at(0);
        if (!charge || charge.amount !== amount) {
          throw new UserError(`No matching charge for ${orderId}.`, {
            code: 'missing_matching_charge',
          });
        }
      }
      const receiptId = `${kind}:${orderId}`;
      await transaction.execute(
        'INSERT INTO payments (order_id, kind, amount, receipt_id) VALUES (?, ?, ?, ?)',
        orderId,
        kind,
        amount,
        receiptId,
      );
      return { receiptId, created: true };
    },
    { name: 'payment' },
  );
  if (result.created) afterCommit?.(kind, parsed.data);
  return { receiptId: result.receiptId };
}

export function createPaymentLedger(
  options: {
    afterCommit?: (kind: PaymentKind, request: PaymentRequest) => void;
  } = {},
) {
  return actor({
    createState: () => ({}),
    db: db({
      onMigrate: async (database) => {
        await database.execute(`
          CREATE TABLE IF NOT EXISTS payments (
            order_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK (kind IN ('charge', 'refund')),
            amount INTEGER NOT NULL CHECK (typeof(amount) = 'integer' AND amount > 0),
            receipt_id TEXT NOT NULL,
            PRIMARY KEY (order_id, kind)
          )
        `);
      },
    }),
    actions: {
      charge: (context, request: PaymentRequest): Promise<{ receiptId: string }> =>
        payment(context.db, 'charge', request, options.afterCommit),
      refund: (context, request: PaymentRequest): Promise<{ receiptId: string }> =>
        payment(context.db, 'refund', request, options.afterCommit),
      receipts: async (context): Promise<Receipt[]> =>
        z.array(receiptSchema).parse(
          await context.db.execute(
            'SELECT order_id AS orderId, kind, amount, receipt_id AS receiptId FROM payments ORDER BY order_id, kind',
          ),
        ),
    },
  });
}

export const paymentLedger = createPaymentLedger();
