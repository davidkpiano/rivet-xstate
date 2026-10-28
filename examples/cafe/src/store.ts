import { mkdirSync } from 'node:fs';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';
import { z } from 'zod';

const eventSchema = z.object({ type: z.string() }).passthrough();
export const deliverySchema = z.object({
  seq: z.number(),
  at: z.number(),
  address: z.string(),
  event: eventSchema,
});
export type Delivery = z.infer<typeof deliverySchema>;
const rowSchema = z.object({
  seq: z.number(),
  at: z.number(),
  address: z.string(),
  payload: z.string(),
});
const timerSchema = z.object({
  key: z.string(),
  address: z.string(),
  timer: z.string(),
  due_at: z.number(),
});
const paymentSchema = z.object({ amount: z.number(), receipt: z.string() });
const ownerSchema = z.object({ pid: z.number(), token: z.string() });
export const runSchema = z.object({
  executionId: z.string(),
  machineId: z.string(),
  machineVersion: z.string(),
  input: z.unknown(),
  startedAt: z.number(),
  roomId: z.string(),
  producerId: z.string(),
});
export type RunRecord = z.infer<typeof runSchema>;
export const resultSchema = z.discriminatedUnion('status', [
  z.object({ status: z.literal('done'), value: z.unknown() }),
  z.object({
    status: z.literal('error'),
    error: z.object({ name: z.string(), message: z.string() }),
  }),
]);
export type StoredResult = z.infer<typeof resultSchema>;

export class CafeStore {
  readonly db: DatabaseSync;
  readonly ownerToken = randomUUID();

  constructor(path: string) {
    mkdirSync(dirname(path), { recursive: true });
    this.db = new DatabaseSync(path);
    this.db.exec(`
      PRAGMA journal_mode=WAL;
      PRAGMA synchronous=FULL;
      PRAGMA busy_timeout=1000;
      CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, at INTEGER NOT NULL,
        address TEXT NOT NULL, payload TEXT NOT NULL, source TEXT UNIQUE
      );
      CREATE TABLE IF NOT EXISTS timers (
        key TEXT PRIMARY KEY, address TEXT NOT NULL, timer TEXT NOT NULL,
        due_at INTEGER NOT NULL, state TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS results (key TEXT PRIMARY KEY, input TEXT NOT NULL, output TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS payments (
        order_id TEXT NOT NULL, kind TEXT NOT NULL, amount INTEGER NOT NULL,
        receipt TEXT NOT NULL, PRIMARY KEY (order_id, kind)
      );
    `);
    try {
      this.transaction(() => {
        const existing = this.getMetadata('owner');
        if (existing !== undefined) {
          const owner = ownerSchema.parse(existing);
          let alive = true;
          try {
            process.kill(owner.pid, 0);
          } catch (error) {
            if (
              error instanceof Error &&
              'code' in error &&
              error.code === 'ESRCH'
            )
              alive = false;
            else throw error;
          }
          if (alive)
            throw new Error(
              `Café database already owned by process ${owner.pid}.`,
            );
        }
        this.setMetadata('owner', { pid: process.pid, token: this.ownerToken });
      });
    } catch (error) {
      this.db.close();
      throw error;
    }
  }

  transaction<T>(body: () => T): T {
    this.db.exec('BEGIN IMMEDIATE');
    try {
      const result = body();
      this.db.exec('COMMIT');
      return result;
    } catch (error) {
      this.db.exec('ROLLBACK');
      throw error;
    }
  }

  getMetadata(key: string): unknown {
    const row = this.db
      .prepare('SELECT value FROM metadata WHERE key = ?')
      .get(key);
    return row
      ? JSON.parse(z.object({ value: z.string() }).parse(row).value)
      : undefined;
  }

  setMetadata(key: string, value: unknown): void {
    this.db
      .prepare('INSERT OR REPLACE INTO metadata VALUES (?, ?)')
      .run(key, JSON.stringify(value));
  }

  get run(): RunRecord | undefined {
    const value = this.getMetadata('run');
    return value === undefined ? undefined : runSchema.parse(value);
  }

  history(): Delivery[] {
    return this.db
      .prepare('SELECT seq, at, address, payload FROM events ORDER BY seq')
      .all()
      .map((row) => {
        const record = rowSchema.parse(row);
        return deliverySchema.parse({
          ...record,
          event: JSON.parse(record.payload),
        });
      });
  }

  append(
    address: string,
    event: unknown,
    at: number,
    source: string | null = null,
  ): Delivery {
    const payload = JSON.stringify(eventSchema.parse(event));
    const result = this.db
      .prepare(
        'INSERT INTO events (at, address, payload, source) VALUES (?, ?, ?, ?)',
      )
      .run(at, address, payload, source);
    return deliverySchema.parse({
      seq: Number(result.lastInsertRowid),
      at,
      address,
      event,
    });
  }

  schedule(key: string, address: string, timer: unknown, dueAt: number): void {
    this.db
      .prepare("INSERT OR IGNORE INTO timers VALUES (?, ?, ?, ?, 'pending')")
      .run(key, address, JSON.stringify(timer), dueAt);
  }

  cancel(key: string): void {
    this.db
      .prepare(
        "UPDATE timers SET state = 'cancelled' WHERE key = ? AND state = 'pending'",
      )
      .run(key);
  }

  nextTimer(now: number): Delivery | undefined {
    return this.transaction(() => {
      const row = this.db
        .prepare(
          "SELECT key, address, timer, due_at FROM timers WHERE state = 'pending' AND due_at <= ? ORDER BY due_at, rowid LIMIT 1",
        )
        .get(now);
      if (!row) return undefined;
      const timer = timerSchema.parse(row);
      this.db
        .prepare("UPDATE timers SET state = 'fired' WHERE key = ?")
        .run(timer.key);
      return this.append(
        timer.address,
        { type: 'xstate.timer', ...JSON.parse(timer.timer) },
        timer.due_at,
        `timer:${timer.key}`,
      );
    });
  }

  result(key: string, input: string): StoredResult | undefined {
    const row = this.db
      .prepare('SELECT input, output FROM results WHERE key = ?')
      .get(key);
    if (!row) return undefined;
    const record = z
      .object({ input: z.string(), output: z.string() })
      .parse(row);
    if (record.input !== input)
      throw new Error(
        `Replay diverged at async actor ${key}. Use a new database for changed machines.`,
      );
    return resultSchema.parse(JSON.parse(record.output));
  }

  saveResult(key: string, input: string, output: StoredResult): void {
    this.db
      .prepare('INSERT INTO results VALUES (?, ?, ?)')
      .run(key, input, JSON.stringify(output));
  }

  payment(
    kind: 'charge' | 'refund',
    request: { orderId: string; amount: number },
  ): { receiptId: string } {
    const { orderId, amount } = request;
    if (!Number.isSafeInteger(amount) || amount <= 0)
      throw new Error('Payment amount must be positive integer cents.');
    return this.transaction(() => {
      const existing = this.db
        .prepare(
          'SELECT amount, receipt FROM payments WHERE order_id = ? AND kind = ?',
        )
        .get(orderId, kind);
      if (existing) {
        const record = paymentSchema.parse(existing);
        if (record.amount !== amount)
          throw new Error(`Conflicting ${kind} for ${orderId}.`);
        return { receiptId: record.receipt };
      }
      if (kind === 'refund') {
        const charged = this.db
          .prepare(
            "SELECT amount, receipt FROM payments WHERE order_id = ? AND kind = 'charge'",
          )
          .get(orderId);
        if (!charged || paymentSchema.parse(charged).amount !== amount)
          throw new Error(`No matching charge for ${orderId}.`);
      }
      const receiptId = `${kind}:${orderId}`;
      this.db
        .prepare('INSERT INTO payments VALUES (?, ?, ?, ?)')
        .run(orderId, kind, amount, receiptId);
      return { receiptId };
    });
  }

  payments() {
    return this.db
      .prepare(
        'SELECT order_id, kind, amount, receipt FROM payments ORDER BY order_id, kind',
      )
      .all();
  }

  timers() {
    return this.db
      .prepare(
        "SELECT key, address, timer, due_at, state FROM timers WHERE state = 'pending' ORDER BY due_at",
      )
      .all();
  }

  close(): void {
    const owner = this.getMetadata('owner');
    if (
      owner !== undefined &&
      ownerSchema.parse(owner).token === this.ownerToken
    ) {
      this.db.prepare("DELETE FROM metadata WHERE key = 'owner'").run();
    }
    this.db.close();
  }
}
