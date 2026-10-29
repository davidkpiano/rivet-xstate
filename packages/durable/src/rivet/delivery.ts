import type { Delivery } from './execution.ts';

export function selectDelivery(
  timer: Delivery | undefined,
  pending: Delivery | undefined,
  now: number,
): 'timer' | 'message' | 'none' {
  if (timer && timer.at <= now && (!pending || timer.at <= pending.at))
    return 'timer';
  return pending ? 'message' : 'none';
}
