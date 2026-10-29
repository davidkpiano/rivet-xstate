import { setTimeout } from 'node:timers/promises';
import { RivetError } from 'rivetkit/client';

export function isTransportFailure(error: unknown): error is RivetError {
  return RivetError.isRivetError(error) && error.group !== 'user';
}

// Rivet 2.3.31 can return core/internal_error while a crashed runner's route expires.
export async function retryIdempotentRivetRequest<T>(
  request: () => Promise<T>,
  signal?: AbortSignal,
): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    signal?.throwIfAborted();
    try {
      return await request();
    } catch (error) {
      if (
        !isTransportFailure(error) ||
        (error.statusCode ?? 0) < 500 ||
        attempt >= 7
      )
        throw error;
      await setTimeout(Math.min(2000, 100 * 2 ** attempt), undefined, {
        signal,
      });
    }
  }
}
