import { RivetError } from 'rivetkit/client';

export function isTransportFailure(error: unknown): error is RivetError {
  return RivetError.isRivetError(error) && error.group !== 'user';
}
