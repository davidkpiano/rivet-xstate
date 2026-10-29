import { encode, decode } from 'cbor-x';

export function snapshotInput(input: unknown): unknown {
  return decode(encode(input));
}
