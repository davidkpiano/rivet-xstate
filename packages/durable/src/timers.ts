export function timerIdentity(
  address: string,
  timer: object & { occurrence?: number },
): string {
  const { occurrence: _occurrence, ...key } = timer;
  return `${address}:${JSON.stringify(key)}`;
}
