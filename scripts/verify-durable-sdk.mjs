import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync, statSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const [tarball, ...testArgs] = process.argv.slice(2);
if (!tarball)
  throw new Error('Usage: node scripts/verify-durable-sdk.mjs SDK_TARBALL [VITEST_ARGS...]');
const sdk = resolve(tarball);
if (!statSync(sdk).isFile()) throw new Error('SDK_TARBALL must be a packed SDK file.');
const cafe = resolve(dirname(fileURLToPath(import.meta.url)), '../examples/cafe');
const lockfile = resolve(cafe, 'pnpm-lock.yaml');
const manifests = [
  resolve(cafe, 'package.json'),
  resolve(cafe, '../../packages/durable/package.json'),
];
const originals = new Map(
  [...manifests, lockfile].map((path) => [path, readFileSync(path)]),
);

function run(args) {
  const result = spawnSync('pnpm', args, {
    cwd: cafe,
    stdio: 'inherit',
    env: { ...process.env, CI: 'true' },
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`pnpm ${args.join(' ')} failed (${result.status}).`);
}

try {
  for (const path of manifests) {
    const manifest = JSON.parse(readFileSync(path, 'utf8'));
    manifest.dependencies['@statelyai/sdk'] = `file:${sdk}`;
    writeFileSync(path, `${JSON.stringify(manifest, null, 2)}\n`);
  }
  run(['install', '--no-frozen-lockfile']);
  run(['typecheck']);
  run(['test', ...testArgs]);
} finally {
  for (const [path, contents] of originals) writeFileSync(path, contents);
}
