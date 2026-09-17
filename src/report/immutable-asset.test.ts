import test from 'node:test';
import assert from 'node:assert/strict';
import { confirmImmutableAsset } from './immutable-asset.js';
const expected = { name: 'checkpoint.tar.gz', bytes: 123, sha256: 'a'.repeat(64) };
const asset = { id: 9, name: expected.name, size: expected.bytes, digest: `sha256:${expected.sha256}` };
test('crash/timeout after remote commit recovers exact asset without duplicate upload', async () => {
  let committed = false, uploads = 0;
  assert.deepEqual(await confirmImmutableAsset(expected, {
    find: async () => committed ? asset : undefined,
    upload: async () => { uploads++; committed = true; throw new Error('lost response'); },
    downloadedHash: async () => { throw new Error('digest available'); },
  }), asset);
  assert.equal(uploads, 1);
});
test('existing matching asset is reused; missing digest requires byte verification', async () => {
  let downloads = 0;
  const existing = { ...asset, digest: undefined };
  await confirmImmutableAsset(expected, { find: async () => existing,
    upload: async () => { throw new Error('must not clobber'); },
    downloadedHash: async () => { downloads++; return expected.sha256; } });
  assert.equal(downloads, 1);
});
test('name conflicts, corrupt content and repeated upload failures fail closed', async () => {
  for (const conflicting of [{ ...asset, size: 124 }, { ...asset, digest: `sha256:${'b'.repeat(64)}` }, { ...asset, digest: undefined }]) {
    await assert.rejects(confirmImmutableAsset(expected, { find: async () => conflicting,
      upload: async () => { throw new Error('must not clobber'); }, downloadedHash: async () => 'b'.repeat(64) }), /conflict/);
  }
  let uploads = 0;
  await assert.rejects(confirmImmutableAsset(expected, { find: async () => undefined,
    upload: async () => { uploads++; throw new Error('offline'); }, downloadedHash: async () => '' }), /bounded/);
  assert.equal(uploads, 3);
});
