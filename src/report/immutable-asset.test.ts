import test from 'node:test';
import assert from 'node:assert/strict';
import { confirmImmutableAsset } from './immutable-asset.js';
import { CheckpointStorageFailure } from './checkpoint-diagnostic.js';
const expected = { name: 'checkpoint.tar.gz', bytes: 123, sha256: 'a'.repeat(64) };
const asset = { id: 9, name: expected.name, size: expected.bytes, state: 'uploaded' as const,
  digest: `sha256:${expected.sha256}` };
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
test('a matching starter is never confirmed or downloaded, even with matching size and digest', async () => {
  let uploads = 0, downloads = 0;
  await assert.rejects(confirmImmutableAsset(expected, { find: async () => ({ ...asset, state: 'starter' }),
    upload: async () => { uploads++; }, downloadedHash: async () => { downloads++; return expected.sha256; } }), /REMOTE_INVALID/);
  assert.equal(uploads, 0); assert.equal(downloads, 0);
});
test('name conflicts, corrupt content and repeated upload failures fail closed', async () => {
  for (const conflicting of [{ ...asset, size: 124 }, { ...asset, digest: `sha256:${'b'.repeat(64)}` }, { ...asset, digest: undefined }]) {
    await assert.rejects(confirmImmutableAsset(expected, { find: async () => conflicting,
      upload: async () => { throw new Error('must not clobber'); }, downloadedHash: async () => 'b'.repeat(64) }), /IDENTITY_CONFLICT/);
  }
  let uploads = 0;
  await assert.rejects(confirmImmutableAsset(expected, { find: async () => undefined,
    upload: async () => { uploads++; throw new Error('secret-url-token'); }, downloadedHash: async () => '' }),
  error => { assert.equal(String(error), 'CheckpointStorageFailure: ASSET_UPLOAD:UNKNOWN'); return true; });
  assert.equal(uploads, 1);
});

test('transient inventory errors are retried before upload; permanent auth is not', async () => {
  let finds = 0, uploads = 0;
  const transient = new CheckpointStorageFailure({ storageStage: 'ASSET_INVENTORY', storageCategory: 'HTTP_TRANSIENT',
    httpStatus: 503, retryable: true });
  assert.deepEqual(await confirmImmutableAsset(expected, { find: async () => { finds++; if (finds < 3) throw transient; return asset; },
    upload: async () => { uploads++; }, downloadedHash: async () => expected.sha256 }), asset);
  assert.equal(finds, 3); assert.equal(uploads, 0);
  finds = 0;
  await assert.rejects(confirmImmutableAsset(expected, { find: async () => { finds++; throw new CheckpointStorageFailure({
    storageStage: 'ASSET_INVENTORY', storageCategory: 'HTTP_PERMANENT', httpStatus: 403, retryable: false }); },
  upload: async () => { uploads++; }, downloadedHash: async () => expected.sha256 }), /HTTP_PERMANENT/);
  assert.equal(finds, 1); assert.equal(uploads, 0);
});

test('uncertain transient upload is reconciled before retry and cannot clobber a committed asset', async () => {
  let finds = 0, uploads = 0;
  const result = await confirmImmutableAsset(expected, {
    find: async () => { finds++; return finds >= 3 ? asset : undefined; },
    upload: async () => { uploads++; throw new CheckpointStorageFailure({ storageStage: 'ASSET_UPLOAD',
      storageCategory: 'TIMEOUT', httpStatus: null, retryable: true }); },
    downloadedHash: async () => expected.sha256,
  });
  assert.deepEqual(result, asset); assert.equal(uploads, 1);
});

test('transient upload may repeat only after three absent inventory observations', async () => {
  let finds = 0, uploads = 0;
  const result = await confirmImmutableAsset(expected, {
    find: async () => { finds++; return uploads >= 2 ? asset : undefined; },
    upload: async () => { uploads++; throw new CheckpointStorageFailure({ storageStage: 'ASSET_UPLOAD',
      storageCategory: 'HTTP_TRANSIENT', httpStatus: 503, retryable: true }); },
    downloadedHash: async () => expected.sha256,
  });
  assert.deepEqual(result, asset); assert.equal(uploads, 2); assert.ok(finds >= 5);
});
