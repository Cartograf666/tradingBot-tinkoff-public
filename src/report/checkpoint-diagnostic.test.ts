import assert from 'node:assert/strict';
import test from 'node:test';
import { checkpointStorageFailure } from './checkpoint-diagnostic.js';

test('HTTP and transport diagnostics retain only allowlisted fields', () => {
  const secret = 'token=https://private.example/secret';
  for (const [status, category, retryable] of [
    [403, 'HTTP_PERMANENT', false], [422, 'HTTP_PERMANENT', false],
    [429, 'HTTP_TRANSIENT', true], [503, 'HTTP_TRANSIENT', true],
  ] as const) {
    const failure = checkpointStorageFailure(Object.assign(new Error(secret), {
      stderr: Buffer.from(`HTTP ${status} ${secret}`), stdout: Buffer.from(secret),
    }), 'ASSET_UPLOAD');
    assert.deepEqual(failure.diagnostic, { storageStage: 'ASSET_UPLOAD', storageCategory: category,
      httpStatus: status, retryable });
    assert.ok(!JSON.stringify(failure).includes(secret));
    assert.ok(!failure.message.includes(secret));
  }
  const timeout = checkpointStorageFailure(Object.assign(new Error(secret), { code: 'ETIMEDOUT' }), 'ASSET_INVENTORY');
  assert.equal(timeout.diagnostic.storageCategory, 'TIMEOUT'); assert.equal(timeout.diagnostic.retryable, true);
  const unknown = checkpointStorageFailure(new Error(secret), 'ASSET_UPLOAD');
  assert.equal(unknown.diagnostic.storageCategory, 'UNKNOWN'); assert.equal(unknown.diagnostic.retryable, false);
});

test('abort takes precedence over raw network failure and cannot trigger retries', () => {
  const controller = new AbortController(); controller.abort();
  const failure = checkpointStorageFailure(Object.assign(new Error('private'), { code: 'ETIMEDOUT' }),
    'RELEASE_DISCOVERY', controller.signal);
  assert.deepEqual(failure.diagnostic, { storageStage: 'RELEASE_DISCOVERY', storageCategory: 'ABORTED',
    httpStatus: null, retryable: false });
});
