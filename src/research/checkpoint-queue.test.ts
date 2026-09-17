import assert from 'node:assert/strict';
import test from 'node:test';
import { CheckpointQueue, CheckpointQueueError } from './checkpoint-queue.js';

const turn = () => new Promise<void>(resolve => setImmediate(resolve));

test('checkpoint uploads are sequential and drain waits for the final queued segment', async () => {
  const started: number[] = [], completed: number[] = [];
  let release: (() => void) | undefined;
  const queue = new CheckpointQueue<number>({ upload: async item => {
    started.push(item);
    if (item === 1) await new Promise<void>(resolve => { release = resolve; });
    completed.push(item);
  }, onFailure: () => assert.fail('Unexpected upload failure') });
  queue.enqueue(1); queue.enqueue(2);
  await turn();
  assert.deepEqual(started, [1]); assert.equal(queue.pending, 2);
  let drained = false;
  const draining = queue.drain().then(() => { drained = true; });
  await turn(); assert.equal(drained, false);
  release!(); await draining;
  assert.deepEqual(started, [1, 2]); assert.deepEqual(completed, [1, 2]);
  assert.equal(queue.pending, 0);
  assert.throws(() => queue.enqueue(3), /closed/);
});

test('slow upload overflow aborts once, cancels the active upload and does not upload queued segments', async () => {
  const errors: CheckpointQueueError[] = [], started: number[] = [];
  let signal: AbortSignal | undefined;
  const queue = new CheckpointQueue<number>({ upload: async (item, uploadSignal) => {
    started.push(item); signal = uploadSignal;
    await new Promise<void>(() => undefined);
  }, onFailure: error => { errors.push(error); }, uploadTimeoutMs: 10_000 });
  queue.enqueue(1); await turn(); queue.enqueue(2);
  assert.throws(() => queue.enqueue(3), /CHECKPOINT_OVERFLOW/);
  assert.equal(signal?.aborted, true);
  await assert.rejects(queue.drain(), /CHECKPOINT_OVERFLOW/);
  await turn(); assert.equal(queue.pending, 0);
  assert.deepEqual(started, [1]); assert.equal(errors.length, 1);
});

test('a failing storage request rejects the drain without exposing the upload error', async () => {
  let failures = 0;
  const queue = new CheckpointQueue<number>({ upload: async () => { throw new Error('secret-url-token'); },
    onFailure: () => { failures += 1; } });
  queue.enqueue(1); queue.enqueue(2);
  await assert.rejects(queue.drain(), error => {
    assert.equal(String(error), 'CheckpointQueueError: CHECKPOINT_UPLOAD_FAILED'); return true;
  });
  assert.equal(failures, 1);
});

test('a hung storage request and graceful drain are both bounded and abort their signal', async () => {
  for (const drainTimeout of [undefined, 5]) {
    let signal: AbortSignal | undefined;
    const queue = new CheckpointQueue<number>({ upload: async (_, uploadSignal) => {
      signal = uploadSignal; await new Promise<void>(() => undefined);
    }, onFailure: () => undefined, uploadTimeoutMs: 15 });
    queue.enqueue(1);
    await assert.rejects(queue.drain(drainTimeout), /CHECKPOINT_TIMEOUT/);
    assert.equal(signal?.aborted, true);
    await turn(); assert.equal(queue.pending, 0);
  }
});
