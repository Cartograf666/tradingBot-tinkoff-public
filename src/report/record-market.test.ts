import assert from 'node:assert/strict';
import test from 'node:test';
import { performance } from 'node:perf_hooks';
import { nextMainSessionWindow } from '../research/observation-session.js';
import { captureMarketStream } from '../research/market-recorder.js';
import { TinkoffApiError } from 'tinkoff-invest-api';
import { boundedCaptureDuration, captureCompletionReason, normalizeCaptureStop, recorderFailure } from './record-market.js';
import { captureDeadlineSignal, MetadataRequestFailure } from './metadata-retry.js';

test('failed chunk continuation is permitted only for classified transient metadata exhaustion', () => {
  for (const [category, code] of [['TIMEOUT', 23], ['UNAVAILABLE', 14], ['RESOURCE_EXHAUSTED', 8]] as const) {
    const failure = new MetadataRequestFailure({ operation: 'shareBy', attempt: 4, code, classification: category, retry: false });
    assert.deepEqual(recorderFailure(failure, 'metadata'), { code, stage: 'metadata', category, retryable: true, operation: 'shareBy', attempt: 4 });
    assert.equal(recorderFailure(failure, 'stream').retryable, false);
  }
  for (const classification of ['DEADLINE', 'CANCELLED', 'TERMINAL'] as const) {
    assert.equal(recorderFailure(new MetadataRequestFailure({ operation: 'shareBy', attempt: 1, code: null, classification, retry: false }), 'metadata').retryable, false);
  }
  assert.equal(recorderFailure(new TinkoffApiError('/private', 16, 'private-token'), 'metadata').retryable, false);
  assert.equal(recorderFailure({ code: 14, details: 'private-token' }, 'metadata').retryable, false);
  assert.doesNotMatch(JSON.stringify(recorderFailure(new Error('private-token'), 'metadata')), /private-token/);
});

test('a recording that reaches its byte budget keeps the cap and records a typed capacity failure', async () => {
  const { mkdtempSync, rmSync } = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const path = await import('node:path');
  const { RecordingWriter } = await import('../research/market-recording.js');
  const directory = mkdtempSync(path.join(tmpdir(), 'recording-capacity-'));
  try {
    const writer = new RecordingWriter({ path: path.join(directory, 'events.ndjson'), runId: 'capacity', maxBytes: 180 });
    let failure: unknown;
    try { writer.append('response', { payload: 'x'.repeat(500) }, 1); } catch (error) { failure = error; }
    assert.deepEqual(recorderFailure(failure, 'stream'), {
      code: null, stage: 'stream', category: 'CAPACITY_EXHAUSTED', retryable: false,
    });
    assert.equal(writer.close().bytes, 0, 'the event that exceeds the hard cap is not partially written');
  } finally { rmSync(directory, { recursive: true, force: true }); }
});


test('absolute deadline completion is duration but never masks user cancellation or a stream failure', () => {
  assert.equal(captureCompletionReason('aborted', false, 1000, 1000), 'duration');
  assert.equal(captureCompletionReason('aborted', false, 1001, 1000), 'duration');
  assert.equal(captureCompletionReason('aborted', true, 1000, 1000), 'aborted');
  assert.equal(captureCompletionReason('aborted', false, 999, 1000), 'aborted');
  assert.equal(captureCompletionReason('aborted', false, 1000), 'aborted');
  assert.equal(captureCompletionReason('max_reconnects', false, 1000, 1000), 'max_reconnects');
});


test('the real capture loop obeys the earlier setup-bound deadline and discards a simultaneous late response', async t => {
  let now = 0, expire: (() => void) | undefined;
  t.mock.method(performance, 'now', () => now);
  const user = new AbortController();
  const bound = captureDeadlineSignal(100, user.signal, {
    now: () => now, setTimer: callback => { expire = callback; return 1; }, clearTimer: () => { expire = undefined; },
  });
  now = 70; // Metadata consumed seventy milliseconds of the absolute budget.
  const durationMs = boundedCaptureDuration(100, now, 100);
  now = 80; // Writer setup occurs after the relative duration was calculated.
  const events: string[] = [];
  let rawStop: { reason: string } | undefined;
  const result = await captureMarketStream({
    openStream: () => ({ [Symbol.asyncIterator]: () => ({ next: async () => {
      now = 100; expire?.(); return { done: false, value: { late: true } };
    } }) }),
    record: (kind, payload) => {
      events.push(kind);
      if (kind === 'stop') rawStop = normalizeCaptureStop(payload, user.signal.aborted, now, 100);
    }, expectedSubscriptions: [], acknowledgments: () => [],
    durationMs, tickIntervalMs: 10, signal: bound.signal, maxReconnects: 0,
  });
  assert.equal(result.reason, 'aborted');
  assert.equal(captureCompletionReason(result.reason, user.signal.aborted, now, 100), 'duration');
  assert.equal(result.responses, 0); assert.equal(events.includes('response'), false);
  assert.equal(events.at(-1), 'stop');
  assert.equal(rawStop?.reason, 'duration');
  const finalCapture = { ...result, reason: rawStop!.reason };
  user.abort(); // A later cleanup/interrupt cannot alter the reason already persisted.
  assert.equal(finalCapture.reason, rawStop?.reason);
  bound.dispose();
});


test('the final main-session chunk uses its remaining capture duration after metadata setup', () => {
  const start = Date.parse('2026-09-16T15:15:00Z'), end = Date.parse('2026-09-16T15:45:00Z');
  const instruments = [{ exchange: 'MOEX' }];
  const intervals = [{ exchange: 'MOEX', type: 'regular_trading_session_main',
    start: new Date(start).toISOString(), end: new Date(end).toISOString() }];
  const requestedMs = 1_795_000, captureDeadlineMs = end - 5_000, preparedAt = start + 6_000;
  // Reproduce the previous rejection: the original duration no longer fits after the six-second preparation.
  assert.equal(nextMainSessionWindow(instruments, intervals, preparedAt, requestedMs), null);
  const remainingMs = boundedCaptureDuration(requestedMs, preparedAt, captureDeadlineMs);
  assert.equal(remainingMs, 1_789_000);
  assert.deepEqual(nextMainSessionWindow(instruments, intervals, preparedAt, remainingMs),
    { start: new Date(preparedAt).toISOString(), end: new Date(end).toISOString() });
  // The calendar still requires its existing one-second margin and an explicit main interval.
  assert.equal(nextMainSessionWindow(instruments, intervals, preparedAt, end - preparedAt), null);
  assert.equal(nextMainSessionWindow(instruments, intervals.map(interval => ({ ...interval, type: 'regular_trading_session' })),
    preparedAt, remainingMs), null);
  assert.throws(() => boundedCaptureDuration(requestedMs, captureDeadlineMs, captureDeadlineMs));
});

test('checkpoint failures are classified as storage failures and never metadata retries', async () => {
  const { CheckpointQueueError } = await import('../research/checkpoint-queue.js');
  for (const category of ['CHECKPOINT_OVERFLOW', 'CHECKPOINT_UPLOAD_FAILED', 'CHECKPOINT_TIMEOUT'] as const) {
    assert.deepEqual(recorderFailure(new CheckpointQueueError(category), 'stream'),
      { code: null, stage: 'checkpoint', category, retryable: false });
  }
});

test('the capture loop keeps one connection across temporal checkpoints', async () => {
  const { mkdtempSync } = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const path = await import('node:path');
  const { RecordingWriter, scanRecording } = await import('../research/market-recording.js');
  const recordingPath = path.join(mkdtempSync(path.join(tmpdir(), 'continuous-capture-')), 'events.ndjson');
  let opened = 0, checkpoints = 0;
  const writer = new RecordingWriter({ path: recordingPath, runId: 'one-connection', checkpointIntervalMs: 15,
    onSegmentClosed: () => { checkpoints += 1; } });
  const result = await captureMarketStream({
    openStream: () => { opened += 1; return { [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => undefined) }) }; },
    record: (kind, payload, epoch) => writer.append(kind, payload, epoch),
    expectedSubscriptions: [], acknowledgments: () => [], durationMs: 90, tickIntervalMs: 5,
    heartbeatTimeoutMs: 1000,
  });
  const summary = writer.close();
  assert.equal(opened, 1); assert.equal(result.epochs, 1); assert.equal(result.disconnects, 0);
  assert.ok(checkpoints >= 2);
  const kinds: string[] = [];
  const scanned = await scanRecording(recordingPath, event => { kinds.push(event.kind); }, summary.segments);
  assert.equal(scanned.hasStop, true); assert.equal(kinds.filter(kind => kind === 'connect_attempt').length, 1);
  assert.equal(kinds.filter(kind => kind === 'stop').length, 1);
  assert.equal(kinds.includes('gap'), false);
});

test('upload failure aborts the actual capture and cannot produce a successful duration stop', async () => {
  const { mkdtempSync } = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const path = await import('node:path');
  const { RecordingWriter, scanRecording } = await import('../research/market-recording.js');
  const { CheckpointQueue } = await import('../research/checkpoint-queue.js');
  const controller = new AbortController();
  const queue = new CheckpointQueue<unknown>({ upload: async () => { throw new Error('storage unavailable'); },
    onFailure: error => controller.abort(error) });
  const recordingPath = path.join(mkdtempSync(path.join(tmpdir(), 'failed-continuous-')), 'events.ndjson');
  const writer = new RecordingWriter({ path: recordingPath, runId: 'storage-failure', checkpointIntervalMs: 5,
    onSegmentClosed: segment => queue.enqueue(segment) });
  await assert.rejects(captureMarketStream({
    openStream: () => ({ [Symbol.asyncIterator]: () => ({ next: () => new Promise(() => undefined) }) }),
    record: (kind, payload, epoch) => { queue.throwIfFailed(); writer.append(kind, payload, epoch); },
    expectedSubscriptions: [], acknowledgments: () => [], durationMs: 500, tickIntervalMs: 5,
    signal: controller.signal,
  }), /CHECKPOINT_UPLOAD_FAILED/);
  try { writer.close(); } catch { /* Final file remains durable even if it cannot be queued. */ }
  await assert.rejects(queue.drain(), /CHECKPOINT_UPLOAD_FAILED/);
  assert.equal(controller.signal.aborted, true);
  assert.equal((await scanRecording(recordingPath, undefined, writer.close().segments)).hasStop, false);
});


test('terminal raw stop normalization preserves user/storage aborts even at an expired deadline', () => {
  const payload = { reason: 'aborted', diagnostic: 'original' };
  assert.deepEqual(normalizeCaptureStop(payload, false, 100, 100), { reason: 'duration', diagnostic: 'original' });
  assert.deepEqual(payload, { reason: 'aborted', diagnostic: 'original' });
  for (const cause of ['user', 'storage']) {
    const controller = new AbortController(); controller.abort(cause);
    assert.equal(normalizeCaptureStop(payload, controller.signal.aborted, 101, 100).reason, 'aborted');
  }
  assert.equal(normalizeCaptureStop({ reason: 'max_reconnects' }, false, 101, 100).reason, 'max_reconnects');
  assert.throws(() => normalizeCaptureStop({ private: 'invalid' }, false, 101, 100), /Invalid capture stop/);
});
