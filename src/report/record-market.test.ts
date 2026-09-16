import assert from 'node:assert/strict';
import test from 'node:test';
import { performance } from 'node:perf_hooks';
import { nextMainSessionWindow } from '../research/observation-session.js';
import { captureMarketStream } from '../research/market-recorder.js';
import { TinkoffApiError } from 'tinkoff-invest-api';
import { boundedCaptureDuration, captureCompletionReason, recorderFailure } from './record-market.js';
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
  const result = await captureMarketStream({
    openStream: () => ({ [Symbol.asyncIterator]: () => ({ next: async () => {
      now = 100; expire?.(); return { done: false, value: { late: true } };
    } }) }),
    record: kind => { events.push(kind); }, expectedSubscriptions: [], acknowledgments: () => [],
    durationMs, tickIntervalMs: 10, signal: bound.signal, maxReconnects: 0,
  });
  assert.equal(result.reason, 'aborted');
  assert.equal(captureCompletionReason(result.reason, user.signal.aborted, now, 100), 'duration');
  assert.equal(result.responses, 0); assert.equal(events.includes('response'), false);
  assert.equal(events.at(-1), 'stop'); bound.dispose();
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
