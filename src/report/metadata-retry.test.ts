import assert from 'node:assert/strict';
import test from 'node:test';
import { ClientError, Status } from 'nice-grpc';
import { TinkoffApiError } from 'tinkoff-invest-api';
import { captureDeadlineSignal, classifyMetadataError, MetadataRequestFailure, retryMetadata,
  type MetadataDiagnostic, type MetadataRetryRuntime } from './metadata-retry.js';

class Clock implements MetadataRetryRuntime {
  time = 0;
  timers = new Map<number, { at: number; callback: () => void }>();
  nextId = 1;
  now = () => this.time;
  setTimer = (callback: () => void, milliseconds: number) => {
    const id = this.nextId++; this.timers.set(id, { at: this.time + milliseconds, callback }); return id;
  };
  clearTimer = (id: unknown) => { this.timers.delete(id as number); };
  advance(milliseconds: number) {
    const target = this.time + milliseconds;
    while (true) {
      const timer = [...this.timers].filter(([, value]) => value.at <= target).sort((a, b) => a[1].at - b[1].at)[0];
      if (!timer) break;
      this.time = timer[1].at; this.timers.delete(timer[0]); timer[1].callback();
    }
    this.time = target;
  }
}
async function settle() { for (let i = 0; i < 20; i++) await Promise.resolve(); }
const unavailable = () => new TinkoffApiError('/InstrumentsService/ShareBy', Status.UNAVAILABLE, 'private-error-and-token');

test('actual SDK errors distinguish local timeout, transient RPC and terminal failures without numeric-code guessing', () => {
  assert.deepEqual(classifyMetadataError(new DOMException('private', 'TimeoutError')), { code: 23, classification: 'TIMEOUT' });
  for (const [code, classification] of [[4, 'TIMEOUT'], [8, 'RESOURCE_EXHAUSTED'], [14, 'UNAVAILABLE']] as const) {
    assert.deepEqual(classifyMetadataError(new TinkoffApiError('/private', code, 'private')), { code, classification });
  }
  for (const code of [3, 5, 7, 13, 16]) {
    assert.deepEqual(classifyMetadataError(new ClientError('/private', code, 'private')), { code, classification: 'TERMINAL' });
  }
  assert.deepEqual(classifyMetadataError({ code: 23, message: 'private' }), { code: 23, classification: 'TERMINAL' });
  assert.equal(classifyMetadataError(new ClientError('/private', 14, 'Received HTTP status code 403 private')).classification, 'TERMINAL');
});

test('transient SDK metadata failure recovers once after bounded backoff and stores only safe fields', async () => {
  const clock = new Clock(), diagnostics: MetadataDiagnostic[] = []; let calls = 0;
  const result = retryMetadata(async () => { if (++calls === 1) throw unavailable(); return { raw: 'unchanged' }; },
    { operation: 'shareBy', runtime: clock, deadlineMs: 1000, attemptTimeoutMs: 100, backoffMs: [10], onDiagnostic: d => diagnostics.push(d) });
  await settle(); assert.equal(calls, 1); clock.advance(9); await settle(); assert.equal(calls, 1);
  clock.advance(1); await settle(); assert.deepEqual(await result, { raw: 'unchanged' }); assert.equal(calls, 2);
  assert.deepEqual(diagnostics, [{ operation: 'shareBy', attempt: 1, code: 14, classification: 'UNAVAILABLE', retry: true }]);
  assert.doesNotMatch(JSON.stringify(diagnostics), /private|token|InstrumentsService/); assert.equal(clock.timers.size, 0);
});

test('transient exhaustion stops at the configured attempt bound and keeps its category', async () => {
  const clock = new Clock(); let calls = 0; const diagnostics: MetadataDiagnostic[] = [];
  const result = retryMetadata(async () => { calls++; throw unavailable(); },
    { operation: 'getTradingStatuses', runtime: clock, maxAttempts: 3, attemptTimeoutMs: 100, backoffMs: [10], onDiagnostic: d => diagnostics.push(d) });
  const rejected = assert.rejects(result, (error: unknown) => error instanceof MetadataRequestFailure
    && error.diagnostic.attempt === 3 && error.diagnostic.classification === 'UNAVAILABLE' && !error.diagnostic.retry);
  await settle(); clock.advance(10); await settle(); clock.advance(10); await rejected;
  assert.equal(calls, 3); assert.equal(diagnostics.length, 3); assert.equal(clock.timers.size, 0);
});

test('attempt timeout bounds even an SDK request ignoring abort and can recover on the next request', async () => {
  const clock = new Clock(); let calls = 0, firstSignal: AbortSignal | undefined;
  const result = retryMetadata(async signal => { calls++; if (calls === 1) { firstSignal = signal; return new Promise<string>(() => {}); } return 'recovered'; },
    { operation: 'shareBy', runtime: clock, deadlineMs: 1000, attemptTimeoutMs: 100, backoffMs: [10] });
  await settle(); clock.advance(100); await settle(); assert.equal(firstSignal?.aborted, true);
  clock.advance(10); await settle(); assert.equal(await result, 'recovered'); assert.equal(calls, 2);
  assert.equal(clock.timers.size, 0);
});

test('user abort during an active request or retry backoff is terminal and starts no subsequent request', async () => {
  for (const backoff of [false, true]) {
    const clock = new Clock(), controller = new AbortController(); let calls = 0, activeSignal: AbortSignal | undefined;
    const result = retryMetadata(async signal => { calls++; activeSignal = signal;
      if (backoff) throw unavailable(); return new Promise<string>(() => {}); },
      { operation: 'shareBy', runtime: clock, signal: controller.signal, attemptTimeoutMs: 100, backoffMs: [10] });
    const rejected = assert.rejects(result, (error: unknown) => error instanceof MetadataRequestFailure
      && error.diagnostic.classification === 'CANCELLED' && !error.diagnostic.retry);
    await settle(); controller.abort(new Error('private-user-reason')); await rejected;
    clock.advance(1000); await settle(); assert.equal(calls, 1); assert.equal(clock.timers.size, 0);
    if (!backoff) assert.equal(activeSignal?.aborted, true);
  }
});

test('absolute deadline bounds preparation requests, forbids a late response and prevents further retries', async () => {
  const clock = new Clock(); let calls = 0;
  const result = retryMetadata(async () => { calls++; return new Promise<string>(() => {}); },
    { operation: 'tradingSchedules', runtime: clock, deadlineMs: 35, attemptTimeoutMs: 100 });
  const rejected = assert.rejects(result, (error: unknown) => error instanceof MetadataRequestFailure
    && error.diagnostic.classification === 'DEADLINE');
  await settle(); clock.advance(35); await rejected; assert.equal(calls, 1); assert.equal(clock.timers.size, 0);
  await assert.rejects(retryMetadata(async () => { calls++; return 'too late'; },
    { operation: 'shareBy', runtime: clock, deadlineMs: 35 }), MetadataRequestFailure);
  assert.equal(calls, 1);
});

test('backoff is never started if it would consume the remaining absolute deadline', async () => {
  const clock = new Clock(); let calls = 0;
  await assert.rejects(retryMetadata(async () => { calls++; throw unavailable(); },
    { operation: 'shareBy', runtime: clock, deadlineMs: 10, backoffMs: [10] }), MetadataRequestFailure);
  assert.equal(calls, 1); assert.equal(clock.timers.size, 0);
});

test('auth, malformed instrument/schema and unknown errors remain terminal without retry', async () => {
  for (const error of [new TinkoffApiError('/private', 16, 'token'), new TinkoffApiError('/private', 7, 'denied'),
    new TinkoffApiError('/private', 3, 'invalid'), new Error('Unsupported or mismatched instrument'), new SyntaxError('schema')]) {
    const clock = new Clock(); let calls = 0;
    await assert.rejects(retryMetadata(async () => { calls++; throw error; }, { operation: 'shareBy', runtime: clock }),
      (error: unknown) => error instanceof MetadataRequestFailure && error.diagnostic.classification === 'TERMINAL');
    assert.equal(calls, 1); assert.equal(clock.timers.size, 0);
  }
});

test('one absolute capture signal includes metadata time and stream setup, without restarting its budget', async () => {
  const clock = new Clock(), parent = new AbortController();
  const bound = captureDeadlineSignal(100, parent.signal, clock);
  clock.advance(70); assert.equal(bound.signal.aborted, false); // metadata preparation
  clock.advance(20); assert.equal(bound.signal.aborted, false); // writer and stream setup
  clock.advance(10); assert.equal(bound.signal.aborted, true);
  bound.dispose(); assert.equal(clock.timers.size, 0);
  const cancelled = captureDeadlineSignal(1000, parent.signal, clock);
  parent.abort(); assert.equal(cancelled.signal.aborted, true); cancelled.dispose(); assert.equal(clock.timers.size, 0);
});


test('original DOM timeout code23 recovers, while an already-cancelled request never starts', async () => {
  const clock = new Clock(); let calls = 0;
  const recovered = retryMetadata(async () => { if (++calls === 1) throw new DOMException('private', 'TimeoutError'); return 'ok'; },
    { operation: 'shareBy', runtime: clock, attemptTimeoutMs: 100, backoffMs: [10] });
  await settle(); clock.advance(10); await settle(); assert.equal(await recovered, 'ok'); assert.equal(calls, 2);
  const cancelled = new AbortController(); cancelled.abort();
  await assert.rejects(retryMetadata(async () => { calls++; return 'unexpected'; },
    { operation: 'shareBy', runtime: clock, signal: cancelled.signal }), MetadataRequestFailure);
  assert.equal(calls, 2);
});

test('response crossing the absolute deadline is rejected and a late cancelled SDK rejection is consumed', async () => {
  const clock = new Clock();
  await assert.rejects(retryMetadata(async () => { clock.advance(35); return 'late'; },
    { operation: 'shareBy', runtime: clock, deadlineMs: 35 }),
    (error: unknown) => error instanceof MetadataRequestFailure && error.diagnostic.classification === 'DEADLINE');
  const controller = new AbortController(); let rejectRequest: ((reason: unknown) => void) | undefined;
  const result = retryMetadata(() => new Promise<string>((_resolve, reject) => { rejectRequest = reject; }),
    { operation: 'shareBy', runtime: clock, signal: controller.signal });
  const rejected = assert.rejects(result, MetadataRequestFailure);
  await settle(); controller.abort(); await rejected;
  rejectRequest?.(unavailable()); await settle(); assert.equal(clock.timers.size, 0);
});


test('four full default request timeouts exhaust the retry budget as TIMEOUT, not the caller capture deadline', async () => {
  const clock = new Clock(); let calls = 0; const diagnostics: MetadataDiagnostic[] = [];
  const result = retryMetadata(async () => { calls++; return new Promise<string>(() => {}); },
    { operation: 'shareBy', runtime: clock, deadlineMs: 100_000, onDiagnostic: d => diagnostics.push(d) });
  const rejected = assert.rejects(result, (error: unknown) => error instanceof MetadataRequestFailure
    && error.diagnostic.classification === 'TIMEOUT' && error.diagnostic.code === 23
    && error.diagnostic.attempt === 4 && !error.diagnostic.retry);
  await settle();
  for (const ms of [10_000, 1_000, 10_000, 2_000, 10_000, 5_000, 10_000]) {
    clock.advance(ms); await settle();
  }
  await rejected; assert.equal(clock.now(), 48_000); assert.equal(calls, 4);
  assert.equal(diagnostics.length, 4); assert.equal(clock.timers.size, 0);
});

test('a shorter caller slot deadline interrupts the fourth request as DEADLINE, preserving window expiration', async () => {
  const clock = new Clock(); let calls = 0;
  const result = retryMetadata(async () => { calls++; return new Promise<string>(() => {}); },
    { operation: 'shareBy', runtime: clock, deadlineMs: 44_000 });
  const rejected = assert.rejects(result, (error: unknown) => error instanceof MetadataRequestFailure
    && error.diagnostic.classification === 'DEADLINE' && error.diagnostic.attempt === 4 && !error.diagnostic.retry);
  await settle();
  for (const ms of [10_000, 1_000, 10_000, 2_000, 10_000, 5_000, 6_000]) {
    clock.advance(ms); await settle();
  }
  await rejected; assert.equal(clock.now(), 44_000); assert.equal(calls, 4); assert.equal(clock.timers.size, 0);
});
