import assert from 'node:assert/strict';
import test from 'node:test';
import { SmokeQualityError, SmokeRetryError, StudyStageError, runSmokeWithRetries, type SmokeCheckEvent } from './smoke-retry.js';

function clock(initial = 0): { now: () => number; advance: (ms: number) => void } {
  let value = initial;
  return { now: () => value, advance: ms => { value += ms; } };
}

test('quality rejections retry with the same deadline and pass only after the final probe completes', async () => {
  const time = clock();
  const events: SmokeCheckEvent[] = [], deadlines: number[] = [];
  let attempts = 0, releaseArchive!: () => void, releaseRetry!: () => void, notifyRetry!: () => void, notifyArchive!: () => void;
  const retryStarted = new Promise<void>(resolve => { notifyRetry = resolve; });
  const archiveStarted = new Promise<void>(resolve => { notifyArchive = resolve; });
  const retryDone = new Promise<void>(resolve => { releaseRetry = resolve; });
  const archiveDone = new Promise<void>(resolve => { releaseArchive = resolve; });
  const work = runSmokeWithRetries(async deadline => {
    deadlines.push(deadline);
    attempts += 1;
    if (attempts === 1) throw new SmokeQualityError(['LOW_COVERAGE']);
    notifyArchive();
    await archiveDone;
    return 'recorded';
  }, {
    signal: new AbortController().signal,
    now: time.now,
    wait: async ms => { time.advance(ms); notifyRetry(); await retryDone; },
    onEvent: event => events.push(event),
  });
  await retryStarted;
  assert.equal(attempts, 1);
  releaseRetry();
  await archiveStarted;
  assert.equal(attempts, 2, 'the accepted probe is still awaiting its archive confirmation');
  releaseArchive();
  assert.equal(await work, 'recorded');
  assert.equal(attempts, 2);
  assert.deepEqual(deadlines, [480_000, 480_000]);
  assert.deepEqual(events.map(event => event.type), [
    'attempt', 'quality-rejected', 'retry', 'attempt', 'passed',
  ]);
});

test('three quality rejections stop exactly at the fixed retry limit', async () => {
  const time = clock(), events: SmokeCheckEvent[] = [];
  let attempts = 0;
  await assert.rejects(runSmokeWithRetries(async () => {
    attempts += 1;
    throw new SmokeQualityError(['VKCO_COVERAGE']);
  }, { signal: new AbortController().signal, now: time.now, wait: async ms => time.advance(ms), onEvent: event => events.push(event) }),
  (error: unknown) => error instanceof SmokeRetryError && error.reason === 'QUALITY_RETRIES_EXHAUSTED');
  assert.equal(attempts, 3);
  assert.deepEqual(events.at(-1), { type: 'stopped', attempt: 3, reason: 'QUALITY_RETRIES_EXHAUSTED' });
});

test('recording, quality implementation, replay, archive and ordinary errors never retry', async () => {
  const failures: Error[] = [
    new StudyStageError('recording'), new StudyStageError('quality'), new StudyStageError('replay'),
    new StudyStageError('private-archive'), new Error('quality looked bad'),
  ];
  for (const failure of failures) {
    const events: SmokeCheckEvent[] = [];
    let attempts = 0;
    await assert.rejects(runSmokeWithRetries(async () => { attempts += 1; throw failure; }, {
      signal: new AbortController().signal, onEvent: event => events.push(event),
    }), error => error === failure);
    assert.equal(attempts, 1);
    assert.deepEqual(events.at(-1), {
      type: 'fatal', attempt: 1, stage: failure instanceof StudyStageError ? failure.stage : 'unknown',
    });
  }
});

test('an already expired window does not start a probe or schedule a wait', async () => {
  const time = clock(1_000);
  let probes = 0, waits = 0;
  await assert.rejects(runSmokeWithRetries(async () => { probes += 1; return 1; }, {
    signal: new AbortController().signal, now: time.now, deadlineMs: 90_999,
    wait: async () => { waits += 1; },
  }), (error: unknown) => error instanceof SmokeRetryError && error.reason === 'CHECK_WINDOW_EXPIRED');
  assert.equal(probes, 0);
  assert.equal(waits, 0);
});

test('a late successful probe is rejected after it finishes', async () => {
  const time = clock();
  const events: SmokeCheckEvent[] = [];
  await assert.rejects(runSmokeWithRetries(async () => { time.advance(480_001); return 'late'; }, {
    signal: new AbortController().signal, now: time.now, onEvent: event => events.push(event),
  }), (error: unknown) => error instanceof SmokeRetryError && error.reason === 'CHECK_WINDOW_EXPIRED');
  assert.deepEqual(events.map(event => event.type), ['attempt', 'stopped']);
});

test('does not wait when a quality retry would leave less than the minimum probe window', async () => {
  const time = clock();
  let waits = 0, attempts = 0;
  await assert.rejects(runSmokeWithRetries(async () => {
    attempts += 1;
    throw new SmokeQualityError(['LOW_COVERAGE']);
  }, {
    signal: new AbortController().signal, now: time.now, deadlineMs: 104_999,
    wait: async () => { waits += 1; },
  }), (error: unknown) => error instanceof SmokeRetryError && error.reason === 'CHECK_WINDOW_EXPIRED');
  assert.equal(attempts, 1);
  assert.equal(waits, 0);
});

test('abort before, during a probe, and during the retry wait cannot continue', async () => {
  const before = new AbortController();
  before.abort();
  let probes = 0;
  await assert.rejects(runSmokeWithRetries(async () => { probes += 1; return 1; }, { signal: before.signal }), /abort/i);
  assert.equal(probes, 0);

  const duringProbe = new AbortController();
  await assert.rejects(runSmokeWithRetries(async () => { duringProbe.abort(); return 1; }, { signal: duringProbe.signal }), /abort/i);

  const duringWait = new AbortController();
  let attempts = 0;
  await assert.rejects(runSmokeWithRetries(async () => {
    attempts += 1;
    throw new SmokeQualityError(['LOW_COVERAGE']);
  }, {
    signal: duringWait.signal,
    wait: async () => { duringWait.abort(); },
  }), /abort/i);
  assert.equal(attempts, 1);
});

test('deadline and clock validation reject invalid inputs before collecting data', async () => {
  await assert.rejects(runSmokeWithRetries(async () => 1, {
    signal: new AbortController().signal, deadlineMs: NaN,
  }), /Invalid smoke retry deadline/);
  await assert.rejects(runSmokeWithRetries(async () => 1, {
    signal: new AbortController().signal, now: () => NaN,
  }), /Invalid smoke retry clock/);
});
