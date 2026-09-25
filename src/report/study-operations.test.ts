import assert from 'node:assert/strict';
import test from 'node:test';
import { closedPlansNeedingFinalization, collectorStatus, confirmOperationCheckpoint, continuousOperationFailureStage, continuousRecordingCompleted,
  markOperationRecordingStopped, mergeBlockOperations, operationalDay, recordOwnedSlot, recoverableRecordingFailure,
  renderOperationalDay, type BlockOperation } from './study-operations.js';
import { createStudyLedger } from '../research/study-state.js';
import { planStudyBlock } from '../research/study-protocol.js';
import type { StudyRuntimeSnapshot } from './study-runtime.js';

test('the report recovers an unfinished historical day once and waits until its session closes', () => {
  const plan = planStudyBlock('2026-09-15', '2026-09-15T06:00:00Z', '2026-09-15T15:54:59Z', 'late', Date.parse('2026-09-15T10:50:00Z'))!;
  const operation: BlockOperation = { schemaVersion: 1, attemptId: '1:1', plan, state: 'CAPTURING', failure: null,
    parts: [], startedAt: plan.prepareAt, updatedAt: plan.prepareAt };
  const ledger = createStudyLedger();
  assert.deepEqual(closedPlansNeedingFinalization(ledger, [operation], Date.parse('2026-09-15T14:00:00Z')), []);
  assert.deepEqual(closedPlansNeedingFinalization(ledger, [operation, operation], Date.parse('2026-09-16T08:00:00Z')), [plan]);
  ledger.days.push({ sessionDate: plan.sessionDate } as typeof ledger.days[number]);
  assert.deepEqual(closedPlansNeedingFinalization(ledger, [operation], Date.parse('2026-09-16T08:00:00Z')), []);
});

test('missing capture remains an incomplete operational day without accepting scientific data', () => {
  const ledger = createStudyLedger();
  const day = operationalDay(ledger, [], '2026-09-15', Date.parse('2026-09-16T08:00:00Z'));
  assert.equal(day.state, 'INCOMPLETE_DAY'); assert.equal(day.fullDayAccepted, false);
  assert.equal(day.confirmedArchives, 0); assert.equal(ledger.days.length, 0);
  assert.match(renderOperationalDay(day), /не засчитан/);
});

const reportDate = '2026-09-17', reportNow = Date.parse(`${reportDate}T12:10:00Z`);
function operation(state: BlockOperation['state'] = 'CAPTURING'): BlockOperation {
  const plan = planStudyBlock(reportDate, `${reportDate}T06:00:00Z`, `${reportDate}T15:54:59Z`, 'late', Date.parse(`${reportDate}T10:50:00Z`))!;
  return { schemaVersion: 1, attemptId: '123:1', plan, state, failure: null, parts: [],
    startedAt: `${reportDate}T12:05:00Z`, updatedAt: `${reportDate}T12:06:00Z`, currentChunkIndex: 3 };
}
function runtime(runs: StudyRuntimeSnapshot['runs'] = []): StudyRuntimeSnapshot {
  return { checkedAt: new Date(reportNow).toISOString(), available: true, reason: null, runs };
}
const activeRun = { runId: '123', mode: 'campaign' as const, block: 'early' as const, status: 'in_progress',
  captureJobRunning: true, captureCommandRunning: true, preparing: false, queued: false, startedAt: null };

test('stored capture state never substitutes for fresh GitHub liveness evidence', () => {
  assert.equal(collectorStatus([operation()], reportDate, reportNow).status, 'UNKNOWN');
  assert.equal(collectorStatus([operation()], reportDate, reportNow, { ...runtime(), available: false, reason: 'API_UNAVAILABLE' }).status, 'UNKNOWN');
  assert.equal(collectorStatus([operation()], reportDate, reportNow, runtime()).status, 'STOPPED');
  assert.equal(collectorStatus([], reportDate, reportNow, runtime()).status, 'IDLE');
  assert.equal(collectorStatus([operation()], '2026-09-16', reportNow, runtime()).status, 'HISTORICAL');
  const earlierFailure = { ...operation('FAILED'), updatedAt: `${reportDate}T12:00:00Z` };
  assert.equal(collectorStatus([earlierFailure, operation('FINISHED')], reportDate, reportNow, runtime()).status, 'IDLE');
});

test('runtime separates waiting, startup and diagnostic commands from canonical capture', () => {
  const status = (run: StudyRuntimeSnapshot['runs'][number], op: BlockOperation = operation()) => collectorStatus([op], reportDate, reportNow, runtime([run])).status;
  assert.equal(status({ ...activeRun, captureCommandRunning: false, captureJobRunning: false, preparing: true }), 'WAITING');
  assert.equal(status({ ...activeRun, captureCommandRunning: false, captureJobRunning: false, queued: true }), 'WAITING');
  assert.equal(status({ ...activeRun, captureCommandRunning: false }), 'STARTING');
  assert.equal(status(activeRun, operation('STARTING')), 'STARTING');
  assert.equal(status(activeRun, operation('PROCESSING')), 'PROCESSING');
  assert.equal(status({ ...activeRun, mode: 'observe', block: null }), 'DIAGNOSTIC');
  assert.equal(status(activeRun), 'CAPTURING');
});

test('archive deadline follows the actually selected chunk, with explicit upload grace and overdue status', () => {
  const live = runtime([activeRun]);
  const day = operationalDay(createStudyLedger(), [operation()], reportDate, reportNow, live);
  assert.equal(day.collector.nextExpectedUploadAt, `${reportDate}T12:33:00.000Z`);
  const overdue = collectorStatus([operation()], reportDate, Date.parse(`${reportDate}T12:34:00Z`), live);
  assert.equal(overdue.status, 'UPLOAD_OVERDUE'); assert.equal(overdue.uploadOverdueSeconds, 60);
  const nextChunk = { ...operation(), currentChunkIndex: 4 };
  assert.equal(collectorStatus([nextChunk], reportDate, Date.parse(`${reportDate}T12:34:00Z`), live).status, 'CAPTURING');
  assert.equal(day.fullDayAccepted, false);
});

test('saved final archive cannot become overdue during scientific replay', () => {
  const op = { ...operation(), currentChunkIndex: 10 };
  op.parts.push({ index: 10, status: 'SAVED', assetId: 123, quality: 'PASS', reasons: [] });
  const status = collectorStatus([op], reportDate, Date.parse(`${reportDate}T16:10:00Z`), runtime([activeRun]));
  assert.equal(status.status, 'PROCESSING');
  assert.equal(status.nextExpectedUploadAt, null); assert.equal(status.uploadOverdueSeconds, 0);
});

test('missing archive windows retain the whole session denominator and do not count future windows', () => {
  const ledger = createStudyLedger(), op = operation();
  const withoutArchives = operationalDay(ledger, [op], reportDate, reportNow, runtime());
  assert.equal(withoutArchives.unarchivedClosedWindows, 12);
  assert.equal(withoutArchives.unarchivedClosedWindowSeconds, 6 * 3600);
  const saved = op.plan.chunks[0];
  ledger.chunks.push({ sessionDate: reportDate, block: 'late', chunkIndex: saved.index,
    plannedStart: saved.plannedStart, plannedEnd: saved.plannedEnd, uploadedAt: `${reportDate}T11:30:20Z`,
    quality: 'INSUFFICIENT_DATA', assetId: 456, chunkId: `${reportDate}:late:1` } as typeof ledger.chunks[number]);
  const withArchive = operationalDay(ledger, [op], reportDate, reportNow, runtime());
  assert.equal(withArchive.unarchivedClosedWindows, 11);
  assert.equal(withArchive.rejectedParts, 1); assert.equal(withArchive.fullDayAccepted, false);
  assert.equal(ledger.days.length, 0);
  assert.match(renderOperationalDay(withArchive), /не оценка свежести/);
  assert.equal(operationalDay(ledger, [], reportDate, reportNow).unarchivedClosedWindows, null);
});
test('only explicit transient metadata failures permit continuation', () => {
  const failure = { stage: 'metadata', retryable: true, category: 'TIMEOUT' };
  assert.equal(recoverableRecordingFailure({ status: 'FAILED', failure }), true);
  for (const f of [{ ...failure, stage: 'stream' }, { ...failure, category: 'TERMINAL' },
    { ...failure, category: 'CANCELLED' }, { ...failure, retryable: false }, { code: 23 }]) {
    assert.equal(recoverableRecordingFailure({ status: 'FAILED', failure: f }), false);
  }
  const expired = { status: 'FAILED', failure: { stage: 'metadata', category: 'DEADLINE', retryable: false } };
  assert.equal(recoverableRecordingFailure(expired), false);
  assert.equal(recoverableRecordingFailure(expired, 100, 99), false);
  assert.equal(recoverableRecordingFailure(expired, 100, 100), true);
  assert.equal(recoverableRecordingFailure({ ...expired, failure: { ...expired.failure, category: 'CANCELLED' } }, 100, 101), false);
});
test('a saved transient failure gets one bounded slot retry, and success ends retrying', async () => {
  let calls = 0, time = 0; const archived: number[] = [];
  const value = await recordOwnedSlot({ deadlineMs: 100_000, signal: new AbortController().signal, now: () => time,
    attempt: async attempt => { calls++; archived.push(attempt); return { value: attempt, retryable: attempt === 1 }; },
    wait: async delay => { assert.deepEqual(archived, [1]); time += delay; } });
  assert.equal(value, 2); assert.equal(calls, 2); assert.equal(time, 5_000);
});
test('slot retries respect deadline, cancellation and failures saving evidence', async () => {
  let calls = 0;
  const options = { deadlineMs: 10_000, signal: new AbortController().signal, now: () => 0,
    attempt: async () => { calls++; return { value: 'failed', retryable: true }; },
    wait: async () => { throw new Error('unexpected wait'); } };
  assert.equal(await recordOwnedSlot(options), 'failed'); assert.equal(calls, 1);
  await assert.rejects(recordOwnedSlot({ ...options, attempt: async () => { throw new Error('archive failed'); } }), /archive failed/);
  const c = new AbortController(); c.abort();
  await assert.rejects(recordOwnedSlot({ ...options, signal: c.signal }));
  assert.equal(calls, 1);
});

test('continuous liveness uses confirmed checkpoint cadence and distinguishes processing', () => {
  const op = { ...operation(), captureFormat: 'continuous-v2' as const, checkpointIntervalMs: 300_000,
    captureStartedAt: `${reportDate}T12:05:00Z`, checkpoints: [{ index: 1, assetId: 900,
      lastReceivedAt: `${reportDate}T12:10:00Z`, confirmedAt: `${reportDate}T12:10:04Z` }] };
  const live = runtime([activeRun]);
  const day = operationalDay(createStudyLedger(), [op], reportDate, reportNow, live);
  assert.equal(day.collector.nextExpectedUploadAt, `${reportDate}T12:18:00.000Z`);
  assert.equal(day.state, 'PARTIAL_DATA'); assert.equal(day.confirmedCheckpoints, 1);
  assert.equal(day.passingParts, 0); assert.equal(day.fullDayAccepted, false);
  assert.equal(collectorStatus([op], reportDate, Date.parse(`${reportDate}T12:19:00Z`), live).status, 'UPLOAD_OVERDUE');
  assert.equal(collectorStatus([{ ...op, recordingStoppedAt: `${reportDate}T12:11:00Z` }], reportDate,
    Date.parse(`${reportDate}T12:19:00Z`), live).status, 'PROCESSING');
  assert.equal(collectorStatus([op], reportDate, reportNow, runtime()).status, 'STOPPED');
});

test('checkpoint confirmations keep their original upload times across later updates', () => {
  const op = operation();
  confirmOperationCheckpoint(op, { index: 1, assetId: 901, lastReceivedAt: `${reportDate}T12:10:00Z`, confirmedAt: `${reportDate}T12:10:04Z` });
  confirmOperationCheckpoint(op, { index: 2, assetId: 902, lastReceivedAt: `${reportDate}T12:15:00Z`, confirmedAt: `${reportDate}T12:15:09Z` });
  assert.deepEqual(op.checkpoints?.map(item => item.confirmedAt), [`${reportDate}T12:10:04Z`, `${reportDate}T12:15:09Z`]);
  assert.throws(() => confirmOperationCheckpoint(op, { index: 1, assetId: 999,
    lastReceivedAt: `${reportDate}T12:10:00Z`, confirmedAt: `${reportDate}T12:20:00Z` }), /Conflicting/);
});

test('recording completion separates recorder failures from raw storage failures', () => {
  const recorderFailure = operation();
  assert.equal(continuousOperationFailureStage(recorderFailure), 'RECORDING');
  const storageFailure = operation();
  markOperationRecordingStopped(storageFailure, `${reportDate}T12:11:00Z`);
  assert.equal(continuousOperationFailureStage(storageFailure), 'STORAGE_OR_PROCESSING');
  assert.equal(continuousRecordingCompleted('COMPLETE', 'duration'), true);
  assert.equal(continuousRecordingCompleted('COMPLETE', 'abort'), false);
  assert.equal(continuousRecordingCompleted('FAILED', 'duration'), false);
});

test('stale processing updates cannot erase finished state, checkpoints or recovered parts', () => {
  const finished = { ...operation('FINISHED'), recordingStoppedAt: `${reportDate}T12:11:00Z`,
    checkpoints: [{ index: 1, assetId: 901, lastReceivedAt: `${reportDate}T12:10:00Z`, confirmedAt: `${reportDate}T12:10:04Z` }],
    parts: [{ index: 1, status: 'SAVED' as const, assetId: 1001, quality: 'PASS' as const, reasons: [] }] };
  const stale = { ...operation('PROCESSING'), updatedAt: `${reportDate}T12:12:00Z`,
    checkpoints: [{ index: 2, assetId: 902, lastReceivedAt: `${reportDate}T12:15:00Z`, confirmedAt: `${reportDate}T12:15:09Z` }] };
  const merged = mergeBlockOperations(finished, stale);
  assert.equal(merged.state, 'FINISHED');
  assert.deepEqual(merged.checkpoints?.map(item => item.assetId), [901, 902]);
  assert.equal(merged.parts.length, 1);
});

test('deferred scientific processing stays visible after the capture command exits', () => {
  const op = operation('PROCESSING');
  const status = collectorStatus([op], reportDate, reportNow, runtime());
  assert.equal(status.status, 'PROCESSING'); assert.equal(status.reason, 'SCIENTIFIC_PROCESSING_PENDING');
});
