import assert from 'node:assert/strict';
import test from 'node:test';
import { closedPlansNeedingFinalization, operationalDay, recordOwnedSlot, recoverableRecordingFailure, renderOperationalDay, type BlockOperation } from './study-operations.js';
import { createStudyLedger } from '../research/study-state.js';
import { planStudyBlock } from '../research/study-protocol.js';

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
