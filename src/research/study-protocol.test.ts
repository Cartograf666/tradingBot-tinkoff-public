import assert from 'node:assert/strict';
import test from 'node:test';
import {
  STUDY_PROTOCOL_HASH, assertStudyPreparationReady, chunkDurationSeconds, hashStudyValue, planRecoverableStudyBlock, planStudyBlock, planStudyPreparation, studyProtocol,
} from './study-protocol.js';

test('protocol hash covers the paused full-session collection contract', () => {
  assert.equal(STUDY_PROTOCOL_HASH, hashStudyValue(studyProtocol));
  assert.equal(studyProtocol.campaignEnabledByDefault, false);
  assert.deepEqual(studyProtocol.automaticBlockCheck,
    { session: 'main', requiredBeforeChunks: true, failure: 'abort-block', includedInReplay: false });
  assert.deepEqual(studyProtocol.tickers, ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS']);
  assert.equal(studyProtocol.blocks.early.owns, '[API main start, 14:00 Europe/Moscow)');
  assert.equal(studyProtocol.blocks.late.owns, '[14:00 Europe/Moscow, API main end)');
  assert.notEqual(hashStudyValue({ ...studyProtocol, depth: 10 }), STUDY_PROTOCOL_HASH);
});

test('armed preparation waits for today only within a bounded runner duration', () => {
  assert.deepEqual(planStudyPreparation('late', Date.parse('2026-09-15T07:30:00Z')),
    { sessionDate: '2026-09-15', readyAt: '2026-09-15T10:50:00.000Z' });
  assert.deepEqual(planStudyPreparation('early', Date.parse('2026-09-15T05:00:00Z')),
    { sessionDate: '2026-09-15', readyAt: '2026-09-15T05:50:00.000Z' });
  assert.equal(planStudyPreparation('late', Date.parse('2026-09-15T10:49:59.999Z')).readyAt, '2026-09-15T10:50:00.000Z');
  for (const now of ['2026-09-15T10:50:00Z', '2026-09-15T11:00:00Z', '2026-09-15T00:00:00Z', '2026-09-15T21:30:00Z', '2026-09-15T05:02:00Z']) {
    assert.throws(() => planStudyPreparation('late', Date.parse(now)), /later today/);
  }
  assert.throws(() => planStudyPreparation('late', NaN));
  const preparation = { sessionDate: '2026-09-15', readyAt: '2026-09-15T10:50:00.000Z' };
  assert.doesNotThrow(() => assertStudyPreparationReady(preparation, Date.parse(preparation.readyAt)));
  assert.throws(() => assertStudyPreparationReady(preparation, Date.parse('2026-09-15T10:49:59Z')));
  assert.throws(() => assertStudyPreparationReady(preparation, Date.parse('2026-09-15T21:00:00Z')));
});

test('early and delayed GitHub schedules preserve the original block admission window', () => {
  const day = '2026-09-16', start = `${day}T06:00:00Z`, end = `${day}T15:54:59Z`;
  for (const block of ['early', 'late'] as const) {
    const hour = block === 'early' ? '05' : '10';
    const preparation = planStudyPreparation(block, Date.parse(`${day}T${hour}:20:00Z`), 'schedule');
    assert.equal(preparation.readyAt, `${day}T${hour}:50:00.000Z`);
    const delayed = Date.parse(`${day}T${hour}:55:00Z`);
    assert.deepEqual(planStudyPreparation(block, delayed, 'schedule'), preparation);
    assert.ok(planStudyBlock(day, start, end, block, delayed));
    const missed = Date.parse(`${day}T15:00:00Z`);
    assert.deepEqual(planStudyPreparation(block, missed, 'schedule'), preparation);
    assert.equal(planStudyBlock(day, start, end, block, missed), null);
    assert.throws(() => planStudyPreparation(block, delayed), /later today/);
  }
  assert.throws(() => planStudyPreparation('late', Date.parse(`${day}T00:00:00Z`), 'schedule'), /bounded job duration/);
});

test('fresh API session is split into two complete ownership blocks and <=30 minute chunks', () => {
  const start = '2026-09-14T06:00:00.000Z', end = '2026-09-14T15:54:59.000Z';
  const early = planStudyBlock('2026-09-14', start, end, 'early', Date.parse('2026-09-14T05:50:00Z'))!;
  const late = planStudyBlock('2026-09-14', start, end, 'late', Date.parse('2026-09-14T10:50:00Z'))!;
  assert.equal(early.ownedStart, start);
  assert.equal(early.ownedEnd, '2026-09-14T11:00:00.000Z');
  assert.equal(early.chunks.length, 10);
  assert.equal(late.ownedStart, early.ownedEnd);
  assert.equal(late.ownedEnd, end);
  assert.equal(late.chunks.length, 10);
  assert.equal(late.chunks.at(-1)?.plannedEnd, end);
  assert.ok(late.chunks.every(chunk => Date.parse(chunk.plannedEnd) - Date.parse(chunk.plannedStart) <= 1_800_000));
  assert.equal(chunkDurationSeconds(late.chunks.at(-1)!, Date.parse(late.chunks.at(-1)!.plannedStart)), 1_494);
});

test('late job admission is bounded and missing time remains observable', () => {
  const start = '2026-09-14T06:00:00.000Z', end = '2026-09-14T15:54:59.000Z';
  const within = planStudyBlock('2026-09-14', start, end, 'early', Date.parse('2026-09-14T06:20:00Z'))!;
  assert.equal(within.latenessMs, 20 * 60 * 1_000);
  assert.equal(chunkDurationSeconds(within.chunks[0], Date.parse('2026-09-14T06:20:00Z')), 595);
  assert.equal(planStudyBlock('2026-09-14', start, end, 'early', Date.parse('2026-09-14T06:20:00.001Z')), null);
  assert.equal(planStudyBlock('2026-09-14', start, end, 'early', Date.parse('2026-09-14T05:49:59Z')), null);
});


test('recovery retains the full morning denominator and original windows at 13:02 Moscow', () => {
  const day = '2026-09-17', start = `${day}T06:00:00Z`, end = `${day}T15:54:59Z`;
  const original = planStudyBlock(day, start, end, 'early', Date.parse(`${day}T05:50:00Z`))!;
  const now = Date.parse(`${day}T10:02:00Z`);
  const recovered = planRecoverableStudyBlock(day, start, end, 'early', now)!;
  assert.equal(recovered.block, 'early');
  assert.equal(recovered.mainStart, original.mainStart); assert.equal(recovered.mainEnd, original.mainEnd);
  assert.equal(recovered.ownedStart, original.ownedStart); assert.equal(recovered.ownedEnd, original.ownedEnd);
  assert.deepEqual(recovered.chunks, original.chunks);
  assert.equal(recovered.latenessMs, 242 * 60_000);
  assert.deepEqual(recovered.recovery, { requestedBlock: 'early', selectedAt: new Date(now).toISOString(), partialStart: true });
  assert.equal(planStudyBlock(day, start, end, 'early', now), null);
});

test('late early triggers choose the afternoon block, retaining its full scientific bounds', () => {
  const day = '2026-09-17', start = `${day}T06:00:00Z`, end = `${day}T15:54:59Z`;
  const original = planStudyBlock(day, start, end, 'late', Date.parse(`${day}T10:50:00Z`))!;
  for (const time of ['11:13', '12:00']) {
    const recovered = planRecoverableStudyBlock(day, start, end, 'early', Date.parse(`${day}T${time}:00Z`))!;
    assert.equal(recovered.block, 'late');
    assert.equal(recovered.ownedStart, original.ownedStart); assert.equal(recovered.ownedEnd, original.ownedEnd);
    assert.equal(recovered.mainStart, original.mainStart); assert.equal(recovered.mainEnd, original.mainEnd);
    assert.deepEqual(recovered.chunks, original.chunks);
    assert.equal(recovered.recovery?.requestedBlock, 'early'); assert.equal(recovered.recovery?.partialStart, true);
  }
});

test('afternoon requests before its start wait for afternoon without recording morning', () => {
  const day = '2026-09-17';
  const plan = planRecoverableStudyBlock(day, `${day}T06:00:00Z`, `${day}T15:54:59Z`, 'late', Date.parse(`${day}T10:10:00Z`))!;
  assert.equal(plan.block, 'late'); assert.equal(plan.captureNotBefore, `${day}T11:00:00.000Z`);
  assert.equal(plan.chunks[0].plannedStart, `${day}T11:00:00.000Z`);
  assert.equal(plan.recovery, undefined);
});

test('on-time recoverable planning equals the strict original contract', () => {
  const day = '2026-09-17', start = `${day}T06:00:00Z`, end = `${day}T15:54:59Z`;
  for (const block of ['early', 'late'] as const) {
    const now = Date.parse(`${day}T${block === 'early' ? '05:50' : '10:50'}:00Z`);
    assert.deepEqual(planRecoverableStudyBlock(day, start, end, block, now), planStudyBlock(day, start, end, block, now));
  }
});

test('recovery is date-bound, requires meaningful remaining time and never extends closed sessions', () => {
  const day = '2026-09-17', start = `${day}T06:00:00Z`, end = `${day}T15:54:59Z`;
  const close = Date.parse(end);
  assert.ok(planRecoverableStudyBlock(day, start, end, 'early', close - 120_000));
  for (const now of [close - 119_999, close, close + 1, Date.parse('2026-09-18T12:00:00Z'), Date.parse(`${day}T00:00:00Z`)]) {
    assert.equal(planRecoverableStudyBlock(day, start, end, 'early', now), null);
  }
  // On a holiday discovery supplies no session; a previous day's API window cannot be reused.
  assert.equal(planRecoverableStudyBlock('2026-09-18', start, end, 'early', Date.parse('2026-09-18T12:00:00Z')), null);
  assert.throws(() => planRecoverableStudyBlock(day, end, start, 'early', Date.parse(`${day}T12:00:00Z`)), /clock/);
});
