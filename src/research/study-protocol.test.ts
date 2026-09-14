import assert from 'node:assert/strict';
import test from 'node:test';
import {
  STUDY_PROTOCOL_HASH, chunkDurationSeconds, hashStudyValue, planStudyBlock, studyProtocol,
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
