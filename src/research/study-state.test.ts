import assert from 'node:assert/strict';
import test from 'node:test';
import {
  acceptStudyChunk, acceptStudyDay, beginStudyAttempt, createStudyLedger, freezeStudy,
  mergeStudyLedgers, planStudyRun, type StudyChunkReceipt, type StudyDayReceipt, type StudyLedger,
} from './study-state.js';
import { STUDY_PROTOCOL_HASH } from './study-protocol.js';

const hex = (digit: string) => digit.repeat(64);
function begin(ledger: StudyLedger, date: string, block: 'early' | 'late', run: number): StudyLedger {
  return beginStudyAttempt(ledger, { runId: String(run), runAttempt: 1, sessionDate: date, block,
    mode: 'COUNTED', startedAt: `${date}T05:50:00.000Z` });
}
function chunk(ledger: StudyLedger, date: string, block: 'early' | 'late', run: number, assetId: number,
  quality: 'PASS' | 'INSUFFICIENT_DATA' = 'PASS'): StudyLedger {
  const receipt: StudyChunkReceipt = {
    schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, assetId, assetName: `${date}-${block}-${assetId}.tar.gz`,
    assetDigest: `sha256:${hex('a')}`, assetBytes: 4_000_000, archiveSha256: hex('b'), releaseTag: 'market-study-archive-v1',
    attemptId: `${run}:1`, chunkId: `${date}:${block}:1`, sessionDate: date,
    phase: ledger.attempts.find(item => item.attemptId === `${run}:1`)!.phase!, block, chunkIndex: 1,
    plannedStart: `${date}T${block === 'early' ? '06' : '11'}:00:00.000Z`,
    plannedEnd: `${date}T${block === 'early' ? '06' : '11'}:30:00.000Z`, runId: `capture-${assetId}`,
    manifestHash: hex('c'), recordingHash: hex('d'), replayConfigHash: hex('f'),
    simulatorHashes: { simulator: hex('1') }, quality, uploadedAt: `${date}T12:00:00.000Z`,
  };
  return acceptStudyChunk(ledger, receipt);
}
function dayReceipt(ledger: StudyLedger, date: string, id: number): StudyDayReceipt {
  const assets = ledger.chunks.filter(chunk => chunk.sessionDate === date && ledger.canonicalChunks[chunk.chunkId] === chunk.assetId);
  return {
    schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, dayId: `${STUDY_PROTOCOL_HASH}:${date}`, sessionDate: date,
    phase: assets[0].phase, mainStart: `${date}T06:00:00.000Z`, mainEnd: `${date}T15:55:00.000Z`,
    chunkAssetIds: assets.map(item => item.assetId), canonicalInputHash: hex('e'), replayConfigHash: hex('f'),
    simulatorHashes: { simulator: hex('1') }, reportAssetId: 100_000 + id,
    reportAssetName: `study-day-${date}.tar.gz`, reportAssetDigest: `sha256:${hex('2')}`,
    reportAssetBytes: 10_000, reportArchiveSha256: hex('3'), quality: { status: 'PASS', recordedShare: .995,
      expectedTicks: 35_700, observedTicks: 35_550,
      perInstrument: ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'].map(ticker => ({ ticker, usableShare: .9 })) },
    results: [{ name: 'baseline', netPnlRub: id % 2 ? -100 : 100 }], finalizedAt: `${date}T16:00:00.000Z`,
  };
}
function addDay(ledger: StudyLedger, date: string, base: number): StudyLedger {
  let next = begin(ledger, date, 'early', base);
  next = begin(next, date, 'late', base + 1);
  next = chunk(next, date, 'early', base, base * 10);
  next = chunk(next, date, 'late', base + 1, base * 10 + 1);
  return acceptStudyDay(next, dayReceipt(next, date, base));
}

test('campaign is configuration-paused while smoke remains available and does not count a day', () => {
  const ledger = createStudyLedger();
  assert.equal(planStudyRun(ledger, { event: 'schedule', campaignEnabled: false, runId: '1', runAttempt: 1 }).action, 'PAUSED_CONFIGURATION');
  const smoke = planStudyRun(ledger, { event: 'smoke', campaignEnabled: false, runId: '1', runAttempt: 1 });
  assert.equal(smoke.action, 'CAPTURE');
  if (smoke.action === 'CAPTURE') assert.equal(smoke.mode, 'SMOKE');
});

test('first quality-passing immutable chunk is canonical without using replay PnL', () => {
  const date = '2026-09-14';
  let ledger = begin(createStudyLedger(), date, 'early', 1);
  ledger = chunk(ledger, date, 'early', 1, 10, 'INSUFFICIENT_DATA');
  ledger = begin(ledger, date, 'early', 2);
  ledger = chunk(ledger, date, 'early', 2, 11, 'PASS');
  ledger = begin(ledger, date, 'early', 3);
  ledger = chunk(ledger, date, 'early', 3, 12, 'PASS');
  assert.equal(ledger.canonicalChunks[`${date}:early:1`], 11);
  assert.equal(ledger.chunks.length, 3);
});

test('a day requires canonical chunks from both blocks and ten unique passing days pause for freeze', () => {
  let ledger = createStudyLedger();
  for (let index = 0; index < 10; index += 1) {
    const date = `2026-09-${String(14 + index).padStart(2, '0')}`;
    ledger = addDay(ledger, date, index * 2 + 1);
  }
  assert.equal(ledger.phase, 'READY_TO_FREEZE');
  assert.equal(ledger.days.length, 10);
  assert.equal(planStudyRun(ledger, { event: 'schedule', campaignEnabled: true, runId: 'next', runAttempt: 1 }).action, 'PAUSED_PHASE');
  const pinned = begin(ledger, '2026-09-23', 'late', 99);
  assert.equal(pinned.attempts.at(-1)?.phase, 'DEVELOPMENT', 'the first counted phase pins the complete Moscow date');
  assert.throws(() => freezeStudy(ledger, { frozenAt: '2026-09-24T05:00:00.000Z', replayConfigHash: hex('8'),
    simulatorHashes: { simulator: hex('1') } }), /match the tested development/);
  ledger = freezeStudy(ledger, { frozenAt: '2026-09-24T05:00:00.000Z', replayConfigHash: hex('f'), simulatorHashes: { simulator: hex('1') } });
  assert.equal(ledger.phase, 'HOLDOUT');
  assert.equal(ledger.freeze?.developmentDates.length, 10);
  assert.throws(() => begin(ledger, '2026-09-14', 'early', 100), /cannot become holdout/);
});

test('all counted development days use one replay implementation identity', () => {
  let ledger = addDay(createStudyLedger(), '2026-09-14', 1);
  ledger = begin(ledger, '2026-09-15', 'early', 3);
  ledger = begin(ledger, '2026-09-15', 'late', 4);
  ledger = chunk(ledger, '2026-09-15', 'early', 3, 30);
  ledger = chunk(ledger, '2026-09-15', 'late', 4, 40);
  for (const item of ledger.chunks.filter(item => item.sessionDate === '2026-09-15')) item.replayConfigHash = hex('8');
  const receipt = { ...dayReceipt(ledger, '2026-09-15', 2), replayConfigHash: hex('8') };
  assert.throws(() => acceptStudyDay(ledger, receipt), /one replay implementation/);
});

test('optimistic merge cannot exceed the hard attempted-job ceiling', () => {
  let base = createStudyLedger();
  for (let index = 1; index <= 59; index += 1) base = begin(base, '2026-09-14', index % 2 ? 'early' : 'late', index);
  const left = begin(base, '2026-09-14', 'early', 60);
  const right = begin(base, '2026-09-14', 'late', 61);
  assert.throws(() => mergeStudyLedgers(left, right), /exceed the campaign limit/);
});

test('holdout capture must use the exact frozen replay source map', () => {
  let ledger = createStudyLedger();
  for (let index = 0; index < 10; index += 1) ledger = addDay(ledger, `2026-09-${String(14 + index).padStart(2, '0')}`, index * 2 + 1);
  ledger = freezeStudy(ledger, { frozenAt: '2026-09-24T16:00:00.000Z', replayConfigHash: hex('f'), simulatorHashes: { simulator: hex('1') } });
  ledger = begin(ledger, '2026-09-25', 'early', 50);
  const bad = chunk(ledger, '2026-09-25', 'early', 50, 500);
  assert.equal(bad.chunks.at(-1)?.phase, 'HOLDOUT');
  const receipt = { ...bad.chunks.at(-1)!, assetId: 501, assetName: 'wrong-source.tar.gz', simulatorHashes: { simulator: hex('9') } };
  assert.throws(() => acceptStudyChunk(ledger, receipt), /differs from the frozen/);
});

test('optimistic merge preserves concurrent early and late receipts', () => {
  const date = '2026-09-14';
  let base = begin(createStudyLedger(), date, 'early', 1);
  base = begin(base, date, 'late', 2);
  const early = chunk(base, date, 'early', 1, 10);
  const late = chunk(base, date, 'late', 2, 20);
  const merged = mergeStudyLedgers(early, late);
  assert.deepEqual(merged.chunks.map(item => item.assetId).sort(), [10, 20]);
  assert.equal(merged.canonicalChunks[`${date}:early:1`], 10);
  assert.equal(merged.canonicalChunks[`${date}:late:1`], 20);
});

test('daily quality is the only gate: incomplete block set and mutable day identity are rejected', () => {
  const date = '2026-09-14';
  let ledger = begin(createStudyLedger(), date, 'early', 1);
  ledger = chunk(ledger, date, 'early', 1, 10);
  const receipt = dayReceipt(ledger, date, 1);
  assert.throws(() => acceptStudyDay(ledger, receipt), /both ownership blocks/);

  ledger = begin(ledger, date, 'late', 2);
  ledger = chunk(ledger, date, 'late', 2, 20);
  const accepted = acceptStudyDay(ledger, dayReceipt(ledger, date, 1));
  assert.throws(() => acceptStudyDay(accepted, { ...dayReceipt(ledger, date, 1), canonicalInputHash: hex('9') }), /changed/);
});
