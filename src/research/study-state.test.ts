import assert from 'node:assert/strict';
import test from 'node:test';
import {
  acceptStudyChunk, acceptStudyDay, beginStudyAttempt, createStudyLedger, freezeStudy,
  mergeStudyLedgers, migrateStudyAttemptBudget, extendStudyAttemptBudget, planStudyRun, type StudyChunkReceipt, type StudyDayReceipt, type StudyLedger,
} from './study-state.js';
import { hashStudyValue, planStudyBlock, STUDY_60_ATTEMPT_PROTOCOL_HASH, STUDY_100_ATTEMPT_PROTOCOL_HASH, STUDY_MAX_ATTEMPTS, STUDY_PROTOCOL_HASH } from './study-protocol.js';
import { selectStudyDayInputs, studyDayInputHash } from './study-day-inputs.js';

const hex = (digit: string) => digit.repeat(64);
function begin(ledger: StudyLedger, date: string, block: 'early' | 'late', run: number): StudyLedger {
  return beginStudyAttempt(ledger, { runId: String(run), runAttempt: 1, sessionDate: date, block,
    mode: 'COUNTED', startedAt: `${date}T05:50:00.000Z` });
}
const bounds = (date: string) => ({ sessionDate: date, mainStart: `${date}T06:00:00.000Z`, mainEnd: `${date}T15:55:00.000Z` });
function planned(date: string, block: 'early' | 'late') {
  const session = bounds(date);
  return planStudyBlock(date, session.mainStart, session.mainEnd, block,
    Date.parse(`${date}T${block === 'early' ? '05' : '10'}:50:00.000Z`))!;
}
function chunk(ledger: StudyLedger, date: string, block: 'early' | 'late', run: number, assetId: number,
  quality: 'PASS' | 'INSUFFICIENT_DATA' = 'PASS', index = 1): StudyLedger {
  const plannedChunk = planned(date, block).chunks[index - 1]!;
  const receipt: StudyChunkReceipt = {
    schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, assetId, assetName: `${date}-${block}-${assetId}.tar.gz`,
    assetDigest: `sha256:${hex('a')}`, assetBytes: 4_000_000, archiveSha256: hex('b'), releaseTag: 'market-study-archive-v1',
    attemptId: `${run}:1`, chunkId: `${date}:${block}:${index}`, sessionDate: date,
    phase: ledger.attempts.find(item => item.attemptId === `${run}:1`)!.phase!, block, chunkIndex: index,
    plannedStart: plannedChunk.plannedStart, plannedEnd: plannedChunk.plannedEnd, runId: `capture-${assetId}`,
    manifestHash: hex('c'), recordingHash: hex('d'), replayConfigHash: hex('f'),
    simulatorHashes: { simulator: hex('1') }, quality, uploadedAt: `${date}T12:00:00.000Z`,
  };
  return acceptStudyChunk(ledger, receipt);
}
function dayReceipt(ledger: StudyLedger, date: string, id: number): StudyDayReceipt {
  const selected = selectStudyDayInputs(ledger, bounds(date));
  const assets = selected.receipts;
  return {
    schemaVersion: 1, protocolHash: STUDY_PROTOCOL_HASH, dayId: `${STUDY_PROTOCOL_HASH}:${date}`, sessionDate: date,
    phase: assets[0].phase, mainStart: bounds(date).mainStart, mainEnd: bounds(date).mainEnd,
    chunkAssetIds: assets.map(item => item.assetId), canonicalInputHash: studyDayInputHash(assets), replayConfigHash: hex('f'),
    simulatorHashes: { simulator: hex('1') }, reportAssetId: 100_000 + id,
    reportAssetName: `study-day-${date}.tar.gz`, reportAssetDigest: `sha256:${hex('2')}`,
    reportAssetBytes: 10_000, reportArchiveSha256: hex('3'), quality: { status: 'PASS', recordedShare: .995,
      expectedTicks: 35_700, observedTicks: 35_550,
      perInstrument: ['SBER', 'GAZP', 'MAGN', 'VKCO', 'SMLT', 'AFKS'].map(ticker => ({ ticker, usableShare: .9 })) },
    results: [{ name: 'baseline', netPnlRub: id % 2 ? -100 : 100 }], finalizedAt: `${date}T16:00:00.000Z`,
  };
}
function prepareDay(ledger: StudyLedger, date: string, base: number): StudyLedger {
  let next = begin(ledger, date, 'early', base);
  next = begin(next, date, 'late', base + 1);
  for (const block of ['early', 'late'] as const) {
    for (const plannedChunk of planned(date, block).chunks) {
      next = chunk(next, date, block, block === 'early' ? base : base + 1,
        base * 100 + (block === 'early' ? 0 : 50) + plannedChunk.index, 'PASS', plannedChunk.index);
    }
  }
  return next;
}
function addDay(ledger: StudyLedger, date: string, base: number): StudyLedger {
  const next = prepareDay(ledger, date, base);
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

test('a complete day with ten unique passing dates pauses for freeze', () => {
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
  ledger = prepareDay(ledger, '2026-09-15', 3);
  for (const item of ledger.chunks.filter(item => item.sessionDate === '2026-09-15')) item.replayConfigHash = hex('8');
  const receipt = { ...dayReceipt(ledger, '2026-09-15', 2), replayConfigHash: hex('8') };
  assert.throws(() => acceptStudyDay(ledger, receipt), /one replay implementation/);
});

test('optimistic merge cannot exceed the hard attempted-job ceiling', () => {
  let base = createStudyLedger();
  for (let index = 1; index < STUDY_MAX_ATTEMPTS; index += 1) base = begin(base, '2026-09-14', index % 2 ? 'early' : 'late', index);
  const left = begin(base, '2026-09-14', 'early', STUDY_MAX_ATTEMPTS);
  const right = begin(base, '2026-09-14', 'late', STUDY_MAX_ATTEMPTS + 1);
  assert.throws(() => mergeStudyLedgers(left, right), /exceed the campaign limit/);
});

function predecessor(): StudyLedger {
  let old = begin(createStudyLedger(), '2026-09-14', 'early', 1);
  old = chunk(old, '2026-09-14', 'early', 1, 55);
  for (let index = 2; index <= 60; index += 1) old = begin(old, '2026-09-14', 'early', index);
  old.protocolHash = STUDY_60_ATTEMPT_PROTOCOL_HASH;
  old.chunks[0]!.protocolHash = STUDY_60_ATTEMPT_PROTOCOL_HASH;
  return old;
}

test('explicit 60-to-100 migration preserves immutable history and admits attempt 61', () => {
  const old = predecessor(), original = structuredClone(old), oldHash = hashStudyValue(old);
  assert.throws(() => planStudyRun(old, { event: 'schedule', campaignEnabled: true, runId: '61', runAttempt: 1 }), /protocol mismatch/);
  const next = migrateStudyAttemptBudget(old, '2026-10-05T10:00:00.000Z');
  assert.deepEqual(old, original);
  assert.equal(next.attemptBudgetMigration?.sourceLedgerHash, oldHash);
  assert.equal(next.protocolHash, STUDY_100_ATTEMPT_PROTOCOL_HASH);
  assert.deepEqual(next.attempts, old.attempts);
  assert.deepEqual(next.chunks, old.chunks);
  assert.deepEqual(next.canonicalChunks, old.canonicalChunks);
  assert.deepEqual(next.days, old.days);
  assert.equal(next.freeze, old.freeze);
  assert.throws(() => planStudyRun(next, { event: 'schedule', campaignEnabled: true, runId: '61', runAttempt: 1 }), /protocol mismatch/);
  const extended = extendStudyAttemptBudget(next, '2026-10-06T10:00:00.000Z');
  assert.equal(planStudyRun(extended, { event: 'schedule', campaignEnabled: true, runId: '61', runAttempt: 1 }).action, 'CAPTURE');
  const resumed = begin(extended, '2026-10-06', 'early', 61);
  assert.equal(resumed.attempts.length, 61);
  assert.deepEqual(extendStudyAttemptBudget(resumed, '2026-10-07T10:00:00.000Z'), resumed);
  assert.deepEqual(acceptStudyChunk(resumed, old.chunks[0]!), resumed, 'old immutable receipt remains idempotent');
  assert.throws(() => acceptStudyChunk(resumed, { ...old.chunks[0]!, assetId: 56, assetName: 'late-old.tar.gz' }), /retired study protocol/);
  assert.throws(() => planStudyRun({ ...resumed, protocolHash: hex('9') },
    { event: 'schedule', campaignEnabled: true, runId: '62', runAttempt: 1 }), /protocol mismatch/);
  assert.throws(() => migrateStudyAttemptBudget({ ...old, protocolHash: hex('9') }, '2026-10-05T10:00:00.000Z'), /authorized/);
});

test('migration receipt pins historical identities through optimistic merges', () => {
  const migrated = extendStudyAttemptBudget(migrateStudyAttemptBudget(predecessor(), '2026-10-05T10:00:00.000Z'), '2026-10-06T10:00:00.000Z');
  const first = begin(migrated, '2026-10-05', 'early', 61);
  const second = begin(migrated, '2026-10-05', 'late', 62);
  const merged = mergeStudyLedgers(first, second);
  assert.equal(merged.attempts.length, 62);
  assert.deepEqual(merged.attempts.slice(0, 60), migrated.attempts);
  assert.throws(() => begin({ ...first, chunks: [{ ...first.chunks[0]!, archiveSha256: hex('9') }] },
    '2026-10-05', 'late', 63), /historical data changed/);
  assert.throws(() => begin({ ...first, canonicalChunks: {} },
    '2026-10-05', 'late', 63), /historical data changed/);
  assert.throws(() => mergeStudyLedgers(first, createStudyLedger()), /Conflicting study attempt-budget migrations/);
});

function hundredPredecessor(): StudyLedger {
  const ledger = migrateStudyAttemptBudget(predecessor(), '2026-10-05T10:00:00.000Z');
  const day = addDay(createStudyLedger(), '2026-09-15', 500);
  ledger.attempts.push(...day.attempts);
  ledger.chunks.push(...day.chunks.map(receipt => ({ ...receipt, protocolHash: STUDY_100_ATTEMPT_PROTOCOL_HASH })));
  Object.assign(ledger.canonicalChunks, day.canonicalChunks);
  ledger.days.push({ ...day.days[0]!, protocolHash: STUDY_100_ATTEMPT_PROTOCOL_HASH,
    dayId: `${STUDY_100_ATTEMPT_PROTOCOL_HASH}:2026-09-15` });
  return ledger;
}

test('100-to-120 extension preserves the original migration, receipts and already accepted day', () => {
  const old = hundredPredecessor(), original = structuredClone(old);
  const extended = extendStudyAttemptBudget(old, '2026-10-06T10:00:00.000Z');
  assert.deepEqual(old, original);
  assert.equal(extended.attemptBudgetExtension?.sourceLedgerHash, hashStudyValue(old));
  assert.deepEqual(extended.attemptBudgetMigration, old.attemptBudgetMigration);
  assert.deepEqual(extended.attempts, old.attempts);
  assert.deepEqual(extended.chunks, old.chunks);
  assert.deepEqual(extended.canonicalChunks, old.canonicalChunks);
  assert.deepEqual(extended.days, old.days);
  assert.equal(extended.days[0]!.protocolHash, STUDY_100_ATTEMPT_PROTOCOL_HASH);
  assert.deepEqual(extendStudyAttemptBudget(extended, '2026-10-07T10:00:00.000Z'), extended);
  assert.equal(planStudyRun(extended, { event: 'schedule', campaignEnabled: true, runId: 'new', runAttempt: 1 }).action, 'CAPTURE');
  assert.throws(() => acceptStudyChunk(extended, { ...old.chunks.at(-1)!, assetId: 99999,
    assetName: 'new-for-old-attempt.tar.gz', protocolHash: STUDY_PROTOCOL_HASH }), /Pre-extension attempt/);
  assert.deepEqual(acceptStudyChunk(extended, old.chunks.at(-1)!), extended);
  assert.throws(() => acceptStudyDay(extended, { ...old.days[0]!, protocolHash: STUDY_PROTOCOL_HASH,
    dayId: `${STUDY_PROTOCOL_HASH}:2026-09-15`, reportAssetId: 88888 }), /Pre-extension study date/);
});

test('new 120-era receipt merges beside a finalized 100-era day without rewriting its science', () => {
  const extended = extendStudyAttemptBudget(hundredPredecessor(), '2026-10-06T10:00:00.000Z');
  const branch = chunk(begin(extended, '2026-10-07', 'early', 700), '2026-10-07', 'early', 700, 70001);
  const merged = mergeStudyLedgers(extended, branch);
  assert.equal(merged.chunks.length, extended.chunks.length + 1);
  assert.deepEqual(merged.days, extended.days);
  assert.deepEqual(merged.attemptBudgetMigration, extended.attemptBudgetMigration);
  assert.deepEqual(merged.attemptBudgetExtension, extended.attemptBudgetExtension);
  assert.equal(merged.days[0]!.protocolHash, STUDY_100_ATTEMPT_PROTOCOL_HASH);
  assert.equal(merged.chunks.at(-1)!.protocolHash, STUDY_PROTOCOL_HASH);
  assert.equal(merged.canonicalChunks['2026-10-07:early:1'], 70001);
});

test('extension rejects historical tampering and protects its attempt ceiling under races', () => {
  const original = hundredPredecessor(), extended = extendStudyAttemptBudget(original, '2026-10-06T10:00:00.000Z');
  const tamper = (change: (ledger: StudyLedger) => void) => {
    const ledger = structuredClone(extended); change(ledger);
    assert.throws(() => begin(ledger, '2026-10-07', 'early', 800), /historical data changed|migration receipt/);
  };
  tamper(ledger => { ledger.attempts[60]!.runId = 'rewritten'; });
  tamper(ledger => { ledger.chunks.at(-1)!.archiveSha256 = hex('9'); });
  tamper(ledger => { delete ledger.canonicalChunks['2026-09-15:early:1']; });
  tamper(ledger => { ledger.days[0]!.reportArchiveSha256 = hex('9'); });
  tamper(ledger => { ledger.attemptBudgetMigration!.sourceLedgerHash = hex('9'); });
  tamper(ledger => { ledger.attemptBudgetExtension!.oldChunkCount -= 1; });
  assert.throws(() => extendStudyAttemptBudget({ ...original, freeze: {
    frozenAt: '2026-10-06T10:00:00Z', holdoutNotBeforeDate: '2026-10-07', protocolHash: original.protocolHash,
    replayConfigHash: hex('a'), simulatorHashes: { simulator: hex('b') }, developmentDates: [],
  } }, '2026-10-06T10:00:00Z'), /authorized/);
  let almostFull = extended;
  for (let index = almostFull.attempts.length + 1; index <= STUDY_MAX_ATTEMPTS - 1; index += 1) {
    almostFull = begin(almostFull, '2026-10-07', index % 2 ? 'early' : 'late', index + 1000);
  }
  const left = begin(almostFull, '2026-10-07', 'early', 2000);
  const right = begin(almostFull, '2026-10-07', 'late', 2001);
  assert.equal(left.attempts.length, STUDY_MAX_ATTEMPTS);
  assert.equal(planStudyRun(left, { event: 'schedule', campaignEnabled: true, runId: '2002', runAttempt: 1 }).action, 'ATTEMPT_LIMIT');
  assert.throws(() => begin(left, '2026-10-07', 'late', 2002), /attempt limit/);
  assert.throws(() => mergeStudyLedgers(left, right), /exceed the campaign limit/);
  assert.throws(() => mergeStudyLedgers(extended, createStudyLedger()), /Conflicting study attempt-budget migrations/);
  const conflicting = structuredClone(extended);
  conflicting.attemptBudgetExtension!.migratedAt = '2026-10-06T10:01:00.000Z';
  assert.throws(() => mergeStudyLedgers(extended, conflicting), /Conflicting study attempt-budget extensions/);
});

test('holdout capture must use the exact frozen replay source map', () => {
  let ledger = createStudyLedger();
  for (let index = 0; index < 10; index += 1) ledger = addDay(ledger, `2026-09-${String(14 + index).padStart(2, '0')}`, index * 2 + 1);
  ledger = freezeStudy(ledger, { frozenAt: '2026-09-24T16:00:00.000Z', replayConfigHash: hex('f'), simulatorHashes: { simulator: hex('1') } });
  ledger = begin(ledger, '2026-09-25', 'early', 50);
  const bad = chunk(ledger, '2026-09-25', 'early', 50, 5000);
  assert.equal(bad.chunks.at(-1)?.phase, 'HOLDOUT');
  const receipt = { ...bad.chunks.at(-1)!, assetId: 5001, assetName: 'wrong-source.tar.gz', simulatorHashes: { simulator: hex('9') } };
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

test('optimistic merge preserves the already accepted PASS even when a later branch has a lower asset ID', () => {
  const date = '2026-09-14';
  let base = begin(createStudyLedger(), date, 'early', 1);
  base = begin(base, date, 'early', 2);
  const firstCommitted = chunk(base, date, 'early', 1, 200);
  const staleBranch = chunk(base, date, 'early', 2, 100);
  const merged = mergeStudyLedgers(firstCommitted, staleBranch);
  assert.equal(merged.canonicalChunks[`${date}:early:1`], 200);
  assert.equal(merged.chunks.length, 2);
});

test('daily acceptance requires all planned inputs and immutable identity', () => {
  const date = '2026-09-14';
  let ledger = begin(createStudyLedger(), date, 'early', 1);
  ledger = chunk(ledger, date, 'early', 1, 10);
  const receipt = dayReceipt(ledger, date, 1);
  assert.throws(() => acceptStudyDay(ledger, receipt), /every planned receipt/);

  const accepted = addDay(createStudyLedger(), date, 1);
  assert.throws(() => acceptStudyDay(accepted, { ...accepted.days[0]!, canonicalInputHash: hex('9') }), /changed/);
});

test('a late first window can fail locally while a complete quality-passing day is counted exactly once', () => {
  const date = '2026-09-14';
  let ledger = prepareDay(createStudyLedger(), date, 1);
  const late = ledger.chunks.find(item => item.chunkId === `${date}:late:1`)!;
  late.quality = 'INSUFFICIENT_DATA';
  delete ledger.canonicalChunks[late.chunkId];
  const selected = selectStudyDayInputs(ledger, bounds(date));
  assert.equal(selected.receipts.length, selected.expectedCount);
  assert.equal(selected.receipts.find(item => item.chunkId === late.chunkId)?.quality, 'INSUFFICIENT_DATA');
  assert.equal(ledger.canonicalChunks[late.chunkId], undefined, 'failed window stays noncanonical for retries');
  const daily = dayReceipt(ledger, date, 1);
  daily.quality.recordedShare = .995;
  const accepted = acceptStudyDay(ledger, daily);
  assert.equal(accepted.days.length, 1);
  assert.equal(accepted.days[0]!.quality.status, 'PASS');
  assert.equal(acceptStudyDay(accepted, daily).days.length, 1, 'identical replay report is idempotent');
  assert.equal(acceptStudyChunk(accepted, late).chunks.length, accepted.chunks.length, 'identical archive receipt is idempotent');
  const retry = { ...late, assetId: 9999, assetName: 'late-retry.tar.gz', quality: 'PASS' as const };
  const audited = acceptStudyChunk(accepted, retry);
  assert.equal(audited.chunks.length, accepted.chunks.length + 1, 'late confirmed retry remains auditable');
  assert.equal(audited.updatedAt, accepted.updatedAt, 'late recovery cannot move ledger time backwards');
  assert.equal(audited.canonicalChunks[late.chunkId], undefined, 'late retry does not shift finalized selection');
  assert.deepEqual(selectStudyDayInputs(audited, bounds(date)).receipts.map(item => item.assetId), daily.chunkAssetIds);
  assert.equal(acceptStudyChunk(audited, retry).chunks.length, audited.chunks.length, 'recovery is idempotent');
});

test('the same incomplete-window selection respects the frozen HOLDOUT phase', () => {
  let ledger = createStudyLedger();
  for (let index = 0; index < 10; index += 1) {
    ledger = addDay(ledger, `2026-09-${String(14 + index).padStart(2, '0')}`, index * 2 + 1);
  }
  ledger = freezeStudy(ledger, { frozenAt: '2026-09-24T16:00:00.000Z',
    replayConfigHash: hex('f'), simulatorHashes: { simulator: hex('1') } });
  const date = '2026-09-25';
  ledger = prepareDay(ledger, date, 50);
  const late = ledger.chunks.find(item => item.chunkId === `${date}:late:1`)!;
  late.quality = 'INSUFFICIENT_DATA'; delete ledger.canonicalChunks[late.chunkId];
  const accepted = acceptStudyDay(ledger, dayReceipt(ledger, date, 50));
  assert.equal(accepted.days.at(-1)?.phase, 'HOLDOUT');
  assert.equal(accepted.phase, 'HOLDOUT');
});

test('a full day below the VKCO 80 percent threshold is recorded but never counted', () => {
  const date = '2026-09-14', ledger = prepareDay(createStudyLedger(), date, 1);
  const daily = dayReceipt(ledger, date, 1);
  daily.quality.perInstrument.find(item => item.ticker === 'VKCO')!.usableShare = .7737;
  assert.throws(() => acceptStudyDay(ledger, daily), /contradicts thresholds/);
  daily.quality.status = 'INSUFFICIENT_DATA';
  const rejected = acceptStudyDay(ledger, daily);
  assert.equal(rejected.phase, 'DEVELOPMENT');
  assert.equal(rejected.days[0]!.quality.status, 'INSUFFICIENT_DATA');
});

test('nine passing days plus one rejected full day cannot advance to freeze', () => {
  let ledger = createStudyLedger();
  for (let index = 0; index < 9; index += 1) {
    ledger = addDay(ledger, `2026-09-${String(14 + index).padStart(2, '0')}`, index * 2 + 1);
  }
  const date = '2026-09-23';
  ledger = prepareDay(ledger, date, 19);
  const rejected = dayReceipt(ledger, date, 19);
  rejected.quality.perInstrument.find(item => item.ticker === 'VKCO')!.usableShare = .79;
  rejected.quality.status = 'INSUFFICIENT_DATA';
  ledger = acceptStudyDay(ledger, rejected);
  assert.equal(ledger.days.length, 10, 'complete rejected day remains auditable');
  assert.equal(ledger.days.filter(day => day.quality.status === 'PASS').length, 9);
  assert.equal(ledger.phase, 'DEVELOPMENT');
  assert.throws(() => freezeStudy(ledger, { frozenAt: '2026-09-24T16:00:00.000Z',
    replayConfigHash: hex('f'), simulatorHashes: { simulator: hex('1') } }), /not ready to freeze/);
});

test('formal day fails closed on duplicate, mixed, overlapping or altered receipt inputs', () => {
  const date = '2026-09-14', ledger = prepareDay(createStudyLedger(), date, 1);
  const daily = dayReceipt(ledger, date, 1);
  assert.throws(() => acceptStudyDay(ledger, { ...daily, chunkAssetIds: [daily.chunkAssetIds[0]!, ...daily.chunkAssetIds] }), /invalid/);
  assert.throws(() => acceptStudyDay(ledger, { ...daily, canonicalInputHash: hex('9') }), /immutable full-day input/);
  const mixed = structuredClone(ledger);
  mixed.chunks.find(item => item.block === 'late')!.phase = 'HOLDOUT';
  assert.throws(() => acceptStudyDay(mixed, daily), /planned window and phase/);
  const overlapping = structuredClone(ledger);
  overlapping.chunks.find(item => item.block === 'late')!.plannedStart = `${date}T10:59:00.000Z`;
  assert.throws(() => acceptStudyDay(overlapping, daily), /planned window and phase/);
  const alteredArchive = structuredClone(ledger);
  alteredArchive.chunks[0]!.archiveSha256 = hex('9');
  assert.throws(() => acceptStudyDay(alteredArchive, daily), /immutable full-day input/);
  const missingCanonical = structuredClone(ledger);
  delete missingCanonical.canonicalChunks[missingCanonical.chunks[0]!.chunkId];
  assert.throws(() => acceptStudyDay(missingCanonical, daily), /lacks its canonical mapping/);
});

test('merge audits a late retry after a sealed day and rejects a day stale against an earlier committed retry', () => {
  const date = '2026-09-14', base = prepareDay(createStudyLedger(), date, 1);
  const first = base.chunks.find(item => item.chunkId === `${date}:late:1`)!;
  first.quality = 'INSUFFICIENT_DATA'; delete base.canonicalChunks[first.chunkId];
  const finalized = acceptStudyDay(base, dayReceipt(base, date, 1));
  let retryBranch = begin(base, date, 'late', 3);
  retryBranch = chunk(retryBranch, date, 'late', 3, 9999, 'PASS');
  const merged = mergeStudyLedgers(finalized, retryBranch);
  assert.equal(merged.chunks.length, base.chunks.length + 1);
  assert.equal(merged.days.length, 1);
  assert.equal(merged.canonicalChunks[first.chunkId], undefined);
  assert.equal(merged.days[0]!.chunkAssetIds.at(10), first.assetId);
  assert.throws(() => mergeStudyLedgers(retryBranch, finalized), /stale relative to the deterministic/);
});

test('a stale first day report cannot select a worse or better replay after PASS retry became canonical', () => {
  const date = '2026-09-14', base = prepareDay(createStudyLedger(), date, 1);
  const first = base.chunks.find(item => item.chunkId === `${date}:late:1`)!;
  first.quality = 'INSUFFICIENT_DATA'; delete base.canonicalChunks[first.chunkId];
  const staleReport = dayReceipt(base, date, 1);
  let retryBranch = begin(base, date, 'late', 3);
  retryBranch = chunk(retryBranch, date, 'late', 3, 9999, 'PASS');
  assert.throws(() => acceptStudyDay(retryBranch, { ...staleReport, canonicalInputHash: hex('9') }),
    /immutable full-day input/, 'a corrupted report cannot be classified as merely stale');
  assert.throws(() => acceptStudyDay(retryBranch, staleReport), /stale relative to the deterministic/);
  const validReport = dayReceipt(retryBranch, date, 2);
  assert.equal(acceptStudyDay(retryBranch, validReport).days[0]!.chunkAssetIds.at(10), 9999);
});
