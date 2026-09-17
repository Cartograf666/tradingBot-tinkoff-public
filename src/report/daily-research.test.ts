import assert from 'node:assert/strict';
import test from 'node:test';
import { buildDailyResearchReport, dailyResearchIdentity, selectDailyResearchInputs } from './daily-research.js';
import { fixedReplayScenarios, type ReplayReport } from './replay-orderbook.js';
import { acceptStudyChunk, beginStudyAttempt, createStudyLedger, type StudyChunkReceipt } from '../research/study-state.js';
import { planStudyBlock } from '../research/study-protocol.js';

const hex = (letter: string) => letter.repeat(64);
const day = '2026-09-15';
const plan = planStudyBlock(day, `${day}T06:00:00Z`, `${day}T15:54:59Z`, 'early', Date.parse(`${day}T05:50:00Z`))!;
function receipt(index: number, quality: StudyChunkReceipt['quality'] = 'PASS'): StudyChunkReceipt {
  const chunk = plan.chunks[index - 1]!;
  return { schemaVersion: 1, protocolHash: createStudyLedger().protocolHash, assetId: index, assetName: `chunk-${index}`,
    assetDigest: `sha256:${hex('a')}`, assetBytes: 1, archiveSha256: hex('b'), releaseTag: 'market-study-archive-v1',
    attemptId: `run-${index}:${index}`, chunkId: `${day}:early:${index}`, sessionDate: day, phase: 'DEVELOPMENT', block: 'early',
    chunkIndex: index, plannedStart: chunk.plannedStart, plannedEnd: chunk.plannedEnd, runId: `run-${index}`,
    manifestHash: hex('c'), recordingHash: hex('d'), replayConfigHash: hex('e'), simulatorHashes: { source: hex('f') }, quality,
    uploadedAt: `${day}T16:00:00.000Z` };
}
function closedSelection() {
  let ledger = createStudyLedger(), first = receipt(1), rejected = receipt(2, 'INSUFFICIENT_DATA');
  for (const item of [first, rejected]) {
    ledger = beginStudyAttempt(ledger, { runId: item.runId, runAttempt: item.chunkIndex, sessionDate: day,
      block: 'early', mode: 'COUNTED', startedAt: item.uploadedAt });
    ledger = acceptStudyChunk(ledger, item);
  }
  assert.equal(ledger.canonicalChunks[first.chunkId], first.assetId);
  assert.equal(ledger.canonicalChunks[rejected.chunkId], undefined);
  return selectDailyResearchInputs(ledger, plan, Date.parse(`${day}T16:00:00Z`));
}
const input = { replayConfigHash: hex('e'), simulatorHashes: { source: hex('f') }, fixedScenarios: fixedReplayScenarios(), reportVersionHash: hex('7') };
function replay(): ReplayReport {
  return { schemaVersion: 1, configHash: input.replayConfigHash, simulatorHashes: input.simulatorHashes, runtimeHashes: {},
    dataset: { scope: 'session', sessionDate: day, mainStart: plan.mainStart, mainEnd: plan.mainEnd, datasetHash: hex('1'), inputs: [], mappings: [] },
    quality: { status: 'INSUFFICIENT_DATA', recordedShare: .5, expectedTicks: 2, observedTicks: 1, perInstrument: [] },
    limitations: ['original limitation'], results: fixedReplayScenarios().map((scenario, index) => ({ name: scenario.name,
      strategy: index < 2 ? 'momentum' : 'exhaustion', scenario: index % 2 ? 'stress' : 'baseline', signals: index,
      entries: index + 1, closedTrades: index, netPnlRub: index === 0 ? null : index, realizedPnlRub: index,
      feesRub: index, profitFactor: null, maxDrawdownRub: index, unresolvedPositions: index === 0 ? 1 : 0, economicSuccess: false })) } as ReplayReport;
}

test('selects sorted PASS canonical receipts plus a production-like quality-rejected fallback after close', () => {
  const selected = closedSelection();
  assert.equal(selected.action, 'closed');
  assert.deepEqual(selected.receipts.map(item => item.chunkIndex), [1, 2]);
  assert.equal(selected.receipts[1]!.quality, 'INSUFFICIENT_DATA');
  assert.equal(selected.fullDayAccepted, false);
});

test('does not replay an open session and reports no input after close', () => {
  const ledger = createStudyLedger();
  assert.equal(selectDailyResearchInputs(ledger, plan, Date.parse(`${day}T12:00:00Z`)).action, 'open');
  assert.equal(selectDailyResearchInputs(ledger, plan, Date.parse(`${day}T16:00:00Z`)).action, 'noinput');
});

test('rejects a canonical asset whose receipt differs from the exact planned bounds', () => {
  const ledger = createStudyLedger(), item = receipt(1); item.plannedEnd = `${day}T16:00:00.000Z`;
  ledger.chunks.push(item); ledger.canonicalChunks[item.chunkId] = item.assetId;
  assert.throws(() => selectDailyResearchInputs(ledger, plan, Date.parse(`${day}T16:00:00Z`)), /planned bounds/);
});

test('identity includes inputs and implementation, while output keeps all fixed scenarios and null PnL', () => {
  const selected = closedSelection(); assert.equal(selected.action, 'closed');
  const original = dailyResearchIdentity(selected, input);
  assert.notEqual(original, dailyResearchIdentity(selected, { ...input, replayConfigHash: hex('9') }));
  const modified = { ...selected, receipts: [{ ...selected.receipts[0]!, archiveSha256: hex('8') }, ...selected.receipts.slice(1)] };
  assert.notEqual(original, dailyResearchIdentity(modified, input));
  const report = buildDailyResearchReport(selected, replay(), input);
  assert.equal(report.json.diagnosticOnly, true); assert.equal(report.json.counted, false); assert.equal(report.json.formalHoldout, false);
  assert.equal(report.json.fullDayAccepted, false); assert.equal(report.json.replay.results.length, 4);
  assert.equal(report.json.replay.results[0]!.netPnlRub, null);
  assert.match(report.markdown, /не определён/); assert.match(report.markdown, /реализованная.*не mark-to-market/);
});
