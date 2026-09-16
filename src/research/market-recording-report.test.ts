import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import type { ObservationManifest } from './market-observation.js';
import { ObservationAccumulator, observationAnalysisProvenance } from './market-recording-report.js';
import { RecordingWriter, type RecordedEvent } from './market-recording.js';
import { diagnoseRecordingFreshness } from '../report/diagnose-freshness.js';

const start = Date.parse('2026-09-15T10:00:00Z');
const iso = (ms: number) => new Date(start + ms).toISOString();
function manifest(): ObservationManifest {
  return { schemaVersion: 1, runId: 'diagnostic-test', createdAt: iso(0), status: 'COMPLETE', endpoint: 'sandbox', source: 'exchange',
    instruments: [{ ticker: 'PRIVATE_TICKER', instrumentId: 'private-uid', uid: 'private-uid', figi: 'private-figi', classCode: 'TQBR', lot: 10, currency: 'RUB', name: 'Private name', exchange: 'TEST', sector: 'test' }],
    intervals: [{ exchange: 'TEST', type: 'regular_trading_session_main', start: iso(0), end: iso(10000) }],
    scheduleFetchedAt: iso(0), codeHashes: {}, notes: [], settings: {
      durationMs: 10000, depth: 20, budgetRub: 4000, commissionRate: .0005, maxBookAgeMs: 2000,
      maxFutureSkewMs: 1000, sampleIntervalMs: 1000, maxBytes: 1000000, fsyncEveryMs: 1000,
      subscriptionTimeoutMs: 1000, heartbeatTimeoutMs: 10000, maxReconnects: 1,
    } };
}
const book = (sourceMs: number) => ({ orderbook: { instrumentUid: 'private-uid', figi: 'private-figi', time: iso(sourceMs),
  isConsistent: true, depth: 20, orderBookType: 1,
  bids: [{ price: { units: 99, nano: 0 }, quantity: 100 }], asks: [{ price: { units: 100, nano: 0 }, quantity: 100 }] } });
const ack = { subscribeOrderBookResponse: { orderBookSubscriptions: [{ instrumentUid: 'private-uid', depth: 20, orderBookType: 1, subscriptionStatus: 1 }] } };
const status = { observationOrigin: 'UNARY_GET_TRADING_STATUSES', response: { tradingStatuses: [{ instrumentUid: 'private-uid', tradingStatus: 5 }] } };
function feed(acc: ObservationAccumulator) {
  let sequence = 0;
  return (kind: RecordedEvent['kind'], ms: number, payload: unknown = {}, epoch = 1, wallMs = ms) => acc.consume({
    schemaVersion: 1, runId: 'diagnostic-test', sequence: ++sequence, connectionEpoch: epoch,
    receivedAt: iso(wallMs), monotonicOffsetNs: String(ms * 1e6), kind, payload,
  });
}
function setup() {
  const acc = new ObservationAccumulator(manifest()), send = feed(acc);
  send('connect_attempt', 0); send('response', 1, ack); send('response', 2, status);
  return { acc, send };
}
const group = (acc: ObservationAccumulator) => acc.result().groups.find(g => g.source === 'EXCHANGE')!;

test('late at receipt and source expiry after fresh receipt remain distinct without admitting stale samples', () => {
  const { acc, send } = setup();
  send('response', 2000, book(500)); send('tick', 3000);
  send('response', 4900, { ping: { time: iso(4900) } }); send('tick', 5000);
  send('response', 6000, book(3500)); send('tick', 7000);
  const out = group(acc), d = out.freshnessDiagnostics;
  assert.equal(out.eligibleSamples, 0);
  assert.equal(out.sampleExclusions.STALE, 1);
  assert.equal(out.sampleExclusions.STALE_OR_PREVIOUS_CONNECTION, 1);
  assert.equal(out.sampleExclusions.NO_VALID_BOOK, 1);
  assert.equal(d.lateAtReceiptBooks, 1); assert.equal(d.usableAtReceiptBooks, 1);
  assert.equal(d.samplesExpiredBySourceAgeWithinReceiptLimit, 1);
  assert.equal(d.samplesExpiredByReceiptSilence, 1); assert.equal(d.samplesAfterLateAtReceiptBook, 1);
  assert.equal(d.sourceAgeAtReceiptMs.mean, 2000);
  assert.equal(d.streamResponseSilenceAtExpiredSampleMs.min, 100);
});

test('exact freshness boundary is preserved and diagnostics never create or replace a sample', () => {
  const { acc, send } = setup();
  send('response', 1000, book(0)); send('tick', 2000); send('tick', 2001);
  assert.equal(group(acc).eligibleSamples, 1);
  assert.equal(group(acc).freshnessDiagnostics.samplesExpiredBySourceAgeWithinReceiptLimit, 0);
  send('tick', 3000);
  assert.equal(group(acc).eligibleSamples, 1);
  assert.equal(group(acc).freshnessDiagnostics.samplesExpiredBySourceAgeWithinReceiptLimit, 1);
  assert.equal(acc.result().freshnessDiagnostics.ignoredDuplicateTicks, 1);
});

test('reconnect, gap and wall-clock resets do not turn disconnected time into a per-book receipt interval', () => {
  const { acc, send } = setup();
  send('response', 100, book(100)); send('disconnect', 1000); send('gap', 1001);
  send('connect_attempt', 4000, {}, 2); send('response', 4100, book(4100), 2); send('response', 4200, book(4200), 2);
  send('tick', 5000, {}, 2, 7000); // Observable local wall jump; prior book is invalidated.
  send('response', 5100, book(7100), 2, 7100); send('response', 5200, book(7200), 2, 7200);
  const d = group(acc).freshnessDiagnostics;
  assert.equal(d.bookReceiptIntervalMs.samples, 2); assert.equal(d.bookReceiptIntervalMs.max, 100);
  assert.equal(d.samplesWithoutCurrentBook, 1); assert.equal(acc.result().clockJumps, 1);
  assert.equal(acc.result().freshnessDiagnostics.connectionAttempts, 2);
});

test('rejected ordering, future time and unknown timestamps are not mislabeled as receipt silence', () => {
  const { acc, send } = setup();
  send('response', 100, book(100)); send('response', 200, book(50)); send('tick', 1000);
  send('response', 1100, book(5000)); send('tick', 2000);
  const missing = book(2100); delete (missing.orderbook as { time?: string }).time;
  send('response', 2100, missing); send('tick', 3000);
  const d = group(acc).freshnessDiagnostics;
  assert.equal(d.samplesAfterOtherRejectedBook, 3); assert.equal(d.samplesExpiredByReceiptSilence, 0);
  assert.equal(d.futureAtReceiptBooks, 1); assert.equal(d.missingTimestampBooks, 1);
  assert.equal(group(acc).eligibleSamples, 0);
});

test('timing quantiles are bounded and reproducible; counts and extrema include observations beyond reservoir size', () => {
  const run = () => {
    const { acc, send } = setup();
    for (let i = 1; i <= 5000; i++) send('response', i + 10, book(i));
    send('response', 5011, book(11));
    return group(acc).freshnessDiagnostics.sourceAgeAtReceiptMs;
  };
  const a = run(); assert.deepEqual(a, run());
  assert.equal(a.samples, 5001); assert.equal(a.quantileSamples, 4096);
  assert.equal(a.quantileMethod, 'DETERMINISTIC_RESERVOIR_4096'); assert.equal(a.max, 5000);
  assert.equal(a.mean! > 10, true);
});

test('read-only diagnostics bind integrity and current provenance, omit private payload fields, and protect archived report', async () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'freshness-diagnostic-test-'));
  try {
    const m = manifest(), writer = new RecordingWriter({ path: path.join(directory, 'events.ndjson'), runId: m.runId });
    writer.append('connect_attempt', {}, 1); writer.append('response', book(100), 1); writer.append('stop', {}, 1);
    m.recording = writer.close(); m.codeHashes = { ...observationAnalysisProvenance().sourceHashes, 'src/research/market-recording-report.ts': 'original-analyzer-hash' };
    const bytes = JSON.stringify(m); writeFileSync(path.join(directory, 'manifest.json'), bytes);
    writeFileSync(path.join(directory, 'summary.json'), 'original-summary');
    const result = await diagnoseRecordingFreshness(directory), encoded = JSON.stringify(result);
    assert.equal(result.originalAnalysisSourcesMatch, false); assert.equal(result.integrityVerified, true);
    assert.equal(result.groups[0].instrumentIndex, 0);
    for (const raw of ['PRIVATE_TICKER', 'private-uid', 'private-figi', 'Private name', 'orderbook', 'bids', 'asks']) assert.equal(encoded.includes(raw), false);
    assert.equal(readFileSync(path.join(directory, 'manifest.json'), 'utf8'), bytes);
    assert.equal(readFileSync(path.join(directory, 'summary.json'), 'utf8'), 'original-summary');
    m.recording.sha256 = '0'.repeat(64); writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify(m));
    await assert.rejects(diagnoseRecordingFreshness(directory), /integrity or completion mismatch/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
