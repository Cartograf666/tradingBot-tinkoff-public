import assert from 'node:assert/strict';
import test, { type TestContext } from 'node:test';
import { mkdtempSync, writeFileSync, mkdirSync, rmSync, readFileSync, existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { consumeSessionChunks, inspectReplayChunk, validateSessionChunks, SessionCoverage } from './session-replay.js';
import { OrderBookSimulator, defaultSimulationConfig } from './order-book-simulator.js';
import type { ObservationManifest } from './market-observation.js';
import type { RecordedEvent } from './market-recording.js';
import { archiveDiagnosticRecording } from '../report/market-study.js';

const start = Date.parse('2026-09-14T06:00:00Z');
const iso = (ms: number) => new Date(start + ms).toISOString();
const instrument = { ticker: 'A', instrumentId: 'a', uid: 'a', figi: 'figi-a', classCode: 'TQBR', lot: 10, currency: 'RUB', name: 'A', exchange: 'MOEX', sector: 'test' };
function manifest(runId: string, durationMs = 10_000): ObservationManifest {
  return { schemaVersion: 1, runId, status: 'COMPLETE', createdAt: iso(0), endpoint: 'sandbox', source: 'exchange',
    instruments: [instrument], intervals: [{ exchange: 'MOEX', type: 'regular_trading_session_main', start: iso(0), end: iso(20_000) }],
    scheduleFetchedAt: iso(0), codeHashes: {}, notes: [], settings: { durationMs, depth: 20, budgetRub: 4_000, commissionRate: .0005,
      maxBookAgeMs: 2_000, maxFutureSkewMs: 1_000, sampleIntervalMs: 1_000, maxBytes: 1_000_000, fsyncEveryMs: 1_000,
      subscriptionTimeoutMs: 10_000, heartbeatTimeoutMs: 20_000, maxReconnects: 5 } };
}
const acks = {
  subscribeOrderBookResponse: { orderBookSubscriptions: [{ instrumentUid: 'a', depth: 20, orderBookType: 1, subscriptionStatus: 1 }] },
  subscribeTradesResponse: { tradeSource: 1, tradeSubscriptions: [{ instrumentUid: 'a', subscriptionStatus: 1 }] },
  subscribeInfoResponse: { infoSubscriptions: [{ instrumentUid: 'a', subscriptionStatus: 1 }] },
};
const statuses = { observationOrigin: 'UNARY_GET_TRADING_STATUSES', response: { tradingStatuses: [{ instrumentUid: 'a', tradingStatus: 5 }] } };
function book(at: number, bid = 99, ask = 100) { return { orderbook: { instrumentUid: 'a', figi: 'figi-a', time: iso(at), isConsistent: true, depth: 20, orderBookType: 1,
  bids: [{ price: { units: bid, nano: 0 }, quantity: 100 }], asks: [{ price: { units: ask, nano: 0 }, quantity: 100 }] } }; }
type Frame = [RecordedEvent['kind'], number, unknown?];
function frames(begin: number): Frame[] { return [['connect_attempt', begin], ['response', begin + 1, statuses], ['response', begin + 2, acks]]; }
function writeChunk(root: string, id: string, input: Frame[], clockJump = false) {
  const directory = path.join(root, id); mkdirSync(directory);
  const events = input.map(([kind, at, payload = {}], i): RecordedEvent => ({ schemaVersion: 1, runId: id, sequence: i + 1,
    connectionEpoch: 1, receivedAt: iso(at + (clockJump && i === input.length - 1 ? 5_000 : 0)),
    monotonicOffsetNs: String((at - input[0][1]) * 1e6), kind, payload }));
  const bytes = Buffer.from(events.map(e => JSON.stringify(e)).join('\n') + '\n');
  const m = manifest(id); m.recording = { events: events.length, bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') };
  writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify(m)); writeFileSync(path.join(directory, 'events.ndjson'), bytes);
  return directory;
}
const setup = (t: TestContext) => { const root = mkdtempSync(path.join(tmpdir(), 'session-replay-')); t.after(() => rmSync(root, { recursive: true, force: true })); return root; };

test('diagnostic finalization archives real replay beside immutable raw files', async t => {
  const root = setup(t), directory = writeChunk(root, 'diagnostic', [...frames(0), ['response', 100, book(100)], ['tick', 1000], ['stop', 2000]]);
  writeFileSync(path.join(directory, 'summary.json'), '{}');
  const rawBefore = readFileSync(path.join(directory, 'events.ndjson'));
  const manifestBefore = readFileSync(path.join(directory, 'manifest.json'));
  const result = await archiveDiagnosticRecording(directory, start + 20_000);
  assert.equal(result.status, 'COMPLETE'); assert.equal(result.replayFailed, false);
  assert.deepEqual(readFileSync(path.join(directory, 'events.ndjson')), rawBefore);
  assert.deepEqual(readFileSync(path.join(directory, 'manifest.json')), manifestBefore);
  assert.equal(existsSync(path.join(directory, 'replay')), false);
  const entries = execFileSync('tar', ['-tzf', result.archive], { encoding: 'utf8' });
  assert.match(entries, /diagnostic\/events\.ndjson/);
  assert.match(entries, /diagnostic-replay\/replay\.json/);
  const receipt = JSON.parse(execFileSync('tar', ['-xOzf', result.archive, 'diagnostic/diagnostic-observation.json'], { encoding: 'utf8' }));
  assert.equal(receipt.replayFailed, false); assert.equal(receipt.counted, false);
  assert.equal(receipt.replayDirectory, 'diagnostic-replay');
});

test('diagnostic replay integrity failure preserves raw archive and remains a failure', async t => {
  const root = setup(t), directory = writeChunk(root, 'damaged', [...frames(0), ['stop', 2000]]);
  writeFileSync(path.join(directory, 'summary.json'), '{}');
  writeFileSync(path.join(directory, 'events.ndjson'), 'corrupt');
  const result = await archiveDiagnosticRecording(directory, start + 20_000);
  assert.equal(result.replayFailed, true);
  assert.equal(execFileSync('tar', ['-xOzf', result.archive, 'damaged/events.ndjson'], { encoding: 'utf8' }), 'corrupt');
  const receipt = JSON.parse(execFileSync('tar', ['-xOzf', result.archive, 'damaged/diagnostic-observation.json'], { encoding: 'utf8' }));
  assert.equal(receipt.failureStage, 'replay'); assert.equal(receipt.counted, false);
});

test('a position bought in chunk A closes in B with one capital and nonzero exit latency', async t => {
  const root = setup(t);
  const a = await inspectReplayChunk(writeChunk(root, 'a', [...frames(0), ['response', 100, book(100)], ['response', 200, book(200)], ['stop', 1_000]]));
  const b = await inspectReplayChunk(writeChunk(root, 'b', [...frames(2_000), ['response', 2_100, book(2_100, 110, 111)], ['response', 2_300, book(2_300, 110, 111)], ['stop', 3_000]]));
  const m = manifest('day');
  const sim = new OrderBookSimulator(m, defaultSimulationConfig('momentum', { warmupMs: 0, latencyMs: 100,
    closeBeforeEndMs: 0, maxEntries: 1, signalHook: () => true, stopLossBps: 99_999, takeProfitBps: 35 }));
  const mappings = await consumeSessionChunks(validateSessionChunks([b, a], start, start + 10_000), m, start, e => sim.consume(e));
  const result = sim.finish();
  assert.equal(result.closedTrades.length, 1); assert.equal(result.openPositions.length, 0);
  assert.equal(result.fills[1].atMs, start + 2_300);
  assert.equal(result.cashRub, result.config.initialCashRub + result.realizedPnlRub);
  assert.equal(mappings.length, 2); assert.ok(mappings[1].dailySequenceStart > mappings[0].dailySequenceEnd);
});

test('missing chunks retain the whole-day denominator and repeated ticks cannot double count a second', async t => {
  const root = setup(t);
  const a = await inspectReplayChunk(writeChunk(root, 'a', [...frames(2_000), ['response', 2_100, book(2_100)],
    ['tick', 2_200], ['tick', 2_300], ['response', 3_000, book(3_000)], ['tick', 3_100], ['stop', 4_000]]));
  const m = manifest('day'), coverage = new SessionCoverage(m, start, start + 10_000);
  await consumeSessionChunks([a], m, start, e => coverage.consume(e));
  const quality = coverage.result();
  assert.equal(quality.expectedTicks, 10); assert.equal(quality.observedTicks, 2);
  assert.equal(quality.recordedShare, .2); assert.equal(quality.perInstrument[0].usableShare, .2);
  assert.equal(quality.status, 'INSUFFICIENT_DATA');
});

test('late capture never moves the day-end entry cutoff forward', async t => {
  const root = setup(t);
  const a = await inspectReplayChunk(writeChunk(root, 'a', [...frames(9_000), ['response', 9_100, book(9_100)], ['response', 9_200, book(9_200)], ['stop', 9_500]]));
  const m = manifest('day'), sim = new OrderBookSimulator(m, defaultSimulationConfig('momentum', {
    warmupMs: 0, closeBeforeEndMs: 2_000, latencyMs: 1, signalHook: () => true }));
  await consumeSessionChunks([a], m, start, e => sim.consume(e));
  assert.equal(sim.finish().fills.length, 0);
});

test('overlaps, duplicate inputs, incompatible metadata and clock jumps are rejected', async t => {
  const root = setup(t);
  const a = await inspectReplayChunk(writeChunk(root, 'a', [...frames(0), ['stop', 2_000]]));
  const b = await inspectReplayChunk(writeChunk(root, 'b', [...frames(1_000), ['stop', 3_000]]));
  assert.throws(() => validateSessionChunks([a, b], start, start + 10_000), /Overlapping/);
  assert.throws(() => validateSessionChunks([a, a], start, start + 10_000), /Duplicate/);
  b.manifest.settings.depth = 10;
  assert.throws(() => validateSessionChunks([a, b], start, start + 10_000), /Incompatible/);
  await assert.rejects(inspectReplayChunk(writeChunk(root, 'jump', [...frames(0), ['stop', 2_000]], true)), /clock jump/);
});

test('archive mutation fails its original checksum', async t => {
  const root = setup(t), directory = writeChunk(root, 'a', [...frames(0), ['stop', 2_000]]);
  const m = manifest('a'); m.recording = { events: 4, bytes: 100, sha256: '0'.repeat(64) };
  writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify(m));
  await assert.rejects(inspectReplayChunk(directory), /integrity/);
});

test('a source-time reversal invalidates the current coverage book until a newer update', async t => {
  const root = setup(t), lateBook = book(1_050);
  const a = await inspectReplayChunk(writeChunk(root, 'a', [...frames(0), ['response', 1_100, book(1_100)],
    ['response', 1_200, lateBook], ['tick', 1_300], ['response', 2_100, book(2_100)], ['tick', 2_200], ['stop', 3_000]]));
  const m = manifest('day'), coverage = new SessionCoverage(m, start, start + 10_000);
  await consumeSessionChunks([a], m, start, event => coverage.consume(event));
  assert.equal(coverage.result().observedTicks, 2);
  assert.equal(coverage.result().perInstrument[0].usableTicks, 1);
});
