import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import test, { type TestContext } from 'node:test';
import { assessContinuousWindows } from './continuous-windows.js';
import type { ObservationInstrument, ObservationManifest } from './market-observation.js';
import type { RecordedEvent, RecordedEventKind } from './market-recording.js';

const start = Date.parse('2026-09-14T06:00:00.000Z');
const iso = (offset: number) => new Date(start + offset).toISOString();
const instruments: ObservationInstrument[] = ['A', 'B', 'C', 'D', 'E', 'F'].map((ticker, index) => ({
  ticker, instrumentId: `id-${index}`, uid: `uid-${index}`, figi: `figi-${index}`, classCode: 'TQBR', lot: 1,
  currency: 'RUB', name: ticker, exchange: 'MOEX', sector: 'test',
}));
const plans = [
  { index: 1, plannedStart: iso(0), plannedEnd: iso(10_000) },
  { index: 2, plannedStart: iso(10_000), plannedEnd: iso(20_000) },
];

function manifest(runId: string, durationMs: number): ObservationManifest {
  return {
    schemaVersion: 1, runId, createdAt: iso(0), status: 'COMPLETE', endpoint: 'sandbox', source: 'exchange',
    // Calendar confirmation is independent from how much of that window the
    // recorder actually captured; coverage supplies the fixed denominator.
    instruments, intervals: [{ exchange: 'MOEX', type: 'regular_trading_session_main', start: iso(-1_000), end: iso(Math.max(durationMs + 1_000, 1_801_000)) }],
    scheduleFetchedAt: iso(0), codeHashes: {}, notes: [], settings: {
      durationMs, depth: 20, budgetRub: 4_000, commissionRate: .0005, maxBookAgeMs: 2_000, maxFutureSkewMs: 1_000,
      sampleIntervalMs: 1_000, maxBytes: 1_000_000, fsyncEveryMs: 1_000, subscriptionTimeoutMs: 10_000,
      heartbeatTimeoutMs: 20_000, maxReconnects: 5, session: 'main',
    },
  };
}

function acknowledgements() {
  return {
    subscribeOrderBookResponse: { orderBookSubscriptions: instruments.map(i => ({ instrumentUid: i.uid, depth: 20, orderBookType: 1, subscriptionStatus: 1 })) },
    subscribeTradesResponse: { tradeSource: 1, tradeSubscriptions: instruments.map(i => ({ instrumentUid: i.uid, subscriptionStatus: 1 })) },
    subscribeInfoResponse: { infoSubscriptions: instruments.map(i => ({ instrumentUid: i.uid, subscriptionStatus: 1 })) },
  };
}
function book(instrument: ObservationInstrument, at: number) {
  return { orderbook: { instrumentUid: instrument.uid, figi: instrument.figi, time: iso(at), isConsistent: true, depth: 20, orderBookType: 1,
    bids: [{ price: { units: 100, nano: 0 }, quantity: 100 }], asks: [{ price: { units: 101, nano: 0 }, quantity: 100 }] } };
}
function writeRun(root: string, runId: string, seconds: number, omitUid?: string, firstEventOffset = 0): string {
  const directory = path.join(root, runId); mkdirSync(directory);
  const frames: Array<[RecordedEventKind, number, unknown]> = [
    ['connect_attempt', firstEventOffset, {}],
    ['response', firstEventOffset + 1, { observationOrigin: 'UNARY_GET_TRADING_STATUSES', response: { tradingStatuses: instruments.map(i => ({ instrumentUid: i.uid, tradingStatus: 5 })) } }],
    ['response', firstEventOffset + 2, acknowledgements()],
  ];
  for (let second = 0; second < seconds; second += 1) {
    const bookAt = Math.max(firstEventOffset + 3, second * 1_000 + 100);
    for (const instrument of instruments) if (instrument.uid !== omitUid) frames.push(['response', bookAt, book(instrument, bookAt)]);
    frames.push(['tick', second * 1_000 + 500, {}]);
  }
  frames.push(['stop', seconds * 1_000, { reason: 'duration' }]);
  const events: RecordedEvent[] = frames.map(([kind, at, payload], index) => ({ schemaVersion: 1, runId, sequence: index + 1,
    connectionEpoch: 1, receivedAt: iso(at), monotonicOffsetNs: String(at * 1_000_000), kind, payload }));
  const bytes = Buffer.from(events.map(event => JSON.stringify(event)).join('\n') + '\n');
  const m = manifest(runId, seconds * 1_000);
  m.capture = { reason: 'duration', epochs: 1, responses: events.filter(event => event.kind === 'response').length };
  m.recording = { events: events.length, bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') };
  writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify(m));
  writeFileSync(path.join(directory, 'events.ndjson'), bytes);
  return directory;
}
function setup(t: TestContext): string {
  const root = mkdtempSync(path.join(tmpdir(), 'continuous-windows-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

test('one continuous receipt-ordered stream produces independent planned-window coverage', async t => {
  const directory = writeRun(setup(t), 'full', 20);
  const result = await assessContinuousWindows(directory, plans);
  assert.deepEqual(result.map(window => window.quality), ['PASS', 'PASS']);
  assert.deepEqual(result.map(window => window.coverage.observedTicks), [10, 10]);
  assert.deepEqual(result.map(window => window.coverage.expectedTicks), [10, 10]);
  assert.ok(result.every(window => window.checks.captureCompletedByDuration));
  assert.ok(result.every(window => window.coverage.perInstrument.every(instrument => instrument.usableShare === 1)));
});

test('future events cannot alter an already eligible planned window', async t => {
  const root = setup(t);
  const before = await assessContinuousWindows(writeRun(root, 'before', 20), plans);
  const after = await assessContinuousWindows(writeRun(root, 'after', 30), plans);
  assert.deepEqual(after, before);
});

test('one bad instrument marks each window insufficient but preserves diagnostic coverage for the rest', async t => {
  const result = await assessContinuousWindows(writeRun(setup(t), 'missing', 20, instruments[5].uid), plans);
  assert.deepEqual(result.map(window => window.quality), ['INSUFFICIENT_DATA', 'INSUFFICIENT_DATA']);
  assert.ok(result.every(window => window.coverage.perInstrument.slice(0, 5).every(instrument => instrument.usableShare === 1)));
  assert.ok(result.every(window => window.coverage.perInstrument[5].usableShare === 0));
  assert.ok(result.every(window => window.checks.timerCoverage && !window.checks.perInstrumentCoverage));
});

test('a 100ms late start and a five-second short tail still pass the fixed 99% 30-minute denominator', async t => {
  const halfHour = [{ index: 1, plannedStart: iso(0), plannedEnd: iso(1_800_000) }];
  const result = await assessContinuousWindows(writeRun(setup(t), 'tolerated-tail', 1_795, undefined, 100), halfHour);
  assert.equal(result[0].quality, 'PASS');
  assert.equal(result[0].coverage.expectedTicks, 1_800);
  assert.ok(result[0].coverage.recordedShare > .99);
});

test('a meaningful 60-second short tail fails the same fixed denominator', async t => {
  const halfHour = [{ index: 1, plannedStart: iso(0), plannedEnd: iso(1_800_000) }];
  const result = await assessContinuousWindows(writeRun(setup(t), 'short-tail', 1_740, undefined, 100), halfHour);
  assert.equal(result[0].quality, 'INSUFFICIENT_DATA');
  assert.ok(result[0].coverage.recordedShare < .99);
});

test('overlapping planned windows are rejected before raw data are reinterpreted', async t => {
  const directory = writeRun(setup(t), 'overlap', 20);
  await assert.rejects(assessContinuousWindows(directory, [plans[0], { index: 3, plannedStart: iso(9_999), plannedEnd: iso(15_000) }]), /overlap/);
});
