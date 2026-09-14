import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { MarketDataResponse, OrderBook } from 'tinkoff-invest-api/dist/generated/marketdata.js';
import { TradingSchedulesResponse } from 'tinkoff-invest-api/dist/generated/instruments.js';
import { normalizeIntervals, qualifyBook, classifyObservationSession, type ObservationManifest } from './market-observation.js';
import { ObservationAccumulator, reportMarketRecording, observationAnalysisProvenance } from './market-recording-report.js';
import { RecordingWriter, type RecordedEvent } from './market-recording.js';
import { parseRecorderArguments } from '../report/record-market.js';

const start = Date.parse('2026-09-13T10:00:00.000Z');
const iso = (offset: number) => new Date(start + offset).toISOString();
function manifest(): ObservationManifest {
  return {
    schemaVersion: 1, runId: 'test-run', createdAt: iso(0), status: 'COMPLETE', endpoint: 'sandbox', source: 'all',
    instruments: [{ ticker: 'TEST', instrumentId: 'uid', uid: 'uid', figi: 'figi', classCode: 'TQBR', lot: 10, currency: 'RUB', name: 'Test', exchange: 'MOEX_TEST', sector: 'test' }],
    intervals: [{ exchange: 'MOEX_TEST', type: 'dealer_holiday_trading_session_main', start: iso(-3600000), end: iso(3600000) }],
    scheduleFetchedAt: iso(0), codeHashes: {}, notes: [],
    settings: { durationMs: 10000, depth: 20, budgetRub: 4000, commissionRate: .0005, maxBookAgeMs: 2000,
      maxFutureSkewMs: 1000, sampleIntervalMs: 1000, maxBytes: 1000000, fsyncEveryMs: 1000,
      subscriptionTimeoutMs: 1000, heartbeatTimeoutMs: 10000, maxReconnects: 1 },
  };
}
const bookPayload = (offset = 100, source = 2) => ({ orderbook: {
  instrumentUid: 'uid', figi: 'figi', time: iso(offset), isConsistent: true, depth: 20, orderBookType: source,
  bids: [{ price: { units: 99, nano: 0 }, quantity: 100 }], asks: [{ price: { units: 100, nano: 0 }, quantity: 100 }],
} });
const ack = {
  subscribeOrderBookResponse: { orderBookSubscriptions: [{ instrumentUid: 'uid', depth: 20, orderBookType: 3, subscriptionStatus: 1 }] },
  subscribeTradesResponse: { tradeSource: 3, tradeSubscriptions: [{ instrumentUid: 'uid', subscriptionStatus: 1 }] },
  subscribeInfoResponse: { infoSubscriptions: [{ instrumentUid: 'uid', subscriptionStatus: 1 }] },
};
const statusSnapshot = (status = 14) => ({ observationOrigin: 'UNARY_GET_TRADING_STATUSES', response: { tradingStatuses: [{ instrumentUid: 'uid', tradingStatus: status }] } });
function feed(acc: ObservationAccumulator) {
  let sequence = 0;
  return (kind: RecordedEvent['kind'], offset: number, payload: unknown = {}, epoch = 1) => acc.consume({
    schemaVersion: 1, runId: 'test-run', sequence: ++sequence, connectionEpoch: epoch,
    receivedAt: iso(offset), monotonicOffsetNs: String(offset * 1e6), kind, payload,
  });
}

test('ACK and ping prove subscriptions but never create a market sample', () => {
  const acc = new ObservationAccumulator(manifest()), send = feed(acc);
  send('connect_attempt', 0); send('response', 10, ack); send('response', 100, { ping: { time: iso(100) } }); send('tick', 1000);
  const out = acc.result();
  assert.equal(out.allSubscriptionsAcknowledged, true); assert.equal(out.evidence, 'NO_USABLE_MARKET_SAMPLE');
  assert.equal(out.exchangeSamples, 0); assert.equal(out.dealerSamples, 0);
});

test('dealer observations stay separate; identical tape events are retained and lots remain lots', () => {
  const acc = new ObservationAccumulator(manifest()), send = feed(acc);
  send('connect_attempt', 0); send('response', 5, statusSnapshot()); send('response', 10, ack); send('response', 100, bookPayload());
  const trade = { trade: { instrumentUid: 'uid', figi: 'figi', time: iso(200), quantity: 2, price: { units: 100, nano: 0 }, direction: 1, tradeSource: 2 } };
  send('response', 200, trade); send('response', 201, trade); send('tick', 1000); send('stop', 1100);
  const out = acc.result(), dealer = out.groups.find(g => g.source === 'DEALER')!;
  assert.equal(out.exchangeSamples, 0); assert.equal(out.dealerSamples, 1);
  assert.equal(dealer.validTradeEvents, 2); assert.equal(dealer.observedTradeLots, 4);
  assert.equal(dealer.phases[0].lastAssessment?.quantityShares, 30);
});

test('disconnect invalidates the book; new ACKs do not resurrect a previous connection snapshot', () => {
  const acc = new ObservationAccumulator(manifest()), send = feed(acc);
  send('connect_attempt', 0); send('response', 5, statusSnapshot()); send('response', 10, ack); send('response', 100, bookPayload()); send('tick', 1000);
  send('disconnect', 1100); send('gap', 1101); send('connect_attempt', 1200, {}, 2); send('response', 1300, ack, 2); send('tick', 2000, {}, 2);
  assert.equal(acc.result().dealerSamples, 1);
  assert.equal(acc.result().groups.find(g => g.source === 'DEALER')!.sampleExclusions.NO_VALID_BOOK, 1);
});

test('missing, stale, inconsistent, misidentified and malformed books cannot become executable data', () => {
  const m = manifest();
  const book = OrderBook.fromJSON(bookPayload().orderbook);
  assert.equal(qualifyBook(book, m.instruments[0], start + 1000, m.settings).usable, true);
  const cases: [OrderBook, string][] = [
    [{ ...book, time: undefined }, 'MISSING_OR_INVALID_TIME'],
    [{ ...book, time: new Date(start - 5000) }, 'STALE'],
    [{ ...book, time: new Date(start + 10000) }, 'FUTURE_TIME'],
    [{ ...book, isConsistent: false }, 'INCONSISTENT'],
    [{ ...book, instrumentUid: 'other' }, 'UNEXPECTED_INSTRUMENT'],
    [{ ...book, orderBookType: 3 }, 'UNKNOWN_SOURCE'],
    [{ ...book, asks: book.bids }, 'INVALID_DEPTH_OR_PRICES'],
  ];
  for (const [b, reason] of cases) assert.deepEqual(qualifyBook(b, m.instruments[0], start + 1000, m.settings), { usable: false, reason });
});

test('API calendar distinguishes main session at 09:00 MSK and never infers one from broad hours', () => {
  const m = manifest();
  const r = TradingSchedulesResponse.fromJSON({ exchanges: [{ exchange: 'moex_test', days: [{ date: '2026-09-14T00:00:00Z', isTradingDay: true,
    startTime: '2026-09-14T04:00:00Z', endTime: '2026-09-14T20:00:00Z', intervals: [
      { type: 'regular_trading_session_main', interval: { startTs: '2026-09-14T06:00:00Z', endTs: '2026-09-14T15:54:59Z' } },
      { type: 'regular_trading_session', interval: { startTs: '2026-09-14T04:00:00Z', endTs: '2026-09-14T15:54:59Z' } },
    ] }] }] });
  const intervals = normalizeIntervals(r, ['MOEX_TEST']);
  const at = Date.parse('2026-09-14T06:01:00Z');
  assert.deepEqual(classifyObservationSession(m.instruments[0], at, 'EXCHANGE', intervals), { phase: 'regular_trading_session_main', regular: true, sourceMatches: true });
  assert.equal(classifyObservationSession(m.instruments[0], at, 'DEALER', intervals).sourceMatches, false);
  r.exchanges[0].days[0].intervals = [];
  assert.deepEqual(normalizeIntervals(r, ['MOEX_TEST']), []);
});

test('periodic sampling excludes stale books, out-of-order replacements and duplicate timer ticks', () => {
  const acc = new ObservationAccumulator(manifest()), send = feed(acc);
  send('connect_attempt', 0); send('response', 5, statusSnapshot()); send('response', 10, ack); send('response', 100, bookPayload());
  send('tick', 1000); send('tick', 1001); send('tick', 3000);
  assert.equal(acc.result().dealerSamples, 1); assert.equal(acc.result().samplingTicks, 2);
  send('response', 3100, bookPayload(3100)); send('response', 3200, bookPayload(3000)); send('tick', 4000);
  assert.equal(acc.result().sourceReorders, 1); assert.equal(acc.result().dealerSamples, 1);
});

test('saved report is reproducible and refuses changed recorded bytes', async () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'market-observation-report-'));
  try {
    const m = manifest(), writer = new RecordingWriter({ path: path.join(directory, 'events.ndjson'), runId: m.runId });
    writer.append('connect_attempt', {}, 1); writer.append('response', MarketDataResponse.fromJSON(ack), 1); writer.append('stop', { reason: 'duration' }, 1);
    m.recording = writer.close(); m.codeHashes = observationAnalysisProvenance().sourceHashes; writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify(m));
    await reportMarketRecording(directory); const first = readFileSync(path.join(directory, 'summary.json'), 'utf8');
    await reportMarketRecording(directory); assert.equal(readFileSync(path.join(directory, 'summary.json'), 'utf8'), first);
    m.recording.sha256 = 'changed'; writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify(m));
    await assert.rejects(reportMarketRecording(directory), /checksum/);
    m.codeHashes['src/research/market-observation.ts'] = 'different-analysis-version'; writeFileSync(path.join(directory, 'manifest.json'), JSON.stringify(m));
    await assert.rejects(reportMarketRecording(directory), /Analysis source differs/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('unknown or halted status is excluded; every reconnect needs its own observed status', () => {
  const acc = new ObservationAccumulator(manifest()), send = feed(acc);
  send('connect_attempt', 0); send('response', 10, ack); send('response', 100, bookPayload()); send('tick', 1000);
  assert.equal(acc.result().dealerSamples, 0);
  send('response', 1100, statusSnapshot(15)); send('tick', 2000);
  assert.equal(acc.result().dealerSamples, 0);
  send('response', 2100, statusSnapshot(14)); send('response', 2200, bookPayload(2200)); send('tick', 3000);
  assert.equal(acc.result().dealerSamples, 1);
  send('disconnect', 3100); send('connect_attempt', 3200, {}, 2); send('response', 3300, ack, 2);
  send('response', 3400, bookPayload(3400), 2); send('tick', 4000, {}, 2);
  assert.equal(acc.result().dealerSamples, 1);
});

test('different regular session phases retain separate cost distributions', () => {
  const m = manifest(); m.intervals = [
    { exchange: 'MOEX_TEST', type: 'regular_trading_session_morning', start: iso(0), end: iso(1500) },
    { exchange: 'MOEX_TEST', type: 'regular_trading_session_main', start: iso(1501), end: iso(10000) },
  ];
  const acc = new ObservationAccumulator(m), send = feed(acc);
  send('connect_attempt', 0); send('response', 5, statusSnapshot(5)); send('response', 10, ack);
  send('response', 100, bookPayload(100, 1)); send('tick', 1000);
  const secondBook = bookPayload(1800, 1); secondBook.orderbook.bids[0].price.units = 98;
  send('response', 1800, secondBook); send('tick', 2000);
  const phases = acc.result().groups.find(g => g.source === 'EXCHANGE')!.phases;
  assert.equal(phases.length, 2); assert.equal(phases[0].eligibleSamples, 1); assert.equal(phases[1].eligibleSamples, 1);
  assert.notEqual(phases[0].spreadBps.median, phases[1].spreadBps.median);
});

test('recorder argument validation keeps a finite sandbox observation and rejects ambiguous input', () => {
  assert.equal(parseRecorderArguments([], '/tmp').source, 'exchange');
  assert.equal(parseRecorderArguments(['--commission-pct', '0.05'], '/tmp').commissionRate, .0005);
  for (const args of [['--seconds', 'Infinity'], ['--seconds', '0'], ['--depth', '2'], ['--tickers', 'SBER,SBER'], ['--source', 'production'], ['--seconds', '2', '--seconds', '3']]) assert.throws(() => parseRecorderArguments(args, '/tmp'));
  assert.equal(parseRecorderArguments(['--session', 'main'], '/tmp').session, 'main');
  assert.equal(parseRecorderArguments(['--max-mb', '1'], '/tmp').segmentMaxBytes, 1024 * 1024);
  for (const args of [['--session', 'main', '--source', 'all'], ['--segment-mb', '0'], ['--segment-mb', '2', '--max-mb', '1']]) assert.throws(() => parseRecorderArguments(args, '/tmp'));
});

test('coverage retains empty phases and disconnected ticks, while timer gaps stay explicit', () => {
  const acc = new ObservationAccumulator(manifest()), send = feed(acc);
  send('connect_attempt', 0); send('response', 5, statusSnapshot()); send('response', 10, ack);
  send('tick', 1000); send('response', 1800, bookPayload(1800)); send('tick', 2000);
  send('disconnect', 2100); send('tick', 5000); send('stop', 5100);
  const out = acc.result(), phase = out.groups.find(g => g.source === 'DEALER')!.phases[0];
  assert.equal(phase.observedScheduledTicks, 3); assert.equal(phase.eligibleSamples, 1);
  assert.equal(phase.usableShareOfObservedScheduledTicks, 1 / 3);
  assert.equal(phase.sampleExclusions.NO_VALID_BOOK, 1); assert.equal(phase.sampleExclusions.DISCONNECTED, 1);
  assert.deepEqual(out.samplingCoverage, { expectedTicksFromElapsedTime: 5, recordedTicks: 3, missingTicks: 2, recordedShare: .6 });
});

test('report recovers every crash segment without claiming finalized integrity or changing the manifest', async () => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'observation-crash-'));
  try {
    const m = manifest(); m.status = 'RECORDING'; m.settings.segmentMaxBytes = 2000;
    m.codeHashes = observationAnalysisProvenance().sourceHashes;
    const writer = new RecordingWriter({ path: path.join(directory, 'events.ndjson'), runId: m.runId, segmentMaxBytes: 2000 });
    writer.append('connect_attempt', {}, 1);
    for (let n = 0; n < 8; n++) writer.append('response', { unused: 'x'.repeat(600) }, 1);
    const written = writer.close(); // Simulate missing close metadata; deliberately no stop event.
    assert.ok(written.segments!.length > 1);
    const manifestPath = path.join(directory, 'manifest.json'), before = JSON.stringify(m);
    writeFileSync(manifestPath, before);
    const out = await reportMarketRecording(directory);
    assert.equal(out.integrity.events, written.events); assert.equal(out.integrity.sha256, written.sha256);
    assert.equal(out.capacity.segmentCount, written.segments!.length);
    assert.deepEqual(out.recovery, { recoveredUnfinalized: true, comparedWithFinalManifest: false });
    assert.equal(out.complete, false); assert.equal(readFileSync(manifestPath, 'utf8'), before);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
