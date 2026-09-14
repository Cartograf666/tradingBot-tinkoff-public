import assert from 'node:assert/strict';
import test from 'node:test';
import { defaultSimulationConfig, OrderBookSimulator } from './order-book-simulator.js';
import type { ObservationManifest } from './market-observation.js';
import type { RecordedEvent } from './market-recording.js';

const start = Date.parse('2026-09-14T06:00:00.000Z');
const iso = (offset: number) => new Date(start + offset).toISOString();
function manifest(two = false): ObservationManifest {
  const instruments = ['a', ...(two ? ['b'] : [])].map(uid => ({ ticker: uid.toUpperCase(), instrumentId: uid, uid, figi: `figi-${uid}`, classCode: 'TQBR', lot: 10, currency: 'RUB', name: uid, exchange: 'MOEX', sector: 'test' }));
  return {
    schemaVersion: 1, runId: 'sim-run', createdAt: iso(0), status: 'COMPLETE', endpoint: 'sandbox', source: 'exchange', instruments,
    intervals: [{ exchange: 'MOEX', type: 'regular_trading_session_main', start: iso(-10_000), end: iso(300_000) }], scheduleFetchedAt: iso(0), codeHashes: {}, notes: [],
    settings: { durationMs: 100_000, depth: 20, budgetRub: 4_000, commissionRate: .0005, maxBookAgeMs: 2_000, maxFutureSkewMs: 1_000, sampleIntervalMs: 1_000, maxBytes: 1, fsyncEveryMs: 0, subscriptionTimeoutMs: 1, heartbeatTimeoutMs: 1, maxReconnects: 0 },
  };
}
function feed(sim: OrderBookSimulator) {
  let sequence = 0;
  return (kind: RecordedEvent['kind'], offset: number, payload: unknown = {}, epoch = 1) => sim.consume({ schemaVersion: 1, runId: 'sim-run', sequence: ++sequence, connectionEpoch: epoch, receivedAt: iso(offset), monotonicOffsetNs: String(offset * 1e6), kind, payload });
}
function ack(uids: string[]) { return {
  subscribeOrderBookResponse: { orderBookSubscriptions: uids.map(instrumentUid => ({ instrumentUid, depth: 20, orderBookType: 1, subscriptionStatus: 1 })) },
  subscribeTradesResponse: { tradeSource: 1, tradeSubscriptions: uids.map(instrumentUid => ({ instrumentUid, subscriptionStatus: 1 })) },
  subscribeInfoResponse: { infoSubscriptions: uids.map(instrumentUid => ({ instrumentUid, subscriptionStatus: 1 })) },
}; }
function statuses(uids: string[]) { return { observationOrigin: 'UNARY_GET_TRADING_STATUSES', response: { tradingStatuses: uids.map(instrumentUid => ({ instrumentUid, tradingStatus: 5 })) } }; }
function book(uid: string, offset: number, bid = 99, ask = 100, bidLots = 10, askLots = 10) { return { orderbook: {
  instrumentUid: uid, figi: `figi-${uid}`, time: iso(offset), isConsistent: true, depth: 20, orderBookType: 1,
  bids: [{ price: { units: bid, nano: 0 }, quantity: bidLots }], asks: [{ price: { units: ask, nano: 0 }, quantity: askLots }],
} }; }
function ready(sim: OrderBookSimulator, uids = ['a']) { const send = feed(sim); send('connect_attempt', 0); send('response', 1, statuses(uids)); send('response', 2, ack(uids)); return send; }

test('uses the next received eligible book after latency, never the signal book', () => {
  const sim = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', { warmupMs: 0, closeBeforeEndMs: 0, latencyMs: 300, signalHook: () => true, maxHoldingMs: 99_999, takeProfitBps: 99_999, stopLossBps: 99_999 }));
  const send = ready(sim);
  send('response', 100, book('a', 100, 99, 100));
  send('response', 200, book('a', 200, 99, 101));
  send('response', 500, book('a', 500, 100, 102));
  const out = sim.finish();
  const entry = out.fills.find(fill => fill.side === 'BUY')!;
  assert.equal(entry.atMs, start + 500);
  assert.equal(entry.vwap, 102);
});

test('uses whole lots, shares one cash balance, and records partial displayed-depth entry fees', () => {
  const sim = new OrderBookSimulator(manifest(true), defaultSimulationConfig('momentum', { initialCashRub: 4_500, budgetPerTradeRub: 4_000, commissionRate: .001, warmupMs: 0, closeBeforeEndMs: 0, latencyMs: 1, signalHook: () => true, maxHoldingMs: 99_999, takeProfitBps: 99_999, stopLossBps: 99_999 }));
  const send = ready(sim, ['a', 'b']);
  send('response', 100, book('a', 100, 99, 100, 10, 1)); // reserves 4,000; desired 3 lots but only one displayed
  send('response', 101, book('b', 101)); // only 500 remains: no one 10-share lot
  send('response', 200, book('a', 200, 99, 100, 10, 1));
  const out = sim.finish();
  assert.equal(out.fills[0].lots, 1);
  assert.equal(out.fills[0].shares, 10);
  assert.equal(out.fills[0].grossRub, 1_000);
  assert.equal(out.fills[0].feeRub, 1);
  assert.equal(out.modelOrders[0].status, 'PARTIAL');
  assert.equal(out.modelOrders[0].cancelledLots, 2);
  assert.equal(out.rejections.INSUFFICIENT_SHARED_CASH, 1);
  assert.equal(out.openPositions[0].lots, 1);
});

test('a gap cancels pending entries, invalidates books, and leaves an opened position unresolved without an invented close', () => {
  const sim = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', { warmupMs: 0, closeBeforeEndMs: 0, latencyMs: 1, signalHook: () => true, maxHoldingMs: 99_999 }));
  const send = ready(sim);
  send('response', 100, book('a', 100)); send('response', 200, book('a', 200)); // entry fills
  send('gap', 201);
  send('response', 202, book('a', 202)); // no ACK/status after gap, cannot exit or fabricate price
  const out = sim.finish();
  assert.equal(out.closedTrades.length, 0);
  assert.equal(out.openPositions.length, 1);
  assert.equal(out.rejections.PENDING_EXIT_CANCELLED_GAP, 1);
  assert.match(out.unresolved[0], /no fabricated end price/);
});

test('sell IOC also waits for its latency and cannot execute on the exit signal snapshot', () => {
  const sim = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', { warmupMs: 0, closeBeforeEndMs: 0, latencyMs: 100, signalHook: () => true, maxHoldingMs: 99_999 }));
  const send = ready(sim);
  send('response', 100, book('a', 100));
  send('response', 250, book('a', 250)); // buy arrives and fills; stop-loss creates an exit arriving at 350
  send('response', 300, book('a', 300, 90, 91));
  send('response', 400, book('a', 400, 89, 90));
  const sell = sim.finish().fills.find(fill => fill.side === 'SELL')!;
  assert.equal(sell.atMs, start + 400);
  assert.equal(sell.vwap, 89);
});

test('partial exit charges only the sold lot share of entry cost and leaves the remainder open', () => {
  const sim = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', { budgetPerTradeRub: 4_000, commissionRate: .001, warmupMs: 0, closeBeforeEndMs: 0, latencyMs: 1, signalHook: () => true, maxHoldingMs: 99_999 }));
  const send = ready(sim);
  send('response', 100, book('a', 100, 99, 100, 10, 3));
  send('response', 200, book('a', 200, 99, 100, 10, 3)); // buys 3 lots; stop-loss is now pending
  send('response', 300, book('a', 300, 90, 91, 1, 10)); // only one bid lot exists at arrival
  const out = sim.finish();
  assert.equal(out.closedTrades.length, 1);
  assert.equal(out.closedTrades[0].lots, 1);
  assert.ok(Math.abs(out.closedTrades[0].pnlRub + 101.9) < 1e-9); // 900 - 0.9 - (3,003 / 3)
  assert.equal(out.openPositions[0].lots, 2);
  assert.equal(out.feesRub, 3.9);
});

test('valid books build signal history during warmup, but a post-gap short span cannot borrow pre-gap prices', () => {
  const settings = { warmupMs: 10_000, closeBeforeEndMs: 0, latencyMs: 1, momentumReturnBps: 3, momentumFlowRatio: 0, momentumImbalance: -1 };
  const trade = (offset: number) => ({ trade: { instrumentUid: 'a', figi: 'figi-a', time: iso(offset), quantity: 1, price: { units: 100, nano: 0 }, direction: 1, tradeSource: 1 } });
  const continuous = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', settings));
  const sendContinuous = ready(continuous);
  sendContinuous('response', 10, book('a', 10, 99, 100)); // stored despite warmup
  sendContinuous('response', 10_010, trade(10_010));
  sendContinuous('response', 10_010, book('a', 10_010, 100, 101));
  assert.equal(continuous.finish().signals, 1);

  const afterGap = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', { ...settings, warmupMs: 0 }));
  const sendAfterGap = ready(afterGap);
  sendAfterGap('response', 10, book('a', 10, 99, 100));
  sendAfterGap('gap', 5_000);
  sendAfterGap('connect_attempt', 5_001, {}, 2); sendAfterGap('response', 5_002, statuses(['a']), 2); sendAfterGap('response', 5_003, ack(['a']), 2);
  sendAfterGap('response', 10_010, trade(10_010), 2);
  sendAfterGap('response', 10_011, book('a', 10_011, 100, 101), 2);
  assert.equal(afterGap.finish().signals, 0);
});

test('a fresh but source-older book cannot fill a pending IOC order', () => {
  const sim = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', { warmupMs: 0, closeBeforeEndMs: 0, latencyMs: 1, signalHook: () => true, maxHoldingMs: 99_999, takeProfitBps: 99_999, stopLossBps: 99_999 }));
  const send = ready(sim);
  send('response', 100, book('a', 100));
  const lateOlder = book('a', 50, 99, 101);
  send('response', 200, lateOlder); // age is fresh, but its exchange source time is older than 100
  send('response', 300, book('a', 300, 99, 102));
  const out = sim.finish();
  assert.equal(out.rejections.OUT_OF_ORDER_BOOK_SOURCE_TIME, 1);
  assert.equal(out.fills.find(fill => fill.side === 'BUY')!.atMs, start + 300);
});

test('replay is deterministic and duplicate trade records are retained as receipt-order input', () => {
  const run = () => {
    const sim = new OrderBookSimulator(manifest(), defaultSimulationConfig('momentum', { warmupMs: 0, closeBeforeEndMs: 0, latencyMs: 1, signalHook: context => context.receivedAtMs >= start + 300, maxHoldingMs: 1 }));
    const send = ready(sim);
    const trade = { trade: { instrumentUid: 'a', figi: 'figi-a', time: iso(150), quantity: 2, price: { units: 100, nano: 0 }, direction: 1, tradeSource: 1 } };
    send('response', 150, trade); send('response', 151, trade); // simulator never deduplicates these records
    send('response', 300, book('a', 300)); send('response', 400, book('a', 400, 110, 111));
    return sim.finish();
  };
  assert.deepEqual(run(), run());
});
