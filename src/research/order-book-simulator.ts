/**
 * Causal, receipt-order replay of an exchange order book.  This is deliberately
 * an IOC model: it consumes displayed depth on the first eligible snapshot after
 * latency.  It does not model queue priority, passive fills, or a price after the
 * last recorded snapshot.
 */
import { MarketDataResponse } from 'tinkoff-invest-api/dist/generated/marketdata.js';
import {
  bookSource, classifyObservationSession, observationSubscriptions, qualifyBook, qualifyTrade,
  subscriptionAcknowledgments, tradeSource, type ObservationManifest,
} from './market-observation.js';
import type { DepthBook, BookLevel } from './order-book-costs.js';
import type { RecordedEvent } from './market-recording.js';

export type SimulationStrategy = 'momentum' | 'exhaustion';

export interface SignalContext {
  instrumentUid: string;
  receivedAtMs: number;
  mid: number;
  book: DepthBook;
}

export interface SimulationConfig {
  version: 1;
  strategy: SimulationStrategy;
  initialCashRub: number;
  budgetPerTradeRub: number;
  commissionRate: number;
  latencyMs: number;
  maxHoldingMs: number;
  takeProfitBps: number;
  stopLossBps: number;
  cooldownMs: number;
  maxEntries: number;
  warmupMs: number;
  closeBeforeEndMs: number;
  /** Momentum requires this 10 s mid-price increase. */
  momentumReturnBps: number;
  /** Net buy lots / all lots over ten seconds. */
  momentumFlowRatio: number;
  /** (top-five bid lots - ask lots) / all top-five lots. */
  momentumImbalance: number;
  /** Exhaustion requires this preceding 20 s decline. */
  exhaustionDeclineBps: number;
  /** And this recent 5 s recovery. */
  exhaustionRecoveryBps: number;
  exhaustionFlowRatio: number;
  /** Test-only deterministic override; production callers leave it undefined. */
  signalHook?: (context: SignalContext) => boolean;
}

export function defaultSimulationConfig(strategy: SimulationStrategy, overrides: Partial<SimulationConfig> = {}): SimulationConfig {
  return {
    version: 1, strategy, initialCashRub: 100_000, budgetPerTradeRub: 4_000,
    commissionRate: 0.0005, latencyMs: 300, maxHoldingMs: 180_000,
    takeProfitBps: 35, stopLossBps: 30, cooldownMs: 60_000, maxEntries: 20,
    warmupMs: 60_000, closeBeforeEndMs: 10_000,
    momentumReturnBps: 3, momentumFlowRatio: 0.3, momentumImbalance: 0,
    exhaustionDeclineBps: 5, exhaustionRecoveryBps: 1, exhaustionFlowRatio: 0.2,
    ...overrides,
  };
}

export interface ModelOrder { id: number; instrumentUid: string; side: 'BUY' | 'SELL'; reason: string; signalAtMs: number; arrivalAtMs: number; requestedLots: number; filledLots: number; cancelledLots: number; status: 'FILLED' | 'PARTIAL' | 'CANCELLED'; }
export interface SimulationFill { orderId: number; instrumentUid: string; side: 'BUY' | 'SELL'; atMs: number; lots: number; shares: number; vwap: number; grossRub: number; feeRub: number; }
export interface ClosedTrade { instrumentUid: string; entryOrderId: number; exitOrderId: number; openedAtMs: number; closedAtMs: number; lots: number; pnlRub: number; feesRub: number; exitReason: string; }
export interface OpenPosition { instrumentUid: string; lots: number; shares: number; entryCostRub: number; openedAtMs: number; reason: string; }
export interface SimulationResult {
  config: Omit<SimulationConfig, 'signalHook'>;
  signals: number; modelOrders: ModelOrder[]; fills: SimulationFill[]; closedTrades: ClosedTrade[];
  cashRub: number; reservedCashRub: number; feesRub: number; realizedPnlRub: number;
  openPositions: OpenPosition[]; unresolved: string[]; rejections: Record<string, number>;
  maxDrawdownRub: number; economicSuccess: false;
}

type BookState = { book: DepthBook; atMs: number; offset: bigint; epoch: number };
type PendingEntry = { instrumentUid: string; signalAtMs: number; arrivalAtMs: number; reservedRub: number; reason: string };
type PendingExit = { instrumentUid: string; signalAtMs: number; arrivalAtMs: number; reason: string };
type Position = OpenPosition & { entryOrderId: number; feesRub: number };
type PricePoint = { atMs: number; mid: number };
type FlowPoint = { atMs: number; signedLots: number };
const SIGNAL_ANCHOR_TOLERANCE_MS = 1_000;
const SIGNAL_HISTORY_RETENTION_MS = 20_000 + SIGNAL_ANCHOR_TOLERANCE_MS;

function cloneBook(book: DepthBook): DepthBook { return { bids: book.bids.map(x => ({ ...x })), asks: book.asks.map(x => ({ ...x })) }; }
function mid(book: DepthBook): number { return (book.bids[0].price + book.asks[0].price) / 2; }
function bps(from: number, to: number): number { return ((to / from) - 1) * 10_000; }
function increment(values: Record<string, number>, reason: string) { values[reason] = (values[reason] ?? 0) + 1; }
function trim<T extends { atMs: number }>(items: T[], now: number, window: number) { while (items.length && items[0].atMs < now - window) items.shift(); }
function levelsAvailable(levels: BookLevel[]): number { return levels.reduce((n, level) => n + level.quantityLots, 0); }

/** Execute as much as the displayed snapshot permits, mutating only this snapshot. */
function execute(book: DepthBook, side: 'BUY' | 'SELL', maximumLots: number, lotSize: number, feeRate: number, maximumCash = Number.POSITIVE_INFINITY) {
  const levels = side === 'BUY' ? book.asks : book.bids;
  let remaining = maximumLots, gross = 0, filled = 0, cashLeft = maximumCash;
  for (const level of levels) {
    if (!remaining) break;
    const perLotWithFee = level.price * lotSize * (1 + feeRate);
    const affordable = side === 'BUY' ? Math.floor((cashLeft / perLotWithFee) + 1e-12) : remaining;
    const lots = Math.min(remaining, level.quantityLots, affordable);
    if (!lots) break;
    level.quantityLots -= lots; remaining -= lots; filled += lots; gross += level.price * lots * lotSize;
    if (side === 'BUY') cashLeft -= lots * perLotWithFee;
  }
  const shares = filled * lotSize;
  return { lots: filled, shares, gross, fee: gross * feeRate, vwap: shares ? gross / shares : null };
}

function quoteBookForLots(book: DepthBook, lots: number, lotSize: number, feeRate: number) {
  const copy = cloneBook(book); return execute(copy, 'SELL', lots, lotSize, feeRate);
}

export class OrderBookSimulator {
  private readonly expectedAcks: Set<string>;
  private readonly acks = new Set<string>();
  private readonly statuses = new Map<string, number>();
  private readonly books = new Map<string, BookState>();
  private readonly priceHistory = new Map<string, PricePoint[]>();
  private readonly flowHistory = new Map<string, FlowPoint[]>();
  private readonly lastBookSourceTimestamp = new Map<string, number>();
  private readonly pending = new Map<string, PendingEntry>();
  private readonly pendingExits = new Map<string, PendingExit>();
  private readonly positions = new Map<string, Position>();
  private readonly cooldowns = new Map<string, number>();
  private readonly resultOrders: ModelOrder[] = [];
  private readonly resultFills: SimulationFill[] = [];
  private readonly closed: ClosedTrade[] = [];
  private readonly rejections: Record<string, number> = {};
  private readonly unresolved: string[] = [];
  private cash: number;
  private reserved = 0;
  private fees = 0;
  private realized = 0;
  private peakEquity: number;
  private maxDrawdown = 0;
  private signals = 0;
  private orderId = 0;
  private entries = 0;
  private connected = false;
  private epoch = 0;
  private firstOffset: bigint | null = null;
  private previousOffset: bigint | null = null;
  private previousAtMs: number | null = null;

  constructor(private readonly manifest: ObservationManifest, private readonly config: SimulationConfig) {
    if (config.version !== 1 || config.initialCashRub <= 0 || config.budgetPerTradeRub <= 0 || config.commissionRate < 0 || config.commissionRate >= 1) throw new Error('Invalid simulation config');
    this.expectedAcks = new Set(observationSubscriptions(manifest.instruments));
    this.cash = config.initialCashRub; this.peakEquity = this.cash;
  }

  consume(event: RecordedEvent): void {
    if (event.runId !== this.manifest.runId) throw new Error('Recording runId does not match manifest');
    const offset = BigInt(event.monotonicOffsetNs), atMs = Date.parse(event.receivedAt);
    if (!Number.isFinite(atMs)) throw new Error('Invalid event receivedAt');
    if (this.previousOffset !== null && offset < this.previousOffset) throw new Error('Recorded events must be consumed in receipt order');
    if (this.previousOffset !== null && this.previousAtMs !== null) {
      const monotonicElapsed = Number(offset - this.previousOffset) / 1e6;
      if (Math.abs((atMs - this.previousAtMs) - monotonicElapsed) > this.manifest.settings.maxFutureSkewMs) this.clearMarket('CLOCK_JUMP');
    }
    this.previousOffset = offset; this.previousAtMs = atMs; this.firstOffset ??= offset;
    if (event.kind === 'connect_attempt') { this.connected = true; this.epoch = event.connectionEpoch; this.clearMarket('RECONNECT'); return; }
    if (['disconnect', 'gap', 'heartbeat_timeout', 'subscription_timeout', 'stop'].includes(event.kind)) { this.connected = false; this.clearMarket(event.kind.toUpperCase()); return; }
    if (event.kind !== 'response') return;
    const frame = event.payload as { observationOrigin?: unknown; response?: { tradingStatuses?: { instrumentUid?: unknown; tradingStatus?: unknown }[] } } | null;
    if (frame?.observationOrigin === 'UNARY_GET_TRADING_STATUSES') {
      if (!this.connected || event.connectionEpoch !== this.epoch) return;
      for (const status of frame.response?.tradingStatuses ?? []) if (typeof status.instrumentUid === 'string' && typeof status.tradingStatus === 'number') this.statuses.set(status.instrumentUid, status.tradingStatus);
      return;
    }
    if (!this.connected || event.connectionEpoch !== this.epoch) return;
    const response = MarketDataResponse.fromJSON(event.payload);
    for (const ack of subscriptionAcknowledgments(event.payload, this.manifest.source, this.manifest.settings.depth)) {
      if (ack.success) this.acks.add(ack.key); else { this.acks.delete(ack.key); this.books.clear(); }
    }
    if (response.tradingStatus) this.statuses.set(response.tradingStatus.instrumentUid, response.tradingStatus.tradingStatus);
    if (response.trade) this.consumeTrade(response.trade, atMs);
    if (response.orderbook) this.consumeBook(response.orderbook, event, atMs, offset);
  }

  private clearMarket(reason: string) {
    this.books.clear(); this.acks.clear(); this.statuses.clear(); this.priceHistory.clear(); this.flowHistory.clear(); this.lastBookSourceTimestamp.clear();
    for (const pending of this.pending.values()) { this.cash += pending.reservedRub; this.reserved -= pending.reservedRub; increment(this.rejections, `PENDING_CANCELLED_${reason}`); }
    this.pending.clear();
    for (const pending of this.pendingExits.values()) increment(this.rejections, `PENDING_EXIT_CANCELLED_${reason}`);
    this.pendingExits.clear();
  }

  private marketAllowed(uid: string, atMs: number, source: 'EXCHANGE' | 'DEALER' | 'UNKNOWN'): boolean {
    const instrument = this.manifest.instruments.find(x => x.uid === uid);
    if (!instrument || source !== 'EXCHANGE' || !this.connected || !this.expectedAcks.size || [...this.expectedAcks].some(key => !this.acks.has(key))) return false;
    const session = classifyObservationSession(instrument, atMs, source, this.manifest.intervals);
    return session.regular && session.sourceMatches && /_main$/.test(session.phase) && this.statuses.get(uid) === 5;
  }

  private consumeTrade(trade: Parameters<typeof qualifyTrade>[0], atMs: number) {
    const instrument = this.manifest.instruments.find(x => x.uid === trade.instrumentUid);
    if (!instrument || tradeSource(trade) !== 'EXCHANGE' || qualifyTrade(trade, instrument, atMs, this.manifest.settings) || !this.marketAllowed(instrument.uid, atMs, 'EXCHANGE')) return;
    const signedLots = trade.direction === 1 ? trade.quantity : -trade.quantity;
    const history = this.flowHistory.get(instrument.uid) ?? []; history.push({ atMs, signedLots }); trim(history, atMs, 20_000); this.flowHistory.set(instrument.uid, history);
  }

  private consumeBook(book: Parameters<typeof qualifyBook>[0], event: RecordedEvent, atMs: number, offset: bigint) {
    const instrument = this.manifest.instruments.find(x => x.uid === book.instrumentUid);
    const source = bookSource(book);
    const quality = qualifyBook(book, instrument, atMs, this.manifest.settings);
    if (!instrument || !quality.usable) { if (instrument) this.books.delete(instrument.uid); return; }
    const previousSourceTime = this.lastBookSourceTimestamp.get(instrument.uid);
    if (previousSourceTime !== undefined && quality.timestampMs < previousSourceTime) {
      this.books.delete(instrument.uid); increment(this.rejections, 'OUT_OF_ORDER_BOOK_SOURCE_TIME'); return;
    }
    this.lastBookSourceTimestamp.set(instrument.uid, quality.timestampMs);
    if (!this.marketAllowed(instrument.uid, atMs, source)) { this.books.delete(instrument.uid); return; }
    const state: BookState = { book: cloneBook(quality.book), atMs, offset, epoch: event.connectionEpoch };
    this.books.set(instrument.uid, state);
    this.recordPrice(instrument.uid, state);
    this.executePending(instrument.uid, state);
    this.executePendingExit(instrument.uid, state);
    this.maybeExit(instrument.uid, state);
    this.maybeSignal(instrument.uid, state);
  }

  private executePending(uid: string, state: BookState) {
    const pending = this.pending.get(uid); if (!pending || state.atMs < pending.arrivalAtMs) return;
    const instrument = this.manifest.instruments.find(x => x.uid === uid)!;
    const maxByCash = Math.floor(pending.reservedRub / (state.book.asks[0].price * instrument.lot * (1 + this.config.commissionRate)));
    // Requested size is derived from the actual first eligible arrival snapshot,
    // never a quote at signal time.  The later snapshot can still have too little
    // depth, in which case IOC fills partially and cancels the remainder.
    const requested = Math.max(0, maxByCash);
    const execution = requested ? execute(state.book, 'BUY', requested, instrument.lot, this.config.commissionRate, pending.reservedRub) : { lots: 0, shares: 0, gross: 0, fee: 0, vwap: null };
    const order = this.newOrder(uid, 'BUY', pending.reason, pending.signalAtMs, pending.arrivalAtMs, requested, execution.lots);
    this.pending.delete(uid); this.reserved -= pending.reservedRub;
    const cost = execution.gross + execution.fee; this.cash += pending.reservedRub - cost;
    if (!execution.lots || execution.vwap === null) { increment(this.rejections, 'ENTRY_NO_DISPLAYED_DEPTH_OR_CASH'); return; }
    this.entries++; this.fees += execution.fee;
    this.resultFills.push({ orderId: order.id, instrumentUid: uid, side: 'BUY', atMs: state.atMs, lots: execution.lots, shares: execution.shares, vwap: execution.vwap, grossRub: execution.gross, feeRub: execution.fee });
    this.positions.set(uid, { instrumentUid: uid, lots: execution.lots, shares: execution.shares, entryCostRub: cost, openedAtMs: state.atMs, reason: pending.reason, entryOrderId: order.id, feesRub: execution.fee });
    this.updateDrawdown();
  }

  private maybeExit(uid: string, state: BookState) {
    const position = this.positions.get(uid); if (!position || this.pendingExits.has(uid)) return;
    const instrument = this.manifest.instruments.find(x => x.uid === uid)!;
    const estimated = quoteBookForLots(state.book, position.lots, instrument.lot, this.config.commissionRate);
    if (!estimated.lots || estimated.vwap === null) return;
    const estimatedNet = estimated.gross - estimated.fee;
    // An incomplete displayed bid can only liquidate part of the position. Compare
    // that partial sale to its proportional entry cost; never call a full position
    // a stop loss by dividing a partial receipt by all of its entry cash.
    const proportionalCost = position.entryCostRub * estimated.lots / position.lots;
    const returnBps = bps(proportionalCost, estimatedNet);
    const elapsed = state.atMs - position.openedAtMs;
    const cutoff = this.firstOffset !== null && state.offset - this.firstOffset >= BigInt(Math.max(0, this.manifest.settings.durationMs - this.config.closeBeforeEndMs)) * 1_000_000n;
    const reason = cutoff ? 'CLOSE_BEFORE_CAPTURE_END' : returnBps >= this.config.takeProfitBps ? 'TAKE_PROFIT' : returnBps <= -this.config.stopLossBps ? 'STOP_LOSS' : elapsed >= this.config.maxHoldingMs ? 'MAX_HOLDING' : null;
    if (!reason) return;
    this.pendingExits.set(uid, { instrumentUid: uid, signalAtMs: state.atMs, arrivalAtMs: state.atMs + this.config.latencyMs, reason });
  }

  private executePendingExit(uid: string, state: BookState) {
    const pending = this.pendingExits.get(uid); const position = this.positions.get(uid);
    if (!pending || !position || state.atMs < pending.arrivalAtMs) return;
    const instrument = this.manifest.instruments.find(x => x.uid === uid)!;
    const execution = execute(state.book, 'SELL', position.lots, instrument.lot, this.config.commissionRate);
    this.pendingExits.delete(uid);
    if (!execution.lots || execution.vwap === null) { increment(this.rejections, 'EXIT_NO_DISPLAYED_DEPTH'); return; }
    const order = this.newOrder(uid, 'SELL', pending.reason, pending.signalAtMs, pending.arrivalAtMs, position.lots, execution.lots);
    this.applyExit(uid, position, order, execution, state.atMs, pending.reason);
  }

  private applyExit(uid: string, position: Position, order: ModelOrder, execution: ReturnType<typeof execute>, atMs: number, reason: string) {
    if (execution.vwap === null) throw new Error('Cannot apply an empty exit');
    this.cash += execution.gross - execution.fee; this.fees += execution.fee;
    this.resultFills.push({ orderId: order.id, instrumentUid: uid, side: 'SELL', atMs, lots: execution.lots, shares: execution.shares, vwap: execution.vwap, grossRub: execution.gross, feeRub: execution.fee });
    const closedCost = position.entryCostRub * execution.lots / position.lots;
    const pnl = execution.gross - execution.fee - closedCost; this.realized += pnl;
    this.closed.push({ instrumentUid: uid, entryOrderId: position.entryOrderId, exitOrderId: order.id, openedAtMs: position.openedAtMs, closedAtMs: atMs, lots: execution.lots, pnlRub: pnl, feesRub: position.feesRub * execution.lots / position.lots + execution.fee, exitReason: reason });
    if (execution.lots === position.lots) this.positions.delete(uid);
    else { position.lots -= execution.lots; position.shares -= execution.shares; position.entryCostRub -= closedCost; position.feesRub -= position.feesRub * execution.lots / (position.lots + execution.lots); }
    this.updateDrawdown();
  }

  private maybeSignal(uid: string, state: BookState) {
    if (this.entries + this.pending.size >= this.config.maxEntries || this.pending.has(uid) || this.positions.has(uid) || this.pendingExits.has(uid)) return;
    const elapsed = this.firstOffset === null ? 0 : Number(state.offset - this.firstOffset) / 1e6;
    if (elapsed < this.config.warmupMs || this.cutoffReached(state.offset)) return;
    const cooldown = this.cooldowns.get(uid); if (cooldown !== undefined && state.atMs < cooldown) return;
    const prices = this.priceHistory.get(uid) ?? [];
    const context: SignalContext = { instrumentUid: uid, receivedAtMs: state.atMs, mid: mid(state.book), book: cloneBook(state.book) };
    const matches = this.config.signalHook ? this.config.signalHook(context) : this.config.strategy === 'momentum' ? this.momentum(prices, uid, state) : this.exhaustion(prices, uid, state);
    if (!matches) return;
    this.signals++; this.cooldowns.set(uid, state.atMs + this.config.cooldownMs);
    const reserved = Math.min(this.config.budgetPerTradeRub, this.cash);
    if (reserved < state.book.asks[0].price * this.manifest.instruments.find(x => x.uid === uid)!.lot * (1 + this.config.commissionRate)) { increment(this.rejections, 'INSUFFICIENT_SHARED_CASH'); return; }
    this.cash -= reserved; this.reserved += reserved;
    this.pending.set(uid, { instrumentUid: uid, signalAtMs: state.atMs, arrivalAtMs: state.atMs + this.config.latencyMs, reservedRub: reserved, reason: this.config.strategy.toUpperCase() });
  }

  private momentum(prices: PricePoint[], uid: string, state: BookState): boolean {
    const old = this.anchor(prices, state.atMs, 10_000); if (!old || bps(old.mid, mid(state.book)) < this.config.momentumReturnBps) return false;
    const flow = this.flow(uid, state.atMs, 10_000); const top = state.book.bids.slice(0, 5).reduce((n, x) => n + x.quantityLots, 0) - state.book.asks.slice(0, 5).reduce((n, x) => n + x.quantityLots, 0);
    const total = state.book.bids.slice(0, 5).reduce((n, x) => n + x.quantityLots, 0) + state.book.asks.slice(0, 5).reduce((n, x) => n + x.quantityLots, 0);
    return flow.total > 0 && flow.net / flow.total >= this.config.momentumFlowRatio && total > 0 && top / total > this.config.momentumImbalance;
  }
  private exhaustion(prices: PricePoint[], uid: string, state: BookState): boolean {
    const prior = this.anchor(prices, state.atMs, 20_000), recent = this.anchor(prices, state.atMs, 5_000);
    if (!prior || !recent || bps(prior.mid, mid(state.book)) > -this.config.exhaustionDeclineBps || bps(recent.mid, mid(state.book)) < this.config.exhaustionRecoveryBps) return false;
    const flow = this.flow(uid, state.atMs, 5_000); return flow.total > 0 && flow.net / flow.total >= this.config.exhaustionFlowRatio;
  }
  private flow(uid: string, now: number, window: number) { const values = (this.flowHistory.get(uid) ?? []).filter(x => x.atMs >= now - window); return { net: values.reduce((n, x) => n + x.signedLots, 0), total: values.reduce((n, x) => n + Math.abs(x.signedLots), 0) }; }
  private recordPrice(uid: string, state: BookState) {
    const prices = this.priceHistory.get(uid) ?? [];
    prices.push({ atMs: state.atMs, mid: mid(state.book) }); trim(prices, state.atMs, SIGNAL_HISTORY_RETENTION_MS); this.priceHistory.set(uid, prices);
  }
  /** Last received midpoint at/before the requested lookback, within one sampling interval. */
  private anchor(prices: PricePoint[], now: number, lookbackMs: number): PricePoint | null {
    const target = now - lookbackMs;
    for (let index = prices.length - 1; index >= 0; index--) {
      const point = prices[index];
      if (point.atMs > target) continue;
      return target - point.atMs <= SIGNAL_ANCHOR_TOLERANCE_MS ? point : null;
    }
    return null;
  }
  private cutoffReached(offset: bigint) { return this.firstOffset !== null && offset - this.firstOffset >= BigInt(Math.max(0, this.manifest.settings.durationMs - this.config.closeBeforeEndMs)) * 1_000_000n; }
  private newOrder(uid: string, side: 'BUY' | 'SELL', reason: string, signalAtMs: number, arrivalAtMs: number, requestedLots: number, filledLots: number) { const order: ModelOrder = { id: ++this.orderId, instrumentUid: uid, side, reason, signalAtMs, arrivalAtMs, requestedLots, filledLots, cancelledLots: requestedLots - filledLots, status: !filledLots ? 'CANCELLED' : filledLots === requestedLots ? 'FILLED' : 'PARTIAL' }; this.resultOrders.push(order); return order; }
  private updateDrawdown() {
    // Open inventory stays at its recorded entry cost.  The simulator deliberately
    // refuses to invent a terminal mark, so only realised execution changes equity.
    const inventoryAtCost = [...this.positions.values()].reduce((sum, position) => sum + position.entryCostRub, 0);
    const equity = this.cash + this.reserved + inventoryAtCost;
    this.peakEquity = Math.max(this.peakEquity, equity); this.maxDrawdown = Math.max(this.maxDrawdown, this.peakEquity - equity);
  }

  finish(): SimulationResult {
    for (const pending of this.pending.values()) { this.cash += pending.reservedRub; this.reserved -= pending.reservedRub; this.unresolved.push(`Pending entry for ${pending.instrumentUid} never reached an eligible book after latency`); }
    this.pending.clear();
    for (const pending of this.pendingExits.values()) this.unresolved.push(`Pending exit for ${pending.instrumentUid} never reached an eligible book after latency`);
    this.pendingExits.clear();
    for (const position of this.positions.values()) this.unresolved.push(`Open ${position.instrumentUid} position of ${position.lots} lots has no fabricated end price`);
    const { signalHook: _hook, ...config } = this.config;
    return { config, signals: this.signals, modelOrders: this.resultOrders, fills: this.resultFills, closedTrades: this.closed, cashRub: this.cash, reservedCashRub: this.reserved, feesRub: this.fees, realizedPnlRub: this.realized, openPositions: [...this.positions.values()].map(({ entryOrderId: _id, feesRub: _fees, ...p }) => p), unresolved: this.unresolved, rejections: this.rejections, maxDrawdownRub: this.maxDrawdown, economicSuccess: false };
  }
}
