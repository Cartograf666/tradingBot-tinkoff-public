import type { TradingSchedulesResponse } from 'tinkoff-invest-api/dist/generated/instruments.js';
import { MarketDataResponse, OrderBookType, TradeSourceType } from 'tinkoff-invest-api/dist/generated/marketdata.js';
import type { OrderBook, Trade } from 'tinkoff-invest-api/dist/generated/marketdata.js';
import type { ResolvedInstrument } from '../domain/trading.js';
import { assessBookCosts, type DepthBook } from './order-book-costs.js';
import type { RecordingSummary } from './market-recording.js';

export type ObservationSource = 'EXCHANGE' | 'DEALER' | 'UNKNOWN';
export type RequestedSource = 'exchange' | 'dealer' | 'all';
export interface ObservationInstrument extends ResolvedInstrument { exchange: string; sector: string }
export interface ObservationInterval { exchange: string; type: string; start: string; end: string }
export interface ObservationManifest {
  schemaVersion: 1;
  runId: string;
  createdAt: string;
  status: 'PREPARING' | 'WAITING_FOR_MAIN_SESSION' | 'RECORDING' | 'COMPLETE' | 'FAILED';
  endpoint: string;
  source: RequestedSource;
  instruments: ObservationInstrument[];
  intervals: ObservationInterval[];
  scheduleFetchedAt: string | null;
  settings: {
    durationMs: number; depth: number; budgetRub: number; commissionRate: number;
    maxBookAgeMs: number; maxFutureSkewMs: number; sampleIntervalMs: number;
    maxBytes: number; fsyncEveryMs: number; subscriptionTimeoutMs: number;
    heartbeatTimeoutMs: number; maxReconnects: number;
    segmentMaxBytes?: number; session?: 'any' | 'main';
  };
  codeHashes: Record<string, string>;
  notes: string[];
  completedAt?: string;
  capture?: { reason: string; epochs: number; responses: number };
  recording?: RecordingSummary;
  nextSession?: { start: string; end: string } | null;
  failure?: { code: number | null; stage: string };
}

export function bookSource(book: OrderBook): ObservationSource {
  return book.orderBookType === OrderBookType.ORDERBOOK_TYPE_EXCHANGE ? 'EXCHANGE'
    : book.orderBookType === OrderBookType.ORDERBOOK_TYPE_DEALER ? 'DEALER' : 'UNKNOWN';
}
export function tradeSource(trade: Trade): ObservationSource {
  return trade.tradeSource === TradeSourceType.TRADE_SOURCE_EXCHANGE ? 'EXCHANGE'
    : trade.tradeSource === TradeSourceType.TRADE_SOURCE_DEALER ? 'DEALER' : 'UNKNOWN';
}

/** Preserve the actual API intervals. Do not infer a main session from broad start/end times. */
export function normalizeIntervals(response: TradingSchedulesResponse, exchanges: string[]): ObservationInterval[] {
  const selected = new Set(exchanges.map(x => x.toUpperCase()));
  return response.exchanges.filter(x => selected.has(x.exchange.toUpperCase())).flatMap(x =>
    x.days.filter(day => day.isTradingDay).flatMap(day => day.intervals.flatMap(interval => {
      const start = interval.interval?.startTs, end = interval.interval?.endTs;
      if (!start || !end || !Number.isFinite(start.getTime()) || !Number.isFinite(end.getTime()) || end <= start) return [];
      return [{ exchange: x.exchange, type: interval.type, start: start.toISOString(), end: end.toISOString() }];
    })),
  );
}

export interface SessionAssessment { phase: string; regular: boolean; sourceMatches: boolean }
export function classifyObservationSession(
  instrument: ObservationInstrument, atMs: number, source: ObservationSource, intervals: ObservationInterval[],
): SessionAssessment {
  const matched = intervals.filter(x => x.exchange.toUpperCase() === instrument.exchange.toUpperCase()
    && atMs >= Date.parse(x.start) && atMs <= Date.parse(x.end));
  // Specific API intervals take precedence over the generic overlapping regular interval.
  matched.sort((a, b) => priority(b.type) - priority(a.type) || a.type.localeCompare(b.type));
  const current = matched[0];
  if (!current) return { phase: 'UNKNOWN_OR_OUTSIDE_SCHEDULE', regular: false, sourceMatches: false };
  const dealer = current.type.startsWith('dealer_');
  const regular = /(?:regular|holiday)_trading_session/.test(current.type);
  return { phase: current.type, regular, sourceMatches: source !== 'UNKNOWN' && source === (dealer ? 'DEALER' : 'EXCHANGE') };
}
function priority(type: string): number {
  if (/auction|clearing|break/.test(type)) return 4;
  if (/_(main|morning|evening)$/.test(type)) return 3;
  return 1;
}

export function subscriptionAcknowledgments(payload: unknown, source: RequestedSource, depth: number): { key: string; success: boolean }[] {
  const r = MarketDataResponse.fromJSON(payload);
  const type = source === 'exchange' ? 1 : source === 'dealer' ? 2 : 3;
  return [
    ...(r.subscribeOrderBookResponse?.orderBookSubscriptions ?? []).map(s => ({
      key: `book:${s.instrumentUid}`, success: s.subscriptionStatus === 1 && s.depth === depth && s.orderBookType === type,
    })),
    ...(r.subscribeTradesResponse?.tradeSubscriptions ?? []).map(s => ({
      key: `trade:${s.instrumentUid}`, success: s.subscriptionStatus === 1 && r.subscribeTradesResponse?.tradeSource === type,
    })),
    ...(r.subscribeInfoResponse?.infoSubscriptions ?? []).map(s => ({ key: `info:${s.instrumentUid}`, success: s.subscriptionStatus === 1 })),
  ];
}

export function observationSubscriptions(instruments: ObservationInstrument[]): string[] {
  return instruments.flatMap(i => [`book:${i.uid}`, `trade:${i.uid}`, `info:${i.uid}`]);
}

function quotation(value: { units: number; nano: number } | undefined): number {
  if (!value || !Number.isSafeInteger(value.units) || !Number.isInteger(value.nano) || Math.abs(value.nano) >= 1e9
    || (value.units > 0 && value.nano < 0) || (value.units < 0 && value.nano > 0)) return Number.NaN;
  return value.units + value.nano / 1e9;
}

export type BookQuality = { usable: false; reason: string } | { usable: true; book: DepthBook; source: ObservationSource; timestampMs: number; ageMs: number };
export function qualifyBook(
  book: OrderBook, instrument: ObservationInstrument | undefined, receivedAtMs: number,
  settings: Pick<ObservationManifest['settings'], 'maxBookAgeMs' | 'maxFutureSkewMs'>,
): BookQuality {
  if (!instrument || !book.instrumentUid || book.instrumentUid !== instrument.uid
    || (book.figi && book.figi !== instrument.figi)) return { usable: false, reason: 'UNEXPECTED_INSTRUMENT' };
  const source = bookSource(book);
  if (source === 'UNKNOWN') return { usable: false, reason: 'UNKNOWN_SOURCE' };
  const time = book.time?.getTime();
  if (time === undefined || !Number.isFinite(time) || !Number.isFinite(receivedAtMs)) return { usable: false, reason: 'MISSING_OR_INVALID_TIME' };
  const age = receivedAtMs - time;
  if (age < -settings.maxFutureSkewMs) return { usable: false, reason: 'FUTURE_TIME' };
  if (age > settings.maxBookAgeMs) return { usable: false, reason: 'STALE' };
  if (!book.isConsistent) return { usable: false, reason: 'INCONSISTENT' };
  const normalized: DepthBook = {
    bids: book.bids.map(x => ({ price: quotation(x.price), quantityLots: x.quantity })),
    asks: book.asks.map(x => ({ price: quotation(x.price), quantityLots: x.quantity })),
  };
  try {
    // Reuse the same strict structural validation, independent of study budget/depth availability.
    assessBookCosts(normalized, { lotSize: instrument.lot, budgetRub: 1, commissionRate: 0 });
  } catch { return { usable: false, reason: 'INVALID_DEPTH_OR_PRICES' }; }
  return { usable: true, book: normalized, source, timestampMs: time, ageMs: age };
}

export function qualifyTrade(
  trade: Trade, instrument: ObservationInstrument | undefined, receivedAtMs: number,
  settings: Pick<ObservationManifest['settings'], 'maxBookAgeMs' | 'maxFutureSkewMs'>,
): string | null {
  if (!instrument || trade.instrumentUid !== instrument.uid || (trade.figi && trade.figi !== instrument.figi)) return 'UNEXPECTED_INSTRUMENT';
  if (tradeSource(trade) === 'UNKNOWN') return 'UNKNOWN_SOURCE';
  if (![1, 2].includes(trade.direction) || !Number.isSafeInteger(trade.quantity) || trade.quantity <= 0
    || !(quotation(trade.price) > 0)) return 'INVALID_TRADE';
  const time = trade.time?.getTime();
  if (time === undefined || !Number.isFinite(time)) return 'MISSING_OR_INVALID_TIME';
  const age = receivedAtMs - time;
  if (age < -settings.maxFutureSkewMs) return 'FUTURE_TIME';
  if (age > settings.maxBookAgeMs) return 'STALE';
  return null;
}
