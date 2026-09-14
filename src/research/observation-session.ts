import type { ObservationInstrument, ObservationInterval } from './market-observation.js';

export interface MainSessionWindow { start: string; end: string }

/** Intersect the explicit main-session intervals for every selected instrument. */
export function nextMainSessionWindow(
  instruments: readonly Pick<ObservationInstrument, 'exchange'>[], intervals: readonly ObservationInterval[],
  nowMs: number, durationMs: number,
): MainSessionWindow | null {
  if (!Number.isFinite(nowMs) || !Number.isSafeInteger(durationMs) || durationMs <= 0 || !instruments.length) throw new Error('Invalid session window input');
  let common: { start: number; end: number }[] = [{ start: nowMs, end: Number.POSITIVE_INFINITY }];
  for (const exchange of new Set(instruments.map(i => i.exchange.toUpperCase()))) {
    const windows = intervals.filter(i => i.exchange.toUpperCase() === exchange && i.type === 'regular_trading_session_main')
      .map(i => ({ start: Date.parse(i.start), end: Date.parse(i.end) }))
      .filter(i => Number.isFinite(i.start) && Number.isFinite(i.end) && i.end > i.start);
    common = common.flatMap(a => windows.map(b => ({ start: Math.max(a.start, b.start), end: Math.min(a.end, b.end) })))
      .filter(w => w.end - w.start >= durationMs + 1000);
  }
  const next = common.sort((a, b) => a.start - b.start || a.end - b.end)[0];
  return next ? { start: new Date(next.start).toISOString(), end: new Date(next.end).toISOString() } : null;
}
