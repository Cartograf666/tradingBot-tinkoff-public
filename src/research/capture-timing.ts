import { monitorEventLoopDelay, type IntervalHistogram } from 'node:perf_hooks';

const LIMITS_MS = [0.25, 0.5, 1, 2, 5, 10, 50, 200, 1000] as const;
const MAX_BOOK_INSTRUMENTS = 12;

export interface TimingDistribution {
  count: number;
  meanMs: number | null;
  maxMs: number | null;
  buckets: number[];
}

/** Fixed-size exact buckets, count, mean and maximum; no per-message samples retained. */
export class BoundedTimingDistribution {
  private count = 0;
  private total = 0;
  private maximum = 0;
  private readonly buckets = Array<number>(LIMITS_MS.length + 1).fill(0);

  add(durationMs: number): void {
    if (!Number.isFinite(durationMs)) return;
    const value = Math.max(0, durationMs);
    this.count += 1;
    this.total += value;
    this.maximum = Math.max(this.maximum, value);
    const index = LIMITS_MS.findIndex(limit => value <= limit);
    this.buckets[index < 0 ? LIMITS_MS.length : index] += 1;
  }

  result(): TimingDistribution {
    return { count: this.count, meanMs: this.count ? this.total / this.count : null,
      maxMs: this.count ? this.maximum : null, buckets: [...this.buckets] };
  }
}

type Stage = 'sdkYieldToDispatchMs' | 'dispatchToAppendReturnMs' | 'acknowledgmentHandlingMs';
const STAGES: Stage[] = ['sdkYieldToDispatchMs', 'dispatchToAppendReturnMs', 'acknowledgmentHandlingMs'];
type StageDistributions = Record<Stage, BoundedTimingDistribution>;
const stages = (): StageDistributions => ({ sdkYieldToDispatchMs: new BoundedTimingDistribution(),
  dispatchToAppendReturnMs: new BoundedTimingDistribution(), acknowledgmentHandlingMs: new BoundedTimingDistribution() });

export interface CaptureTimingResult {
  schemaVersion: 1;
  observationPoint: 'SDK_ASYNC_ITERATOR_AFTER_DECODE';
  appendBoundary: 'RECORD_CALLBACK_RETURN_WRITE_SYNC_WITH_PERIODIC_FSYNC';
  bucketUpperBoundsMs: number[];
  allResponses: Record<Stage, TimingDistribution>;
  bookResponsesByInstrument: Record<string, Record<Stage, TimingDistribution>>;
  tickDeadlineDriftMs: TimingDistribution;
  eventLoopDelayMs: { samples: number; meanMs: number | null; p95Ms: number | null; maxMs: number | null; resolutionMs: number };
  limitations: string;
}

function bookUid(response: unknown): string | null {
  if (!response || typeof response !== 'object') return null;
  const book = (response as { orderbook?: unknown }).orderbook;
  if (!book || typeof book !== 'object') return null;
  const uid = (book as { instrumentUid?: unknown }).instrumentUid;
  return typeof uid === 'string' ? uid : null;
}

/** Local timing after SDK iterator resolution. No socket-arrival or SDK decode clock is available. */
export class CaptureTiming {
  private readonly all = stages();
  private readonly byInstrument = new Map<string, StageDistributions>();
  private readonly tickDrift = new BoundedTimingDistribution();
  private histogram: IntervalHistogram | null = null;

  constructor(instrumentUids: string[] = []) {
    for (const uid of new Set(instrumentUids.slice(0, MAX_BOOK_INSTRUMENTS))) {
      if (typeof uid === 'string' && uid) this.byInstrument.set(uid, stages());
    }
    try {
      this.histogram = monitorEventLoopDelay({ resolution: 20 });
      this.histogram.enable();
    } catch {
      // Optional diagnostics must not prevent mandatory raw recording.
      this.histogram = null;
    }
  }

  observeResponse(response: unknown, sdkYieldToDispatchMs: number, dispatchToAppendReturnMs: number, acknowledgmentHandlingMs: number): void {
    try {
      this.all.sdkYieldToDispatchMs.add(sdkYieldToDispatchMs);
      this.all.dispatchToAppendReturnMs.add(dispatchToAppendReturnMs);
      this.all.acknowledgmentHandlingMs.add(acknowledgmentHandlingMs);
      const group = this.byInstrument.get(bookUid(response) ?? '');
      if (group) {
        group.sdkYieldToDispatchMs.add(sdkYieldToDispatchMs);
        group.dispatchToAppendReturnMs.add(dispatchToAppendReturnMs);
        group.acknowledgmentHandlingMs.add(acknowledgmentHandlingMs);
      }
    } catch {
      // Diagnostics never take ownership of the recording failure path.
    }
  }

  observeTickDeadlineDrift(durationMs: number): void { this.tickDrift.add(durationMs); }

  finish(): CaptureTimingResult {
    let eventLoopDelayMs: CaptureTimingResult['eventLoopDelayMs'] = {
      samples: 0, meanMs: null, p95Ms: null, maxMs: null, resolutionMs: 20,
    };
    try {
      if (this.histogram) {
        this.histogram.disable();
        const samples = this.histogram.count;
        eventLoopDelayMs = { samples, meanMs: samples ? this.histogram.mean / 1e6 : null,
          p95Ms: samples ? this.histogram.percentile(95) / 1e6 : null,
          maxMs: samples ? this.histogram.max / 1e6 : null, resolutionMs: 20 };
      }
    } catch {
      // Histogram is advisory; raw recording and capture result remain usable.
    }
    const result = (group: StageDistributions) => Object.fromEntries(
      STAGES.map(stage => [stage, group[stage].result()]),
    ) as Record<Stage, TimingDistribution>;
    return { schemaVersion: 1, observationPoint: 'SDK_ASYNC_ITERATOR_AFTER_DECODE',
      appendBoundary: 'RECORD_CALLBACK_RETURN_WRITE_SYNC_WITH_PERIODIC_FSYNC',
      bucketUpperBoundsMs: [...LIMITS_MS], allResponses: result(this.all),
      bookResponsesByInstrument: Object.fromEntries([...this.byInstrument].map(([uid, group]) => [uid, result(group)])),
      tickDeadlineDriftMs: this.tickDrift.result(), eventLoopDelayMs,
      limitations: 'SDK iterator resolution follows SDK decode; it is not socket arrival. Append return follows writeSync but per-event fsync is not guaranteed. Neither these timings nor event-loop delay identify provider, exchange or network latency.',
    };
  }
}
